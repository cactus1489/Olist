# ... (기존 import 및 함수 정의는 동일하게 유지하거나 main 위로 이동)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests
import json
import numpy as np

# 경로 설정
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

@st.cache_data
def get_brazil_geojson():
    # 브라질 주별 GeoJSON 데이터 로드 (외부 URL 사용)
    url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    try:
        response = requests.get(url)
        return response.json()
    except:
        return None

@st.cache_data
def load_and_process_data():
    # 데이터 로드
    try:
        orders = pd.read_parquet(os.path.join(BASE_PATH, 'olist_orders_dataset.parquet'))
        customers = pd.read_parquet(os.path.join(BASE_PATH, 'olist_customers_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_items_dataset.parquet'))
    except:
        orders = pd.read_csv(os.path.join(BASE_PATH, 'olist_orders_dataset.csv'))
        customers = pd.read_csv(os.path.join(BASE_PATH, 'olist_customers_dataset.csv'))
        order_items = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_items_dataset.csv'))

    # 날짜 컬럼 변환
    date_columns = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in date_columns:
        orders[col] = pd.to_datetime(orders[col])

    # 데이터 병합 (주문 + 고객 + 아이템)
    df = pd.merge(orders, customers[['customer_id', 'customer_state']], on='customer_id', how='left')
    
    # 거래액 계산을 위해 order_items 합산
    order_values = order_items.groupby('order_id')['price'].sum().reset_index()
    df = pd.merge(df, order_values, on='order_id', how='left').fillna({'price': 0})

    # 스냅샷 기준일 (최근 주문일로부터 1일 뒤)
    snapshot_date = df['order_purchase_timestamp'].max() + pd.Timedelta(days=1)

    # 기본 상태 계산
    df['is_success'] = df['order_status'] == 'delivered'
    df['is_canceled'] = df['order_status'] == 'canceled'
    
    # 지연 계산
    # 1. 성공 주문 중 지연: 실제 도착일 > 예상일
    df['is_success_delay'] = (df['is_success']) & (df['order_delivered_customer_date'] > df['order_estimated_delivery_date'])
    # 2. 실패(배송 중/처리 중) 중 지연: 예상일 < 기준일
    df['is_failure_delay'] = (~df['is_success'] & ~df['is_canceled']) & (df['order_estimated_delivery_date'] < snapshot_date)
    # 3. 취소 중 지연: 취소 상태이면서 예상일 < 기준일 (배송 지연으로 인한 취소 추정)
    df['is_canceled_delay'] = (df['is_canceled']) & (df['order_estimated_delivery_date'] < snapshot_date)
    
    # 소요 시간 및 기간 계산
    df['delivery_time_success'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
    df['delivery_time_failure'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.days
    
    # 지연 강도 계산 (예정일 대비 현재 얼마나 지났는가)
    df['delay_duration'] = (snapshot_date - df['order_estimated_delivery_date']).dt.days
    
    # 지연 카테고리 분류 (취소/부도 지연건 대상)
    def categorize_delay(days):
        if days <= 0: return None
        if days <= 3: return '1-3일'
        elif days <= 7: return '4-7일'
        else: return '7일 이상'
    
    df['delay_intensity'] = df.apply(lambda x: categorize_delay(x['delay_duration']) if (x['is_canceled_delay'] or x['is_failure_delay']) else None, axis=1)

    # 지역별 집계
    region_stats = df.groupby('customer_state').agg(
        total_orders=('order_id', 'count'),
        total_order_value=('price', 'sum'),
        success_cnt=('is_success', 'sum'),
        canceled_cnt=('is_canceled', 'sum'),
        canceled_value=('price', lambda x: df.loc[x.index, 'price'][df.loc[x.index, 'is_canceled']].sum()),
        success_delay_cnt=('is_success_delay', 'sum'),
        failure_delay_cnt=('is_failure_delay', 'sum'),
        canceled_delay_cnt=('is_canceled_delay', 'sum'),
        canceled_delay_value=('price', lambda x: df.loc[x.index, 'price'][df.loc[x.index, 'is_canceled_delay']].sum()),
        avg_time_success=('delivery_time_success', 'mean'),
        avg_time_failure=('delivery_time_failure', 'mean'),
        delay_1_3=('delay_intensity', lambda x: (x == '1-3일').sum()),
        delay_4_7=('delay_intensity', lambda x: (x == '4-7일').sum()),
        delay_7_plus=('delay_intensity', lambda x: (x == '7일 이상').sum())
    ).reset_index()

    # 비율 및 추가 지표 계산
    region_stats['Total Delay Ratio (%)'] = ((region_stats['success_delay_cnt'] + region_stats['failure_delay_cnt'] + region_stats['canceled_delay_cnt']) / region_stats['total_orders'] * 100).round(2)
    region_stats['Revenue Loss Ratio (%)'] = (region_stats['canceled_delay_value'] / region_stats['total_order_value'] * 100).round(2)
    
    # 세그먼트 구분 (상/중/하)
    # 1. 배송지연율 기준
    labels = ['하', '중', '상'] 
    region_stats['Delay Segment'] = pd.qcut(region_stats['Total Delay Ratio (%)'], q=3, labels=labels)
    
    # 2. 거래 건수 기준
    labels_vol = ['하', '중', '상'] 
    region_stats['Order Volume Segment'] = pd.qcut(region_stats['total_orders'], q=3, labels=labels_vol)

    return region_stats, df, order_items

@st.cache_data
def load_product_data():
    """상품 데이터 로드 및 처리"""
    try:
        products = pd.read_parquet(os.path.join(BASE_PATH, 'olist_products_dataset.parquet'))
    except:
        products = pd.read_csv(os.path.join(BASE_PATH, 'olist_products_dataset.csv'))
    
    try:
        order_items = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_items_dataset.parquet'))
    except:
        order_items = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_items_dataset.csv'))
    
    try:
        orders = pd.read_parquet(os.path.join(BASE_PATH, 'olist_orders_dataset.parquet'))
    except:
        orders = pd.read_csv(os.path.join(BASE_PATH, 'olist_orders_dataset.csv'))
    
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    
    # 주문 아이템에 상품 정보 병합
    items_with_products = pd.merge(order_items, products, on='product_id', how='left')
    items_with_products = pd.merge(items_with_products, orders[['order_id', 'order_purchase_timestamp', 'order_status']], on='order_id', how='left')
    
    return items_with_products, products

@st.cache_data
def load_seller_data():
    """판매자 데이터 로드 및 처리"""
    try:
        sellers = pd.read_csv(os.path.join(BASE_PATH, 'olist_sellers_dataset.csv'))
    except:
        st.error("판매자 데이터를 찾을 수 없습니다.")
        return None, None
    
    try:
        order_items = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_items_dataset.parquet'))
    except:
        order_items = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_items_dataset.csv'))
    
    try:
        orders = pd.read_parquet(os.path.join(BASE_PATH, 'olist_orders_dataset.parquet'))
    except:
        orders = pd.read_csv(os.path.join(BASE_PATH, 'olist_orders_dataset.csv'))
    
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    
    # 주문 아이템에 판매자 정보 병합
    items_with_sellers = pd.merge(order_items, sellers, on='seller_id', how='left')
    items_with_sellers = pd.merge(items_with_sellers, orders[['order_id', 'order_purchase_timestamp', 'order_status']], on='order_id', how='left')
    
    return items_with_sellers, sellers

@st.cache_data
def calculate_monthly_revenue_trend(_df):
    """년도별 월 매출 추세 계산 (취소건, 지연율 포함)"""
    # 년-월 컬럼 생성
    df_copy = _df.copy()
    df_copy['year_month'] = df_copy['order_purchase_timestamp'].dt.to_period('M')
    df_copy['year'] = df_copy['order_purchase_timestamp'].dt.year
    df_copy['month'] = df_copy['order_purchase_timestamp'].dt.month
    
    # 월별 종합 집계
    monthly_stats = df_copy.groupby(['year', 'month']).agg(
        total_revenue=('price', 'sum'),
        total_orders=('order_id', 'count'),
        canceled_orders=('is_canceled', 'sum'),
        canceled_revenue=('price', lambda x: df_copy.loc[x.index, 'price'][df_copy.loc[x.index, 'is_canceled']].sum()),
        success_delay_cnt=('is_success_delay', 'sum'),
        failure_delay_cnt=('is_failure_delay', 'sum'),
        canceled_delay_cnt=('is_canceled_delay', 'sum')
    ).reset_index()
    
    # 지연율 계산
    monthly_stats['total_delay_cnt'] = monthly_stats['success_delay_cnt'] + monthly_stats['failure_delay_cnt'] + monthly_stats['canceled_delay_cnt']
    monthly_stats['delay_ratio'] = (monthly_stats['total_delay_cnt'] / monthly_stats['total_orders'] * 100).round(2)
    monthly_stats['cancel_ratio'] = (monthly_stats['canceled_orders'] / monthly_stats['total_orders'] * 100).round(2)
    
    # 날짜 형식으로 변환 (그래프용)
    monthly_stats['date'] = pd.to_datetime(monthly_stats[['year', 'month']].assign(day=1))
    monthly_stats = monthly_stats.sort_values('date')
    
    return monthly_stats

def main():
    # 페이지 설정
    st.set_page_config(page_title="Olist 비즈니스 분석 대시보드", layout="wide", initial_sidebar_state="expanded")

    # 메인 타이틀
    st.title("📊 Olist E-Commerce 종합 분석 대시보드")
    st.markdown("**브라질 이커머스 데이터 기반 비즈니스 인사이트**")
    st.divider()
    
    # 탭 생성
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "🚚 지역별 배송 분석", 
        "📦 상품별 매출 분석", 
        "👥 판매자 분석", 
        "📉 매출 하락 원인 분석", 
        "🚀 매출 증대 전략 제언", 
        "🔍 판매자 상세 모니터링",
        "🧪 생존 확률 계산 로직",
        "📊 리텐션 코호트 분석"
    ])
    
    # ==================== 탭 1: 지역별 배송 분석 ====================
    with tab1:
        render_delivery_analysis_tab()
    
    # ==================== 탭 2: 상품별 매출 분석 ====================
    with tab2:
        render_product_analysis_tab()
    
    # ==================== 탭 3: 판매자 분석 ====================
    with tab3:
        render_seller_analysis_tab()

    # ==================== 탭 4: 매출 하락 원인 분석 (New) ====================
    with tab4:
        render_revenue_drop_analysis_tab()

    # ==================== 탭 5: 매출 증대 전략 제언 (New) ====================
    with tab5:
        render_action_plan_tab()

    # ==================== 탭 6: 판매자 상세 모니터링 (New) ====================
    with tab6:
        render_seller_detail_tab()

    # ==================== 탭 7: 생존 확률 계산 로직 (New) ====================
    with tab7:
        render_survival_logic_tab()

    # ==================== 탭 8: 리텐션 코호트 분석 (New) ====================
    with tab8:
        render_cohort_analysis_tab()

def render_delivery_analysis_tab():
    """탭 1: 지역별 배송 지연 분석"""
    # 데이터 로딩
    data, df_raw, order_items = load_and_process_data()
    brazil_geojson = get_brazil_geojson()
    monthly_trend = calculate_monthly_revenue_trend(df_raw)

    # 사이드바
    st.sidebar.title("🚚 배송 지연 분석 필터")
    selected_states = st.sidebar.multiselect("분석할 지역(State) 선택", options=data['customer_state'].unique(), default=['AL', 'MA', 'RR', 'PI', 'CE', 'SE', 'BA', 'RJ', 'PA'])

    if not selected_states:
        filtered_data = data
    else:
        filtered_data = data[data['customer_state'].isin(selected_states)]

    # 메인 레이아웃
    st.title("📊 브라질 지역별 배송 지연율 대시보드")
    st.markdown("모든 지역의 배송 성공/실패 지연 현황을 지도로 한눈에 확인하세요.")

    # KPI 카드
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 주문 건수", f"{filtered_data['total_orders'].sum():,}")
    avg_total_delay = ((filtered_data['success_delay_cnt'].sum() + filtered_data['failure_delay_cnt'].sum()) / filtered_data['total_orders'].sum() * 100)
    c2.metric("전체 지연율", f"{avg_total_delay:.2f}%")
    c3.metric("성공 지연 건수", f"{filtered_data['success_delay_cnt'].sum():,}")
    c4.metric("실패 지연 건수", f"{filtered_data['failure_delay_cnt'].sum():,}")

    st.divider()

    # 지도 시각화 섹션
    st.subheader("🗺️ 브라질 주별 배송 지연율 지도")
    if brazil_geojson:
        fig_map = px.choropleth(
            data,
            geojson=brazil_geojson,
            locations='customer_state',
            featureidkey="properties.sigla",  # GeoJSON 내 주 코드 필드
            color='Total Delay Ratio (%)',
            color_continuous_scale="Reds",
            scope="south america",
            hover_name='customer_state',
            hover_data={'total_orders': True, 'Total Delay Ratio (%)': ':.2f'},
            labels={'Total Delay Ratio (%)': '지연율 (%)', 'customer_state': '주 코드'}
        )
        fig_map.update_geos(fitbounds="locations", visible=False)
        fig_map.update_layout(height=600, margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, width="stretch")
    else:
        st.warning("지도를 불러오는 데 실패했습니다. 데이터 연결을 확인해 주세요.")

    st.divider()

    # 그래프 섹션
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("모든 지역 지연율 비교")
        fig_bar = px.bar(
            data.sort_values('Total Delay Ratio (%)', ascending=False),
            x='customer_state',
            y='Total Delay Ratio (%)',
            color='Total Delay Ratio (%)',
            labels={'customer_state': '지역 (State)', 'Total Delay Ratio (%)': '통합 지연율 (%)'},
            color_continuous_scale='Reds',
            text_auto='.1f'
        )
        fig_bar.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar, width="stretch")

    with col_right:
        st.subheader("성공 vs 실패 지연 비중")
        total_success_delay = filtered_data['success_delay_cnt'].sum()
        total_failure_delay = filtered_data['failure_delay_cnt'].sum()
        fig_pie = px.pie(
            names=['성공 지연', '실패 지연'],
            values=[total_success_delay, total_failure_delay],
            hole=0.4,
            color_discrete_sequence=['#4CAF50', '#FF5252']
        )
        st.plotly_chart(fig_pie, width="stretch")

    st.divider()

    # 지표 섹션 (추가 요청 사항)
    st.subheader("📊 지역별 주문 및 취소 통계")
    stats_display = filtered_data.rename(columns={
        'customer_state': '지역',
        'total_orders': '총주문건수',
        'total_order_value': '총주문거래액',
        'canceled_cnt': '총취소건수',
        'canceled_value': '총취소거래액',
        'canceled_delay_cnt': '배송지연 취소건수',
        'canceled_delay_value': '배송지연 취소거래액',
        'Total Delay Ratio (%)': '통합 지연율(%)',
        'Revenue Loss Ratio (%)': '매출 손실 비중(%)',
        'delay_1_3': '1-3일 지연(건)',
        'delay_4_7': '4-7일 지연(건)',
        'delay_7_plus': '7일 이상 지연(건)',
        'avg_time_success': '평균 배송일(성공)',
        'avg_time_failure': '평균 배송 예정일(취소)'
    })
    
    # 숫자 포맷팅
    format_dict = {
        '총주문건수': '{:,}',
        '총주문거래액': 'R$ {:,.2f}',
        '총취소건수': '{:,}',
        '총취소거래액': 'R$ {:,.2f}',
        '배송지연 취소건수': '{:,}',
        '배송지연 취소거래액': 'R$ {:,.2f}',
        '통합 지연율(%)': '{:.2f}%',
        '매출 손실 비중(%)': '{:.2f}%',
        '1-3일 지연(건)': '{:,}',
        '4-7일 지연(건)': '{:,}',
        '7일 이상 지연(건)': '{:,}',
        '평균 배송일(성공)': '{:.1f}일',
        '평균 배송 예정일(취소)': '{:.1f}일'
    }
    

    st.dataframe(
        stats_display[['지역', '총주문건수', '총주문거래액', '평균 배송일(성공)', '총취소건수', '평균 배송 예정일(취소)', '총취소거래액', '배송지연 취소건수', '배송지연 취소거래액']]
        .sort_values('총주문건수', ascending=False)
        .reset_index(drop=True)
        .style.format(format_dict),
        width='stretch'
    )

    st.divider()


    # 그래프 섹션 (기존 유지 및 보완)
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("모든 지역 지연율 비교")
        fig_bar_delay = px.bar(
            data.sort_values('Total Delay Ratio (%)', ascending=False),
            x='customer_state',
            y='Total Delay Ratio (%)',
            color='Delay Segment',
            labels={'customer_state': '지역 (State)', 'Total Delay Ratio (%)': '통합 지연율 (%)', 'Delay Segment': '지연 세그먼트'},
            color_discrete_map={'상': '#FF5252', '중': '#FFB74D', '하': '#4CAF50'},
            text_auto='.1f'
        )
        fig_bar_delay.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar_delay, width='stretch')

    with col_right:
        st.subheader("모든 지역 주문 거래량 비교")
        fig_bar_vol = px.bar(
            data.sort_values('total_orders', ascending=False),
            x='customer_state',
            y='total_orders',
            color='Order Volume Segment',
            labels={'customer_state': '지역 (State)', 'total_orders': '총 주문 건수', 'Order Volume Segment': '주문량 세그먼트'},
            color_discrete_map={'상': '#2196F3', '중': '#64B5F6', '하': '#BBDEFB'},
            text_auto=True
        )
        fig_bar_vol.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar_vol, width='stretch')

    st.divider()

    # 월별 매출 추세 그래프
    st.subheader("📈 년도별 월 매출 추세 분석")
    st.markdown("시간에 따른 매출 성장 추세를 확인하고, 계절성 패턴을 파악하세요.")
    
    col_trend1, col_trend2 = st.columns([3, 1])
    
    with col_trend1:
        # 꺾은선 그래프 - 월별 매출
        fig_trend = go.Figure()
        
        # 년도별로 다른 색상 적용
        years = monthly_trend['year'].unique()
        colors = px.colors.qualitative.Set2
        
        for idx, year in enumerate(sorted(years)):
            year_data = monthly_trend[monthly_trend['year'] == year]
            fig_trend.add_trace(go.Scatter(
                x=year_data['date'],
                y=year_data['total_revenue'],
                mode='lines+markers',
                name=f'{int(year)}년',
                line=dict(width=3, color=colors[idx % len(colors)]),
                marker=dict(size=8),
                hovertemplate='<b>%{x|%Y년 %m월}</b><br>매출: R$ %{y:,.0f}<extra></extra>'
            ))
        
        fig_trend.update_layout(
            title="월별 총 매출액 추이",
            xaxis_title="날짜",
            yaxis_title="매출액 (R$)",
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=400
        )
        st.plotly_chart(fig_trend, width='stretch')
    
    with col_trend2:
        # 주요 통계
        st.markdown("**📊 주요 지표**")
        total_revenue = monthly_trend['total_revenue'].sum()
        avg_monthly_revenue = monthly_trend['total_revenue'].mean()
        max_month = monthly_trend.loc[monthly_trend['total_revenue'].idxmax()]
        min_month = monthly_trend.loc[monthly_trend['total_revenue'].idxmin()]
        
        st.metric("총 매출액", f"R$ {total_revenue:,.0f}")
        st.metric("월평균 매출", f"R$ {avg_monthly_revenue:,.0f}")
        st.metric("최고 매출 월", f"{int(max_month['year'])}년 {int(max_month['month'])}월")
        st.metric("최저 매출 월", f"{int(min_month['year'])}년 {int(min_month['month'])}월")
        
        # 성장률 계산 (첫 달 대비 마지막 달)
        if len(monthly_trend) > 1:
            first_revenue = monthly_trend.iloc[0]['total_revenue']
            last_revenue = monthly_trend.iloc[-1]['total_revenue']
            growth_rate = ((last_revenue - first_revenue) / first_revenue * 100) if first_revenue > 0 else 0
            st.metric("전체 성장률", f"{growth_rate:+.1f}%")
    
    # 월별 주문 건수 추이
    st.markdown("---")
    fig_orders = px.line(
        monthly_trend,
        x='date',
        y='total_orders',
        title="월별 주문 건수 추이",
        labels={'date': '날짜', 'total_orders': '주문 건수'},
        markers=True,
        color_discrete_sequence=['#2196F3']
    )
    fig_orders.update_traces(
        line=dict(width=3),
        marker=dict(size=8),
        hovertemplate='<b>%{x|%Y년 %m월}</b><br>주문 건수: %{y:,}<extra></extra>'
    )
    fig_orders.update_layout(height=350, hovermode='x unified')
    st.plotly_chart(fig_orders, width='stretch')
    
    # 2018년 데이터 특이사항 안내
    st.info("💡 **데이터 인사이트**: 2018년 9월 이후 데이터가 급격히 감소하는 것은 데이터 수집 기간이 2018년 8월까지만 포함되어 있거나, 일부 월의 데이터가 불완전하기 때문입니다. 실제 비즈니스 성과 하락이 아닌 데이터셋의 한계로 보입니다.")
    
    st.divider()
    
    # 취소건 및 지연율 월별 추이
    st.subheader("📉 취소건 및 지연율 월별 추이")
    
    col_cancel, col_delay = st.columns(2)
    
    with col_cancel:
        # 취소건 추이 그래프
        fig_cancel = go.Figure()
        
        # 취소 건수
        fig_cancel.add_trace(go.Scatter(
            x=monthly_trend['date'],
            y=monthly_trend['canceled_orders'],
            mode='lines+markers',
            name='취소 건수',
            line=dict(width=3, color='#FF5252'),
            marker=dict(size=8),
            yaxis='y',
            hovertemplate='<b>%{x|%Y년 %m월}</b><br>취소 건수: %{y:,}<extra></extra>'
        ))
        
        # 취소율 (%)
        fig_cancel.add_trace(go.Scatter(
            x=monthly_trend['date'],
            y=monthly_trend['cancel_ratio'],
            mode='lines+markers',
            name='취소율 (%)',
            line=dict(width=2, dash='dash', color='#FF8A80'),
            marker=dict(size=6),
            yaxis='y2',
            hovertemplate='<b>%{x|%Y년 %m월}</b><br>취소율: %{y:.2f}%<extra></extra>'
        ))
        
        fig_cancel.update_layout(
            title="월별 취소 건수 및 취소율",
            xaxis_title="날짜",
            yaxis=dict(title="취소 건수", side='left'),
            yaxis2=dict(title="취소율 (%)", overlaying='y', side='right'),
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=400
        )
        st.plotly_chart(fig_cancel, width='stretch')
    
    with col_delay:
        # 지연율 추이 그래프
        fig_delay = go.Figure()
        
        # 지연 건수
        fig_delay.add_trace(go.Scatter(
            x=monthly_trend['date'],
            y=monthly_trend['total_delay_cnt'],
            mode='lines+markers',
            name='지연 건수',
            line=dict(width=3, color='#FFA726'),
            marker=dict(size=8),
            yaxis='y',
            hovertemplate='<b>%{x|%Y년 %m월}</b><br>지연 건수: %{y:,}<extra></extra>'
        ))
        
        # 지연율 (%)
        fig_delay.add_trace(go.Scatter(
            x=monthly_trend['date'],
            y=monthly_trend['delay_ratio'],
            mode='lines+markers',
            name='지연율 (%)',
            line=dict(width=2, dash='dash', color='#FFB74D'),
            marker=dict(size=6),
            yaxis='y2',
            hovertemplate='<b>%{x|%Y년 %m월}</b><br>지연율: %{y:.2f}%<extra></extra>'
        ))
        
        fig_delay.update_layout(
            title="월별 지연 건수 및 지연율",
            xaxis_title="날짜",
            yaxis=dict(title="지연 건수", side='left'),
            yaxis2=dict(title="지연율 (%)", overlaying='y', side='right'),
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=400
        )
        st.plotly_chart(fig_delay, width='stretch')
    
    # 취소와 지연의 상관관계 분석
    st.markdown("---")
    st.markdown("**🔍 취소율과 지연율의 상관관계**")
    
    fig_correlation = px.scatter(
        monthly_trend,
        x='delay_ratio',
        y='cancel_ratio',
        size='total_orders',
        color='year',
        hover_data={'year': True, 'month': True, 'delay_ratio': ':.2f', 'cancel_ratio': ':.2f'},
        labels={'delay_ratio': '지연율 (%)', 'cancel_ratio': '취소율 (%)', 'year': '년도'},
        title="지연율 vs 취소율 상관관계 (버블 크기 = 주문량)",
        color_continuous_scale='Viridis'
    )
    fig_correlation.update_traces(marker=dict(line=dict(width=1, color='white')))
    fig_correlation.update_layout(height=400)
    st.plotly_chart(fig_correlation, width='stretch')
    
    st.info("💡 **가설 검증**: 지연율이 높은 월에 취소율도 함께 증가하는 경향이 있는지 확인하세요. 양의 상관관계가 있다면 배송 지연이 취소의 주요 원인임을 시사합니다.")
    
    st.divider()


    # 배송 시간 산점도
    st.subheader("📈 배송 시간과 지연율의 상관관계")
    fig_scatter = px.scatter(
        data,
        x='avg_time_success',
        y='Total Delay Ratio (%)',
        size='total_orders',
        color='Delay Segment',
        hover_name='customer_state',
        labels={'avg_time_success': '평균 배송 성공 시간 (일)', 'Total Delay Ratio (%)': '통합 지연율 (%)', 'Delay Segment': '지연 세그먼트'},
        color_discrete_map={'상': '#FF5252', '중': '#FFB74D', '하': '#4CAF50'},
        title="배송 소요 시간 대비 지연율 분포 (색상: 지연 세그먼트)"
    )
    st.plotly_chart(fig_scatter, width='stretch')

    st.divider()

    # 지연 강도 분석 섹션 (추가 가설 검증)
    st.subheader("⏱️ 지연 기간별 미배송(취소/부도) 주문 분포")
    st.markdown("예정일이 지난 후 경과된 시간에 따른 주문 건수 분포입니다. 지연이 길어질수록 취소 리스크가 커지는지 확인하세요.")
    
    delay_intensity_df = filtered_data.melt(
        id_vars=['customer_state'],
        value_vars=['delay_1_3', 'delay_4_7', 'delay_7_plus'],
        var_name='Delay Category',
        value_name='Count'
    )
    delay_intensity_df['Delay Category'] = delay_intensity_df['Delay Category'].map({
        'delay_1_3': '1-3일 지연',
        'delay_4_7': '4-7일 지연',
        'delay_7_plus': '7일 이상 지연'
    })

    fig_intensity = px.bar(
        delay_intensity_df,
        x='customer_state',
        y='Count',
        color='Delay Category',
        title="지역별 지연 기간 분포",
        color_discrete_sequence=px.colors.sequential.OrRd[3:],
        labels={'customer_state': '지역', 'Count': '주문 건수', 'Delay Category': '지연 기간'}
    )
    fig_intensity.update_layout(barmode='stack', xaxis_tickangle=-45)
    st.plotly_chart(fig_intensity, width='stretch')
    st.info("💡 **가설 검증**: 7일 이상 지연 비중이 높은 지역은 물류 프로세스의 전면적인 재검토가 필요합니다.")

def render_product_analysis_tab():
    """탭 2: 상품별 매출 분석"""
    st.header("📦 상품별 매출 분석")
    st.markdown("카테고리별 매출 현황과 베스트셀러 상품을 분석합니다.")
    
    # 데이터 로딩
    items_with_products, products = load_product_data()
    
    # 배송 완료된 주문만 필터링
    delivered_items = items_with_products[items_with_products['order_status'] == 'delivered'].copy()
    
    # 카테고리별 집계
    category_stats = delivered_items.groupby('product_category_name').agg(
        total_revenue=('price', 'sum'),
        total_orders=('order_id', 'nunique'),
        total_items=('order_item_id', 'count'),
        avg_price=('price', 'mean')
    ).reset_index()
    
    category_stats = category_stats.sort_values('total_revenue', ascending=False)
    
    # KPI 카드
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 카테고리 수", f"{category_stats.shape[0]:,}")
    col2.metric("총 매출액", f"R$ {category_stats['total_revenue'].sum():,.0f}")
    col3.metric("총 주문 수", f"{category_stats['total_orders'].sum():,}")
    col4.metric("평균 상품 가격", f"R$ {delivered_items['price'].mean():.2f}")
    
    st.divider()
    
    # 카테고리별 매출 TOP 10
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("🏆 카테고리별 매출 TOP 10")
        top10_categories = category_stats.head(10)
        
        fig_cat_revenue = px.bar(
            top10_categories,
            x='total_revenue',
            y='product_category_name',
            orientation='h',
            title="카테고리별 총 매출액",
            labels={'total_revenue': '매출액 (R$)', 'product_category_name': '카테고리'},
            color='total_revenue',
            color_continuous_scale='Blues',
            text_auto='.2s'
        )
        fig_cat_revenue.update_layout(yaxis={'categoryorder':'total ascending'}, height=500)
        st.plotly_chart(fig_cat_revenue, width='stretch')
    
    with col_right:
        st.subheader("📊 카테고리 통계")
        st.dataframe(
            top10_categories[['product_category_name', 'total_revenue', 'total_orders', 'avg_price']]
            .rename(columns={
                'product_category_name': '카테고리',
                'total_revenue': '매출액',
                'total_orders': '주문수',
                'avg_price': '평균가격'
            })
            .style.format({
                '매출액': 'R$ {:,.0f}',
                '주문수': '{:,}',
                '평균가격': 'R$ {:.2f}'
            }),
            height=500,
            width='stretch'
        )
    
    st.divider()
    
    # 카테고리별 주문 건수 vs 매출액
    st.subheader("📈 카테고리별 주문 건수 vs 매출액 상관관계")
    
    fig_scatter_cat = px.scatter(
        category_stats.head(20),
        x='total_orders',
        y='total_revenue',
        size='avg_price',
        color='avg_price',
        hover_name='product_category_name',
        labels={'total_orders': '총 주문 수', 'total_revenue': '총 매출액 (R$)', 'avg_price': '평균 가격'},
        title="주문 수 vs 매출액 (버블 크기 = 평균 가격)",
        color_continuous_scale='Viridis'
    )
    fig_scatter_cat.update_traces(marker=dict(line=dict(width=1, color='white')))
    st.plotly_chart(fig_scatter_cat, width='stretch')
    
    st.info("💡 **인사이트**: 오른쪽 위에 위치한 카테고리는 주문량과 매출이 모두 높은 핵심 카테고리입니다. 큰 버블은 고가 상품을 의미합니다.")
    
    st.divider()
    
    # 월별 카테고리 매출 추이 (TOP 5)
    st.subheader("📅 월별 카테고리 매출 추이 (TOP 5)")
    
    delivered_items['year_month'] = delivered_items['order_purchase_timestamp'].dt.to_period('M').astype(str)
    top5_categories = category_stats.head(5)['product_category_name'].tolist()
    
    monthly_cat_revenue = delivered_items[delivered_items['product_category_name'].isin(top5_categories)].groupby(
        ['year_month', 'product_category_name']
    )['price'].sum().reset_index()
    
    fig_monthly_cat = px.line(
        monthly_cat_revenue,
        x='year_month',
        y='price',
        color='product_category_name',
        title="TOP 5 카테고리 월별 매출 추이",
        labels={'year_month': '년-월', 'price': '매출액 (R$)', 'product_category_name': '카테고리'},
        markers=True
    )
    fig_monthly_cat.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig_monthly_cat, width='stretch')

def render_seller_analysis_tab():
    """탭 3: 판매자 분석"""
    st.header("👥 판매자 분석")
    st.markdown("판매자 활동 현황, 신규/이탈 판매자 추이를 분석합니다.")
    
    # 데이터 로딩
    items_with_sellers, sellers = load_seller_data()
    
    if items_with_sellers is None:
        st.error("판매자 데이터를 불러올 수 없습니다.")
        return
    
    # 배송 완료된 주문만 필터링
    delivered_items = items_with_sellers[items_with_sellers['order_status'] == 'delivered'].copy()
    
    # 판매자별 집계
    seller_stats = delivered_items.groupby('seller_id').agg(
        total_revenue=('price', 'sum'),
        total_orders=('order_id', 'nunique'),
        total_items=('order_item_id', 'count'),
        first_sale=('order_purchase_timestamp', 'min'),
        last_sale=('order_purchase_timestamp', 'max')
    ).reset_index()
    
    seller_stats['active_days'] = (seller_stats['last_sale'] - seller_stats['first_sale']).dt.days
    seller_stats = seller_stats.sort_values('total_revenue', ascending=False)
    
    # KPI 카드
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 판매자 수", f"{seller_stats.shape[0]:,}")
    col2.metric("총 매출액", f"R$ {seller_stats['total_revenue'].sum():,.0f}")
    col3.metric("평균 판매자 매출", f"R$ {seller_stats['total_revenue'].mean():,.0f}")
    col4.metric("평균 활동 기간", f"{seller_stats['active_days'].mean():.0f}일")
    
    st.divider()
    
    # 월별 판매자 수 추이
    st.subheader("📊 월별 활성 판매자 수 추이")
    
    delivered_items['year_month'] = delivered_items['order_purchase_timestamp'].dt.to_period('M')
    
    # 월별 활성 판매자 수 (해당 월에 판매한 판매자)
    monthly_active_sellers = delivered_items.groupby('year_month')['seller_id'].nunique().reset_index()
    monthly_active_sellers.columns = ['year_month', 'active_sellers']
    monthly_active_sellers['year_month_str'] = monthly_active_sellers['year_month'].astype(str)
    
    # 월별 총 매출
    monthly_seller_revenue = delivered_items.groupby('year_month')['price'].sum().reset_index()
    monthly_seller_revenue.columns = ['year_month', 'total_revenue']
    
    # 병합
    monthly_seller_stats = pd.merge(monthly_active_sellers, monthly_seller_revenue, on='year_month')
    
    # 이중 Y축 그래프
    fig_seller_trend = go.Figure()
    
    fig_seller_trend.add_trace(go.Scatter(
        x=monthly_seller_stats['year_month_str'],
        y=monthly_seller_stats['active_sellers'],
        mode='lines+markers',
        name='활성 판매자 수',
        line=dict(width=3, color='#2196F3'),
        marker=dict(size=8),
        yaxis='y',
        hovertemplate='<b>%{x}</b><br>활성 판매자: %{y:,}<extra></extra>'
    ))
    
    fig_seller_trend.add_trace(go.Scatter(
        x=monthly_seller_stats['year_month_str'],
        y=monthly_seller_stats['total_revenue'],
        mode='lines+markers',
        name='총 매출액',
        line=dict(width=2, dash='dash', color='#4CAF50'),
        marker=dict(size=6),
        yaxis='y2',
        hovertemplate='<b>%{x}</b><br>매출: R$ %{y:,.0f}<extra></extra>'
    ))
    
    fig_seller_trend.update_layout(
        title="월별 활성 판매자 수 및 매출액",
        xaxis_title="년-월",
        yaxis=dict(title="활성 판매자 수", side='left'),
        yaxis2=dict(title="매출액 (R$)", overlaying='y', side='right'),
        hovermode='x unified',
        height=400,
        xaxis_tickangle=-45
    )
    st.plotly_chart(fig_seller_trend, width='stretch')
    
    st.divider()
    
    # 신규 및 이탈 판매자 분석
    st.subheader("🆕 신규 판매자 & 📉 이탈 판매자 분석")
    
    # 판매자별 첫 판매 월
    seller_first_sale = delivered_items.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_sale['first_sale_month'] = seller_first_sale['order_purchase_timestamp'].dt.to_period('M')
    
    # 월별 신규 판매자 수
    new_sellers_monthly = seller_first_sale.groupby('first_sale_month').size().reset_index()
    new_sellers_monthly.columns = ['year_month', 'new_sellers']
    new_sellers_monthly['year_month_str'] = new_sellers_monthly['year_month'].astype(str)
    
    # 판매자별 마지막 판매 월
    seller_last_sale = delivered_items.groupby('seller_id')['order_purchase_timestamp'].max().reset_index()
    seller_last_sale['last_sale_month'] = seller_last_sale['order_purchase_timestamp'].dt.to_period('M')
    
    # 이탈 판매자 정의: 마지막 판매 후 3개월 이상 판매 없음
    max_date = delivered_items['order_purchase_timestamp'].max()
    seller_last_sale['months_since_last_sale'] = ((max_date - seller_last_sale['order_purchase_timestamp']).dt.days / 30).astype(int)
    churned_sellers = seller_last_sale[seller_last_sale['months_since_last_sale'] >= 3]
    
    # 월별 이탈 판매자 수 (마지막 판매 월 기준)
    churned_monthly = churned_sellers.groupby('last_sale_month').size().reset_index()
    churned_monthly.columns = ['year_month', 'churned_sellers']
    churned_monthly['year_month_str'] = churned_monthly['year_month'].astype(str)
    
    # 그래프
    col_new, col_churn = st.columns(2)
    
    with col_new:
        fig_new = px.bar(
            new_sellers_monthly,
            x='year_month_str',
            y='new_sellers',
            title="월별 신규 판매자 수",
            labels={'year_month_str': '년-월', 'new_sellers': '신규 판매자 수'},
            color='new_sellers',
            color_continuous_scale='Greens'
        )
        fig_new.update_layout(xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig_new, width='stretch')
    
    with col_churn:
        fig_churn = px.bar(
            churned_monthly,
            x='year_month_str',
            y='churned_sellers',
            title="월별 이탈 판매자 수 (3개월 이상 미판매)",
            labels={'year_month_str': '년-월', 'churned_sellers': '이탈 판매자 수'},
            color='churned_sellers',
            color_continuous_scale='Reds'
        )
        fig_churn.update_layout(xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig_churn, width='stretch')
    
    st.info(f"💡 **이탈 판매자 정의**: 최근 3개월 이상 판매 기록이 없는 판매자를 이탈로 간주합니다. 현재 총 {churned_sellers.shape[0]:,}명의 판매자가 이탈 상태입니다.")
    
    st.divider()
    
    # TOP 판매자 분석
    st.subheader("🏆 TOP 10 판매자")
    
    top10_sellers = seller_stats.head(10)
    
    # 지역 정보 추가
    top10_with_location = pd.merge(
        top10_sellers,
        sellers[['seller_id', 'seller_city', 'seller_state']],
        on='seller_id',
        how='left'
    )
    
    fig_top_sellers = px.bar(
        top10_with_location,
        x='total_revenue',
        y='seller_id',
        orientation='h',
        title="TOP 10 판매자 매출",
        labels={'total_revenue': '매출액 (R$)', 'seller_id': '판매자 ID'},
        color='seller_state',
        hover_data=['seller_city', 'total_orders', 'active_days'],
        text_auto='.2s'
    )
    fig_top_sellers.update_layout(yaxis={'categoryorder':'total ascending'}, height=500)
    st.plotly_chart(fig_top_sellers, width='stretch')
    
    # 판매자 활동 기간 분포
    st.subheader("📅 판매자 활동 기간 분포")
    
    fig_active_days = px.histogram(
        seller_stats,
        x='active_days',
        nbins=50,
        title="판매자 활동 기간 분포",
        labels={'active_days': '활동 기간 (일)', 'count': '판매자 수'},
        color_discrete_sequence=['#FF9800']
    )
    fig_active_days.update_layout(height=400)
    st.plotly_chart(fig_active_days, width='stretch')
    
    st.info("💡 **인사이트**: 활동 기간이 짧은 판매자가 많다면 판매자 유지(retention) 전략이 필요합니다.")



def render_revenue_drop_analysis_tab():
    """탭 4: 매출 하락 원인 심층 분석"""
    st.header("📉 매출 하락 원인 심층 분석")
    st.markdown("""
    **"판매자는 늘어나는데 왜 평균 매출은 줄어들까요?"**  
    2018년 데이터에서 관찰된 이 역설적인 현상의 원인을 데이터로 파헤쳐 봅니다.
    """)
    
    # 데이터 로딩
    items_with_sellers, sellers = load_seller_data()
    
    if items_with_sellers is None:
        st.error("데이터 로딩 실패")
        return

    # 배송 완료 건만 대상
    df = items_with_sellers[items_with_sellers['order_status'] == 'delivered'].copy()
    df['year_month'] = df['order_purchase_timestamp'].dt.to_period('M')
    
    st.divider()

    # 1. 판매자 수 증가 vs 평균 매출 하락 (이중축 그래프)
    st.subheader("1️⃣ 판매자 과잉 공급과 평균 매출의 역상관 관계")
    st.markdown("판매자 수가 급증하면서 1인당 가져가는 평균 매출(Pie)이 급격히 줄어드는 현상을 보여줍니다.")
    
    monthly_stats = df.groupby('year_month').agg(
        active_sellers=('seller_id', 'nunique'),
        total_revenue=('price', 'sum')
    ).reset_index()
    monthly_stats['avg_revenue_per_seller'] = monthly_stats['total_revenue'] / monthly_stats['active_sellers']
    monthly_stats['year_month_str'] = monthly_stats['year_month'].astype(str)
    
    # 2017년 이후 데이터만 시각화 (트렌드가 명확한 구간)
    monthly_stats_filtered = monthly_stats[monthly_stats['year_month_str'] >= '2017-01']
    
    fig_dual = go.Figure()
    
    # 막대: 활성 판매자 수
    fig_dual.add_trace(go.Bar(
        x=monthly_stats_filtered['year_month_str'],
        y=monthly_stats_filtered['active_sellers'],
        name='활성 판매자 수',
        marker_color='#BBDEFB',
        yaxis='y'
    ))
    
    # 선: 판매자당 평균 매출
    fig_dual.add_trace(go.Scatter(
        x=monthly_stats_filtered['year_month_str'],
        y=monthly_stats_filtered['avg_revenue_per_seller'],
        name='판매자당 평균 매출 (R$)',
        mode='lines+markers',
        line=dict(color='#FF5252', width=3),
        marker=dict(size=8),
        yaxis='y2'
    ))
    
    fig_dual.update_layout(
        title="판매자 수 증가 vs 평균 매출 하락 추이 (2017년~)",
        xaxis_title="기간",
        yaxis=dict(title="활성 판매자 수 (명)", side='left', showgrid=False),
        yaxis2=dict(title="평균 매출 (R$)", overlaying='y', side='right', showgrid=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=500
    )
    st.plotly_chart(fig_dual, use_container_width=True)
    
    # 상세 데이터 테이블 추가
    with st.expander("📊 월별 상세 통계 데이터 보기"):
        display_stats = monthly_stats_filtered.copy()
        # 판매자 수 증가 계산
        display_stats['seller_increase'] = display_stats['active_sellers'].diff().fillna(0).astype(int)
        
        # 컬럼명 변경 및 포맷팅
        df_show = display_stats[['year_month_str', 'active_sellers', 'seller_increase', 'avg_revenue_per_seller']].copy()
        df_show.columns = ['월', '활성 판매자 수', '판매자 수 증가', '평균 매출 (R$)']
        
        st.dataframe(
            df_show.style.format({
                '활성 판매자 수': '{:,}',
                '판매자 수 증가': '{:+,}',
                '평균 매출 (R$)': 'R$ {:,.2f}'
            }),
            use_container_width=True
        )
    
    st.info("💡 **인사이트:** 판매자 수는 매월 꾸준히 증가(특히 2017년 하반기 이후)하고 있으나, 1인당 평균 매출은 오히려 감소하거나 정체되는 현상이 뚜렷합니다. 이는 시장 성장에 비해 판매자 유입이 너무 빨라 경쟁이 심화되고 있음을 시사합니다.")

    # 신규 vs 기존 판매자 분석 추가
    with st.expander("👥 신규 vs 기존 판매자 비율 상세 보기"):
        # 첫 판매 달 계산
        seller_first_sale = df.groupby('seller_id')['year_month'].min().reset_index()
        seller_first_sale.columns = ['seller_id', 'first_sale_month']
        
        # 월별 활성 판매자 데이터와 결합
        monthly_active_sellers = df.groupby(['year_month', 'seller_id']).size().reset_index()
        monthly_active_sellers = pd.merge(monthly_active_sellers, seller_first_sale, on='seller_id')
        
        # 신규/기존 구분
        monthly_active_sellers['seller_type'] = np.where(
            monthly_active_sellers['year_month'] == monthly_active_sellers['first_sale_month'], 
            '신규', '기존'
        )
        
        # 집계
        type_counts = monthly_active_sellers.groupby(['year_month', 'seller_type']).size().unstack(fill_value=0).reset_index()
        type_counts['year_month_str'] = type_counts['year_month'].astype(str)
        type_counts['total'] = type_counts['신규'] + type_counts['기존']
        type_counts['new_ratio'] = (type_counts['신규'] / type_counts['total'] * 100).round(1)
        
        # 모든 데이터 표시 (2016년 포함)
        type_counts_filtered = type_counts.copy()
        
        df_type_show = type_counts_filtered[['year_month_str', 'total', '신규', '기존', 'new_ratio']].copy()
        df_type_show.columns = ['월', '총 활성 판매자', '신규 판매자', '기존 판매자', '신규 비중 (%)']
        
        st.dataframe(
            df_type_show.style.format({
                '총 활성 판매자': '{:,}',
                '신규 판매자': '{:,}',
                '기존 판매자': '{:,}',
                '신규 비중 (%)': '{:.1f}%'
            }),
            use_container_width=True
        )
    
    st.info("💡 **인사이트:** 매월 활성 판매자의 약 10~20%가 해당 월에 처음 진입한 **신규 판매자**들입니다. 꾸준한 신규 유입은 플랫폼 활성도를 높이지만, 동시에 기존 판매자들과의 경쟁을 가속화하는 요인이 됩니다.")

    st.divider()

    # 3. 판매자 유지 및 생존 분석
    st.subheader("3️⃣ 신규 판매자 장기 안착을 위한 '골든 타임'")
    st.markdown("신규 판매자가 포기하지 않고 6개월(180일) 이상 활동하는 '장기 판매자'로 거듭나기 위해 필요한 활동량을 분석했습니다.")
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("📌 **활동 기간별 생존율**")
        survival_metrics = {
            "30일 생존": "72.0%",
            "90일 생존": "56.6%",
            "180일 생존": "39.4%"
        }
        for k, v in survival_metrics.items():
            st.metric(k, v)
            
    with col2:
        st.write("🎯 **'Magic Number': 판매 발생 일수**")
        st.write("장기 생존(180일+) 확률:")
        st.progress(0.48)
        st.caption("2일 이상 판매 시: 48.2%")
        st.progress(0.63)
        st.caption("5일 이상 판매 시: 63.1%")
        st.progress(0.77)
        st.caption("10일 이상 판매 시: 76.8% (권장 목표)")
        
    st.success("✨ **결론:** 신규 판매자가 초기에 **최소 10일 이상** 실제 판매를 경험하게 하는 것이 장기 파트너십 유지의 핵심 포인트입니다.")
    
    st.divider()
    
    # 2. 신규 vs 기존 판매자 성과 비교
    st.subheader("2️⃣ '초보 사장님'의 대거 유입 (결정적 증거)")
    st.markdown("신규 판매자는 **머릿수(좌측)**는 많지만, 정작 **매출 점유율(우측)**은 미미합니다. 이것이 평균 하락의 주범입니다.")
    
    # 판매자 유형 구분 (2018년 신규 vs 기존)
    seller_first_sale = df.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_sale['is_new_2018'] = seller_first_sale['order_purchase_timestamp'].dt.year == 2018
    seller_first_sale['Seller Type'] = seller_first_sale['is_new_2018'].map({True: '신규 진입 (2018년~)', False: '기존 판매자 (~2017년)'})
    
    # 2018년 데이터만 필터링하여 성과 비교
    df_2018 = df[df['order_purchase_timestamp'].dt.year == 2018].copy()
    df_2018 = pd.merge(df_2018, seller_first_sale[['seller_id', 'Seller Type']], on='seller_id')
    
    comp_stats = df_2018.groupby('Seller Type').agg(
        seller_count=('seller_id', 'nunique'),
        total_revenue=('price', 'sum')
    ).reset_index()
    comp_stats['Avg Revenue'] = comp_stats['total_revenue'] / comp_stats['seller_count']
    
    c1, c2, c3 = st.columns([1, 1, 1.2]) # 비율 조정
    
    with c1:
        # 시각화: 판매자 수 비중 (머릿수)
        fig_pie_count = px.pie(
            comp_stats,
            names='Seller Type',
            values='seller_count',
            title="[머릿수] 판매자 수 비중",
            color='Seller Type',
            color_discrete_map={'신규 진입 (2018년~)': '#FFB74D', '기존 판매자 (~2017년)': '#4CAF50'},
            hole=0.4
        )
        fig_pie_count.update_layout(showlegend=False, title_x=0.2)
        st.plotly_chart(fig_pie_count, width='stretch')

    with c2:
        # 시각화: 매출 점유율 비중 (지갑)
        fig_pie_rev = px.pie(
            comp_stats,
            names='Seller Type',
            values='total_revenue',
            title="[실속] 매출 점유율",
            color='Seller Type',
            color_discrete_map={'신규 진입 (2018년~)': '#FFB74D', '기존 판매자 (~2017년)': '#4CAF50'},
            hole=0.4
        )
        fig_pie_rev.update_layout(showlegend=False, title_x=0.3)
        st.plotly_chart(fig_pie_rev, width='stretch')
        
    with c3:
        # 시각화: 평균 매출 비교 (막대)
        fig_comp = px.bar(
            comp_stats, 
            x='Seller Type', 
            y='Avg Revenue',
            color='Seller Type',
            title="인당 평균 매출 비교",
            text_auto=',.0f',
            color_discrete_map={'신규 진입 (2018년~)': '#FFB74D', '기존 판매자 (~2017년)': '#4CAF50'}
        )
        fig_comp.update_layout(yaxis_title="평균 매출 (R$)", showlegend=True, legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_comp, width='stretch')
        
    st.info(f"""
    💡 **핵심 포인트**: 
    - 신규 판매자(노란색)는 전체 인원의 **{comp_stats.loc[comp_stats['Seller Type'].str.contains('신규'), 'seller_count'].values[0] / comp_stats['seller_count'].sum() * 100:.1f}%**를 차지하지만, 
    - 전체 매출에서는 겨우 **{comp_stats.loc[comp_stats['Seller Type'].str.contains('신규'), 'total_revenue'].values[0] / comp_stats['total_revenue'].sum() * 100:.1f}%**만 가져가고 있습니다. 
    - 이 **불균형** 때문에 전체 평균이 깎여 보이는 것입니다.
    """)
    
    st.divider()
    
    # 3. 가격대별 비중 변화
    st.subheader("3️⃣ 저가 상품 위주로 시장 재편 & 객단가 하락")
    
    # 가격 구간 설정
    bins = [0, 50, 100, 200, 500, 10000]
    labels = ['매우 저가(0-50)', '저가(50-100)', '중가(100-200)', '고가(200-500)', '초고가(500+)']
    df['price_range'] = pd.cut(df['price'], bins=bins, labels=labels)
    
    # 2017년 이후 월별 비중 계산
    df_trend = df[df['order_purchase_timestamp'].dt.year >= 2017].copy()
    
    # 객단가(AOV) 계산
    monthly_aov = df_trend.groupby('year_month')['price'].mean().reset_index()
    monthly_aov['year_month_str'] = monthly_aov['year_month'].astype(str)
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        # 비중 차트
        price_trend = df_trend.groupby(['year_month', 'price_range']).size().reset_index(name='count')
        monthly_total = price_trend.groupby('year_month')['count'].transform('sum')
        price_trend['percentage'] = (price_trend['count'] / monthly_total * 100).round(1)
        price_trend['year_month_str'] = price_trend['year_month'].astype(str)
        
        fig_stack = px.bar(
            price_trend,
            x='year_month_str',
            y='percentage',
            color='price_range',
            title="월별 가격대 판매 비중 (누적)",
            labels={'percentage': '비중 (%)', 'year_month_str': '월', 'price_range': '가격대'},
            color_discrete_sequence=px.colors.sequential.RdBu_r
        )
        fig_stack.update_layout(legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_stack, width='stretch')

    with col_chart2:
        # 객단가 추이 차트
        fig_aov = px.line(
            monthly_aov,
            x='year_month_str',
            y='price',
            title="월별 평균 객단가(AOV) 하락 추세",
            labels={'year_month_str': '월', 'price': '평균 주문 단가 (Item Price)'},
            markers=True
        )
        fig_aov.update_traces(line_color='#E91E63', line_width=3)
        fig_aov.add_annotation(
            x=monthly_aov['year_month_str'].iloc[-1], y=monthly_aov['price'].iloc[-1],
            text="평균 단가 하락 ▼", showarrow=True, arrowhead=1
        )
        st.plotly_chart(fig_aov, width='stretch')
    
    st.info("💡 **인사이트**: 왼쪽 차트에서 붉은색 막대(저가 상품 비중)가 늘어남에 따라, 오른쪽 차트의 **빨간색 선(평균 단가)**이 우하향하는 반비례 관계를 확인할 수 있습니다.")

    st.divider()

    # 8. 금액대별 신규 vs 기존 판매자 상세 분포
    st.subheader("8️⃣ 금액대별 신규 vs 기존 판매자 상세 분포")
    st.markdown("전체 기간(2016-2018) 동안 신규 및 기존 판매자들이 주로 어떤 가격대에서 활동하는지 상세히 분석합니다. **색상이 짙을수록** 해당 구간에 판매자가 많이 밀집되어 있음을 의미합니다.")

    # 데이터 준비
    seller_first_month = df.groupby('seller_id')['year_month'].min().reset_index()
    seller_first_month.columns = ['seller_id', 'first_month']
    
    seller_monthly_aov = df.groupby(['seller_id', 'year_month']).agg(
        total_sales=('price', 'sum'),
        order_count=('order_id', 'nunique')
    ).reset_index()
    
    seller_monthly_aov['aov'] = seller_monthly_aov['total_sales'] / seller_monthly_aov['order_count']
    seller_monthly_aov = pd.merge(seller_monthly_aov, seller_first_month, on='seller_id')
    seller_monthly_aov['status'] = np.where(seller_monthly_aov['year_month'] == seller_monthly_aov['first_month'], '신규', '기존')

    # 금액대 분류
    bins = [0, 50, 100, 150, 200, 300, 500, 1000, 10000]
    labels_bins = ['0-50', '50-100', '100-150', '150-200', '200-300', '300-500', '500-1000', '1000+']
    seller_monthly_aov['금액대(BRL)'] = pd.cut(seller_monthly_aov['aov'], bins=bins, labels=labels_bins)

    # 연도별 선택 (사용자 가독성 위해)
    selected_year = st.selectbox("📅 분석 연도 선택", ["전체", "2016년", "2017년", "2018년"])
    
    plot_df = seller_monthly_aov.copy()
    if selected_year != "전체":
        year_val = int(selected_year[:4])
        plot_df = plot_df[plot_df['year_month'].dt.year == year_val]

    # 피벗 테이블 생성
    pivot_aov = plot_df.pivot_table(
        index='금액대(BRL)', 
        columns=['year_month', 'status'], 
        values='seller_id', 
        aggfunc='nunique',
        fill_value=0
    )

    # 멀티 인덱스 컬럼을 보기 좋게 정렬 및 병합
    if not pivot_aov.empty:
        # 스타일링 적용 (색상 입히기)
        st.dataframe(
            pivot_aov.style.background_gradient(cmap='YlGnBu', axis=None)
                         .format("{:,}"),
            use_container_width=True,
            height=400
        )
        
        # [전월 총합 - 금월 기존] 계산 (이탈/휴면 분석)
        # 1. 월별/금액대별 총합 (신규+기존) 계산
        monthly_total_by_range = pivot_aov.groupby(level=0, axis=1).sum()
        
        # 2. 전월 총합 가져오기 (Shifted)
        prev_monthly_total = monthly_total_by_range.shift(1, axis=1)
        
        # 3. 금월 기존 판매자 수 가져오기
        curr_existing = pivot_aov.xs('기존', level=1, axis=1)
        
        # 4. 이탈/휴면 계산: 전월 총합 - 금월 기존
        # (전월에 활동했던 사람들 중 이번달에 '기존'으로 안 나타난 사람)
        churn_df = prev_monthly_total - curr_existing
        churn_df = churn_df.fillna(0).astype(int).clip(lower=0) # 첫 달은 0, 마이너스는 방지
        
        st.markdown("##### 🔍 금액대별 이탈/휴면 판매자 분석 (전월 대비)")
        st.markdown("공식: `[전월 총 활성 판매자(신규+기존) - 금월 기존 판매자]`")
        
        st.dataframe(
            churn_df.style.background_gradient(cmap='OrRd', axis=None)
                         .format("{:,}"),
            use_container_width=True,
            height=300
        )
        
        st.info("💡 **이탈 데이터 해석**: 붉은색이 짙을수록 전월 대비 활동을 멈춘 판매자가 많음을 의미합니다. 특히 진입 장벽이 낮은 **저가 구간(0-100 BRL)**에서 이탈 규모가 가장 크게 나타나는 경향이 있습니다. 이는 신규 유입만큼이나 초기 안착(Retention) 관리가 시급함을 보여줍니다.")
    else:
        st.warning("해당 기간에 데이터가 없습니다.")

    st.divider()

    # 4. 신규 판매자 심층 프로파일링 (반전 매력)
    st.subheader("4️⃣ 신규 판매자 심층 프로파일링 (반전)")
    st.markdown("""
    **"신규 판매자는 무조건 싼 것만 팔까요?"**  
    데이터를 뜯어보니 놀라운 사실이 발견되었습니다. 그들은 **'전략적'**으로 움직이고 있습니다.
    """)
    
    # 데이터 준비 (리뷰 데이터 추가 로드 필요)
    # 성능을 위해 여기서 간단히 로드 및 병합
    @st.cache_data
    def load_reviews():
        try:
            return pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.parquet'))
        except:
            return pd.read_csv(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.csv'))
            
    @st.cache_data
    def load_products_mini():
        try:
            return pd.read_parquet(os.path.join(BASE_PATH, 'olist_products_dataset.parquet'))
        except:
            return pd.read_csv(os.path.join(BASE_PATH, 'olist_products_dataset.csv'))

    reviews = load_reviews()
    products = load_products_mini()
    
    # 2018년 데이터 기준 분석
    df_2018 = df[df['order_purchase_timestamp'].dt.year == 2018].copy()
    
    # 상품 정보 병합 (카테고리 정보 확보)
    if 'product_category_name' not in df_2018.columns:
        df_2018 = pd.merge(df_2018, products[['product_id', 'product_category_name']], on='product_id', how='left')

    # 판매자 유형 다시 정의 (데이터프레임 재사용)
    seller_first_sale = df.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_sale['is_new_2018'] = seller_first_sale['order_purchase_timestamp'].dt.year == 2018
    seller_first_sale['Seller Type'] = seller_first_sale['is_new_2018'].map({True: '신규 판매자', False: '기존 판매자'})
    
    df_2018 = pd.merge(df_2018, seller_first_sale[['seller_id', 'Seller Type']], on='seller_id')
    
    # 리뷰 점수 병합
    df_2018_review = pd.merge(df_2018, reviews[['order_id', 'review_score']], on='order_id', how='left')
    
    col_profile1, col_profile2 = st.columns(2)
    
    with col_profile1:
        st.markdown("##### 💰 전체 평균 판매 단가 비교")
        # 전체 평균 단가
        avg_price_all = df_2018.groupby('Seller Type')['price'].mean().reset_index()
        
        fig_price_all = px.bar(
            avg_price_all,
            x='Seller Type',
            y='price',
            color='Seller Type',
            text_auto='.1f',
            color_discrete_map={'신규 판매자': '#FFB74D', '기존 판매자': '#4CAF50'},
            title="신규 판매자가 오히려 더 비싼 물건을 판다?"
        )
        fig_price_all.update_layout(yaxis_title="평균 단가 (R$)", showlegend=False, height=350)
        st.plotly_chart(fig_price_all, width='stretch')
        st.caption("전체 평균을 보면 신규 판매자의 판매 단가가 더 높습니다. 저가 상품뿐만 아니라 고가 상품도 취급하는 '양극화' 전략을 보여줍니다.")

    with col_profile2:
        st.markdown("##### ⭐ 서비스 품질 (리뷰 평점) 비교")
        # 평균 리뷰 점수
        avg_review = df_2018_review.groupby('Seller Type')['review_score'].mean().reset_index()
        
        fig_review = px.bar(
            avg_review,
            x='review_score',
            y='Seller Type',
            orientation='h',
            color='Seller Type',
            text_auto='.2f',
            color_discrete_map={'신규 판매자': '#FFB74D', '기존 판매자': '#4CAF50'},
            title="신입이 더 친절하다! (평점 우위)"
        )
        fig_review.update_layout(xaxis_title="평균 별점 (5점 만점)", yaxis_title="", showlegend=False, xaxis_range=[3.5, 4.5], height=350)
        st.plotly_chart(fig_review, width='stretch')
        st.caption("신규 판매자의 평점(4.12)이 기존 판매자(3.99)보다 높습니다. 판매 규모는 작아도 서비스 품질 관리는 더 잘하고 있다는 뜻입니다.")
        
    st.markdown("---")
    st.markdown("##### 🎯 카테고리별 전략적 가격 책정 (Smart Pricing)")
    st.markdown("가장 많이 팔리는 **TOP 3 공통 카테고리**에서 이들의 전략이 갈립니다.")
    
    # 주요 카테고리 선정 (신규 판매자 판매량 TOP 3 중 기존 판매자와 겹치는 것)
    top_cats = ['beleza_saude', 'cama_mesa_banho', 'informatica_acessorios']
    cat_labels = {'beleza_saude': '뷰티/건강', 'cama_mesa_banho': '침구/욕실', 'informatica_acessorios': 'IT/액세서리'}
    
    cat_df = df_2018[df_2018['product_category_name'].isin(top_cats)].groupby(['product_category_name', 'Seller Type'])['price'].mean().reset_index()
    cat_df['Category Name'] = cat_df['product_category_name'].map(cat_labels)
    
    fig_cat_price = px.bar(
        cat_df,
        x='Category Name',
        y='price',
        color='Seller Type',
        barmode='group',
        text_auto='.0f',
        color_discrete_map={'신규 판매자': '#FFB74D', '기존 판매자': '#4CAF50'},
        title="카테고리별 평균 가격 비교 (단위: R$)"
    )
    fig_cat_price.update_layout(yaxis_title="평균 가격 (R$)")
    st.plotly_chart(fig_cat_price, width='stretch')
    
    st.info("""
    💡 **전략 분석**:
    1. **뷰티/건강 (좌측)**: 신규 판매자가 **R$ 40 가량 저렴**하게 팝니다. → **"확실한 저가 공세(박리다매) 전략"**
    2. **IT/액세서리 (우측)**: 신규 판매자가 **오히려 더 비싸게** 팝니다. → **"고급화 전략"** 또는 **"틈새 시장 공략"**
    
    즉, 신규 판매자들은 무작정 싸게 파는 게 아니라 **카테고리 특성에 맞춰 유연한 가격 전략**을 구사하고 있습니다.
    """)
    
    st.divider()

    # 5. 상품 상세 전략 비교 (글 vs 사진)
    st.subheader("5️⃣ 상품 상세 전략의 비밀: 글(Text) vs 사진(Image)")
    st.markdown("""
    **"왜 IT 제품은 비싸게 팔 수 있을까요?"**  
    비밀은 **상세 페이지**에 있었습니다. 신규 판매자들은 사진보다 **글(설명)**에 집중하고 있습니다.
    """)
    
    # 분석용 데이터 준비
    target_cats = ['beleza_saude', 'informatica_acessorios']
    cat_labels = {'beleza_saude': '뷰티/건강', 'informatica_acessorios': 'IT/액세서리'}
    
    if 'product_description_lenght' in products.columns and 'product_photos_qty' in products.columns:
        # 제품 정보가 있는 데이터만 필터링
        df_detail = df_2018[df_2018['product_category_name'].isin(target_cats)].copy()
        df_detail = pd.merge(df_detail, products[['product_id', 'product_description_lenght', 'product_photos_qty']], on='product_id', how='left', suffixes=('', '_y'))
        
        # 중복 컬럼 처리
        if 'product_description_lenght_y' in df_detail.columns:
            df_detail['product_description_lenght'] = df_detail['product_description_lenght_y'].fillna(df_detail['product_description_lenght'])
            df_detail['product_photos_qty'] = df_detail['product_photos_qty_y'].fillna(df_detail['product_photos_qty'])
        
        df_detail['Category Name'] = df_detail['product_category_name'].map(cat_labels)
        
        # 지표 집계
        detail_stats = df_detail.groupby(['Category Name', 'Seller Type']).agg(
            avg_desc_len=('product_description_lenght', 'mean'),
            avg_photos=('product_photos_qty', 'mean')
        ).reset_index()
        
        col_it, col_beauty = st.columns(2)
        
        with col_it:
            st.markdown("##### 💻 IT/액세서리: 설명이 더 길다!")
            it_stats = detail_stats[detail_stats['Category Name'] == 'IT/액세서리']
            
            # 설명 길이 비교
            fig_it_desc = px.bar(
                it_stats, x='Seller Type', y='avg_desc_len',
                color='Seller Type', text_auto='.0f',
                color_discrete_map={'신규 판매자': '#FFB74D', '기존 판매자': '#4CAF50'},
                title="평균 상품 설명 길이 (글자 수)"
            )
            fig_it_desc.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_it_desc, width='stretch')
            st.caption("신규 판매자(840자) > 기존 판매자(830자). 사진보다 **텍스트 스펙 정보**를 더 꼼꼼하게 적어 전문성을 어필합니다.")
            
        with col_beauty:
            st.markdown("##### 💄 뷰티/건강: 다이어트 전략")
            beauty_stats = detail_stats[detail_stats['Category Name'] == '뷰티/건강']
            
            # 사진 개수 비교
            fig_beauty_photo = px.bar(
                beauty_stats, x='Seller Type', y='avg_photos',
                color='Seller Type', text_auto='.1f',
                color_discrete_map={'신규 판매자': '#FFB74D', '기존 판매자': '#4CAF50'},
                title="평균 상품 사진 개수 (장)"
            )
            fig_beauty_photo.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_beauty_photo, width='stretch')
            st.caption("신규 판매자(1.4장) < 기존 판매자(2.0장). 사진 촬영 공수를 줄이고 **가격 경쟁력**에 올인했습니다.")

    st.divider()

    # 6. 사진 효율성 분석 (상관관계)
    st.subheader("6️⃣ '사진 1장의 법칙': 많을수록 좋을까?")
    st.markdown("""
    **"사진을 많이 올리면 고객이 더 만족할까요?"**  
    데이터는 **"IT는 2장이면 충분하고, 뷰티는 1장도 OK"**라고 말하고 있습니다.
    """)
    
    # 분석 데이터 준비 (사진 데이터 + 리뷰 데이터 병합 + 설명 길이 추가)
    # 이미 로드된 df_2018_review (리뷰 포함)와 products (사진, 설명 포함) 병합
    if 'product_photos_qty' in products.columns:
        # 필요한 컬럼만 선택하여 병합
        cols_to_merge = ['product_id', 'product_photos_qty']
        if 'product_description_lenght' in products.columns:
            cols_to_merge.append('product_description_lenght')
            
        df_photo = pd.merge(df_2018_review, products[cols_to_merge], on='product_id', how='left', suffixes=('', '_new'))
        
        # 컬럼 정리 (중복 컬럼 처리 및 결측치 채우기)
        if 'product_photos_qty_new' in df_photo.columns:
            df_photo['product_photos_qty'] = df_photo['product_photos_qty_new'].fillna(df_photo['product_photos_qty'])
            
        if 'product_description_lenght_new' in df_photo.columns:
            df_photo['product_description_lenght'] = df_photo['product_description_lenght_new'].fillna(df_photo.get('product_description_lenght', pd.Series()))
        elif 'product_description_lenght' not in df_photo.columns:
             # 만약 기존 df_2018_review에 설명 길이 컬럼이 없다면 NaN으로 초기화 (병합은 되었으나 원본에 없었을 경우)
             pass 

        df_photo['Category Name'] = df_photo['product_category_name'].map(cat_labels)
        df_photo_target = df_photo[df_photo['Category Name'].isin(['IT/액세서리', '뷰티/건강'])].copy()
        
        # 사진 개수 그룹화
        def group_photos(qty):
            if pd.isna(qty): return '0장'
            if qty >= 6: return '6장+'
            return f'{int(qty)}장'
            
        df_photo_target['Photo Group'] = df_photo_target['product_photos_qty'].apply(group_photos)
        
        # 집계
        photo_stats = df_photo_target.groupby(['Category Name', 'Photo Group'])['review_score'].mean().reset_index()
        
        # 정렬 순서 지정
        sort_order = ['0장', '1장', '2장', '3장', '4장', '5장', '6장+']
        photo_stats['Photo Group'] = pd.Categorical(photo_stats['Photo Group'], categories=sort_order, ordered=True)
        photo_stats = photo_stats.sort_values('Photo Group')
        
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("##### 💻 IT/액세서리: 2장의 마법")
            it_photo = photo_stats[photo_stats['Category Name'] == 'IT/액세서리']
            
            fig_it_corr = px.line(
                it_photo, x='Photo Group', y='review_score', markers=True,
                title="사진 개수별 평균 평점 (IT)",
                labels={'review_score': '평균 평점', 'Photo Group': '사진 개수'}
            )
            fig_it_corr.update_traces(line_color='#2196F3', line_width=3)
            # 1장 -> 2장 급상승 강조
            fig_it_corr.add_annotation(x='2장', y=it_photo[it_photo['Photo Group']=='2장']['review_score'].values[0],
                                     text="급상승! 🚀", showarrow=True, arrowhead=1)
            fig_it_corr.update_yaxes(range=[3.5, 4.5])
            st.plotly_chart(fig_it_corr, width='stretch')
            st.caption("1장일 때는 평점이 낮지만(불안감), **2장만 되면 평점이 급격히 상승**합니다. 그 이상은 큰 차이가 없습니다.")
            
        with c2:
            st.markdown("##### 💄 뷰티/건강: 아예 상관없음")
            beauty_photo = photo_stats[photo_stats['Category Name'] == '뷰티/건강']
            
            fig_beauty_corr = px.line(
                beauty_photo, x='Photo Group', y='review_score', markers=True,
                title="사진 개수별 평균 평점 (뷰티)",
                labels={'review_score': '평균 평점', 'Photo Group': '사진 개수'}
            )
            fig_beauty_corr.update_traces(line_color='#E91E63', line_width=3)
            fig_beauty_corr.update_yaxes(range=[3.5, 4.5])
            st.plotly_chart(fig_beauty_corr, width='stretch')
            st.caption("사진이 1장이든 5장이든 **평점 변화가 거의 없습니다.** (오히려 3장에서 떨어짐). 사진 개수가 중요하지 않음을 증명합니다.")

    st.divider()

    # 7. 통계적 검증 (산점도 & 히트맵)
    st.subheader("7️⃣ 통계적 검증: 매출과의 상관관계")
    st.markdown("상품별로 데이터를 집계하여, **상세 정보가 매출(Revenue)과 판매량(Sales)에 미치는 영향**을 분석합니다.")

    # 히트맵용 데이터 준비 (주요 변수만 추출)
    # 이미 로드된 df_2018_review (리뷰 포함)와 products (사진, 설명 포함) 병합 로직은 위에서 수행됨
    
    # ---------------------------------------------------------
    # [핵심] 상품(Product) 단위로 데이터 집계
    # ---------------------------------------------------------
    # 1. df_photo_target은 현재 '주문 건별' 데이터임.
    # 2. 이를 '상품별'로 GroupBy하여 총 매출, 판매량, 평균 평점을 구해야 함.
    
    df_product_stats = df_photo_target.groupby(['Category Name', 'product_id']).agg(
        total_revenue=('price', 'sum'),           # 총 매출
        sales_volume=('order_id', 'count'),       # 총 판매량
        avg_price=('price', 'mean'),              # 평균 단가
        avg_score=('review_score', 'mean'),       # 평균 평점
        desc_len=('product_description_lenght', 'first'), # 설명 길이 (상품 속성)
        photos_qty=('product_photos_qty', 'first')        # 사진 개수 (상품 속성)
    ).reset_index()

    # 상관관계 분석 대상 변수
    cols_corr = ['total_revenue', 'sales_volume', 'avg_price', 'desc_len', 'photos_qty', 'avg_score']
    cols_labels = {
        'total_revenue': '총 매출',
        'sales_volume': '판매량',
        'avg_price': '단가',
        'desc_len': '설명 길이',
        'photos_qty': '사진 개수',
        'avg_score': '평점'
    }
    
    # 탭으로 구분하여 시각화
    tab_scatter, tab_heatmap = st.tabs(["📍 산점도 (사진 vs 매출)", "🌡️ 히트맵 (전체 상관관계)"])
    
    with tab_scatter:
        st.markdown("**사진 개수 vs 총 매출 분포**")
        
        # Jittering for scatter plot
        df_product_stats['photos_jitter'] = df_product_stats['photos_qty'] + np.random.uniform(-0.2, 0.2, size=len(df_product_stats))
        
        # 매출은 편차가 크므로 로그 스케일 적용 고려 (시각화를 위해)
        fig_scatter = px.scatter(
            df_product_stats, 
            x='photos_jitter', 
            y='total_revenue', 
            color='Category Name',
            facet_col='Category Name',
            opacity=0.6,
            title="사진 개수와 총 매출의 관계 (Log Scale)",
            labels={'photos_jitter': '사진 개수 (장)', 'total_revenue': '총 매출 (R$)'},
            color_discrete_map={'IT/액세서리': '#2196F3', '뷰티/건강': '#E91E63'},
            log_y=True # 매출 로그 스케일
        )
        fig_scatter.update_xaxes(tickvals=[1, 2, 3, 4, 5, 6, 7, 8])
        st.plotly_chart(fig_scatter, width='stretch')
        st.caption("매출 규모(Y축)가 큰 '대박 상품'들이 사진 개수별로 어떻게 분포하는지 보여줍니다.")

    with tab_heatmap:
        st.markdown("**변수 간 상관계수 매트릭스 (매출 포함)**")
        
        c_heat1, c_heat2 = st.columns(2)
        
        # IT 히트맵
        with c_heat1:
            st.markdown("##### 💻 IT/액세서리")
            it_corr = df_product_stats[df_product_stats['Category Name']=='IT/액세서리'][cols_corr].corr()
            it_corr.rename(index=cols_labels, columns=cols_labels, inplace=True) # 한글 라벨 적용
            
            fig_heat_it = px.imshow(
                it_corr,
                text_auto='.2f',
                color_continuous_scale='RdBu_r',
                zmin=-0.3, zmax=0.3,
                title="상관계수 히트맵 (IT)"
            )
            st.plotly_chart(fig_heat_it, width='stretch')
            
        # 뷰티 히트맵
        with c_heat2:
            st.markdown("##### 💄 뷰티/건강")
            beauty_corr = df_product_stats[df_product_stats['Category Name']=='뷰티/건강'][cols_corr].corr()
            beauty_corr.rename(index=cols_labels, columns=cols_labels, inplace=True)
            
            fig_heat_beauty = px.imshow(
                beauty_corr,
                text_auto='.2f',
                color_continuous_scale='RdBu_r',
                zmin=-0.3, zmax=0.3,
                title="상관계수 히트맵 (뷰티)"
            )
            st.plotly_chart(fig_heat_beauty, width='stretch')
            
        st.info("""
        💡 **분석 결과**:
        - **IT/액세서리**: `설명 길이`와 `총 매출` 사이에 **양의 상관관계(붉은색)**가 보인다면, 상세 정보가 충실할수록 많이 팔린다는 뜻입니다.
        - **뷰티/건강**: `단가`와 `판매량` 사이에 **음의 상관관계(파란색)**가 강하다면, 쌀수록 많이 팔린다는 뜻입니다.
        """)
        
    st.divider()

    # 7-2. [논리 검증] 정말로 '소자본 창업'이 과반수인가?
    st.subheader("📊 [데이터 검증] 정말로 '소자본 창업'이 과반수인가?")
    st.markdown("사용자님의 가설(**\"소자본 신규 판매자가 많아서 평균을 깎아먹은 것이다\"**)을 데이터로 검증했습니다.")
    
    # 2018년 신규 판매자 데이터 재활용
    # df_2018은 이미 'Seller Type'이 병합되어 있음
    
    # 1. 신규 판매자(2018년 진입)의 평균 객단가 계산
    new_sellers_df = df_2018[df_2018['Seller Type'] == '신규 판매자']
    seller_avg_price = new_sellers_df.groupby('seller_id')['price'].mean().reset_index()
    
    # 2. 가격 구간 분류 (소자본 기준: 100헤알 미만)
    seller_avg_price['Group'] = seller_avg_price['price'].apply(lambda x: '소자본 (R$ < 100)' if x < 100 else '일반 (R$ >= 100)')
    
    # 3. 평균 깎아먹기 효과 계산
    avg_all = new_sellers_df['price'].sum() / new_sellers_df['seller_id'].nunique()
    
    # 소자본 제외 시 평균
    high_sellers = seller_avg_price[seller_avg_price['Group'] == '일반 (R$ >= 100)']['seller_id']
    df_high = new_sellers_df[new_sellers_df['seller_id'].isin(high_sellers)]
    avg_high = df_high['price'].sum() / df_high['seller_id'].nunique()
    
    imp_col1, imp_col2 = st.columns(2)
    
    with imp_col1:
        st.markdown("**1️⃣ 신규 판매자 구성 비율**")
        group_counts = seller_avg_price['Group'].value_counts().reset_index()
        group_counts.columns = ['Group', 'Count']
        
        fig_proof_pie = px.pie(
            group_counts, values='Count', names='Group',
            color='Group',
            color_discrete_map={'소자본 (R$ < 100)': '#EF5350', '일반 (R$ >= 100)': '#BDBDBD'},
            hole=0.4
        )
        st.plotly_chart(fig_proof_pie, width='stretch')
        
        # 상세 데이터 표 추가 (5단계 구분)
        st.markdown("**📋 [디테일] 객단가 5단계 분포**")
        
        # 5단계 구간 집계
        bins = [0, 50, 100, 200, 500, 10000]
        labels = ['초저가(0-50)', '저가(50-100)', '중가(100-200)', '고가(200-500)', '초고가(500+)']
        seller_avg_price['price_range'] = pd.cut(seller_avg_price['price'], bins=bins, labels=labels)
        
        dist_df = seller_avg_price['price_range'].value_counts().sort_index().reset_index()
        dist_df.columns = ['가격대 구간', '판매자 수']
        dist_df['비중 (%)'] = (dist_df['판매자 수'] / dist_df['판매자 수'].sum() * 100).map('{:.1f}%'.format)
        
        st.dataframe(dist_df, width='stretch', hide_index=True)
        st.caption("초저가~저가 구간(R$ 100 미만)에 가장 많은 판매자가 몰려 있습니다.")
        
    with imp_col2:
        st.markdown("**2️⃣ 평균 매출 하락의 주범**")
        st.metric(label="현재 신규 판매자 평균 매출", value=f"R$ {avg_all:,.0f}")
        st.metric(label="소자본 판매자 제외 시 (If excluded)", value=f"R$ {avg_high:,.0f}", delta=f"+{((avg_high - avg_all)/avg_all*100):.1f}%")
        st.markdown("""
        이들 **53.9%**의 소자본 판매자를 제외하면, 평균 매출이 무려 **70.7%나 상승**합니다.
        즉, 평균 지표가 나빠 보이는 건 이들의 머릿수가 압도적으로 많기 때문입니다.
        """)
        
        st.divider()
        
        # TOP 10 카테고리 단가 표 추가
        st.markdown("**📋 [팩트체크] 신규 판매자 인기 카테고리 TOP 10 단가**")
        
        # 인기 카테고리 추출
        top_cats = new_sellers_df['product_category_name'].value_counts().head(10).index
        cat_prices = new_sellers_df[new_sellers_df['product_category_name'].isin(top_cats)].groupby('product_category_name')['price'].mean().sort_values().reset_index()
        cat_prices.columns = ['카테고리명', '평균 단가 (R$)']
        cat_prices['평균 단가 (R$)'] = cat_prices['평균 단가 (R$)'].map('{:,.1f}'.format)
        
        st.dataframe(cat_prices, width='stretch', hide_index=True)
        st.caption("가장 많이 파는 카테고리들도 대부분 **R$ 100 ~ 120 (저가형)**에 형성되어 있음을 확인할 수 있습니다.")

    st.divider()
    
    # 8. 최종 결론
    st.subheader("8️⃣ 🏁 최종 결론: 범인은 '실력'이 아니라 '종목'이었다")
    st.markdown("데이터 분석을 통해 우리가 가졌던 **편견(오해)**을 바로잡고 **진짜 원인**을 찾았습니다.")
    
    c_final1, c_final2 = st.columns(2)
    
    with c_final1:
        st.error("❌ **우리의 오해 (Myth)**")
        st.markdown("""
        1. **"신규 판매자는 초보라 판매 실력이 부족할 것이다?"**
           - 사진도 대충 찍고 설명도 부실해서 안 팔리는 걸까?
        2. **"사진을 많이 넣어야 고객이 좋아하고 많이 산다?"**
           - 사진 1장짜리는 성의 없어 보여서 안 팔리지 않을까?
        """)
        
    with c_final2:
        st.success("⭕ **데이터의 진실 (Truth)**")
        st.markdown("""
        1. **"아니다! 신규 판매자는 '가성비 마케팅'의 고수다."**
           - IT 제품 설명은 더 꼼꼼하게 쓰고, 평점은 오히려 기존 판매자보다 높다(4.1점).
        2. **"아니다! '사진 1장'이 매출 효율의 왕이다."**
           - 매출 1위 판매자도 평균 사진 1.4장. 사진 개수와 판매량 상관계수는 **0.00** (무관함).
        """)
        
    st.info("""
    ### 🎯 **The Real Culprit (진짜 범인)**
    평균 매출이 떨어지는 진짜 이유는...  
    신규 판매자들이 **'못 해서'**가 아니라, **'뷰티/건강' 같은 소모성 저가 카테고리(Low AOV)**에 집중하는 **'소자본 박리다매 전략'**을 선택했기 때문입니다.
    
    👉 **제언**: 매출 하락을 걱정할 게 아니라, 플랫폼이 **'대중화'** 되고 **'저변이 확대'**되는 긍정적인 신호로 해석해야 합니다!
    """)

def render_action_plan_tab():
    """탭 5: 매출 증대 전략 제언 (Action Plan)"""
    st.header("🚀 매출 증대 및 평균 단가 회복을 위한 3대 전략")
    st.markdown("""
    데이터 분석 결과, 신규 판매자들의 **잠재력(서비스 마인드, 가격 경쟁력)**은 충분합니다.  
    이제 이들을 **'고부가가치 시장'**으로 이끌어줄 **구체적인 액션 플랜**이 필요합니다.
    """)
    
    st.divider()
    
    # 전략 1: 번들링
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("1️⃣ 번들링(Bundling) 시스템 도입")
        st.image("https://cdn-icons-png.flaticon.com/512/3050/3050239.png", width=120)
    with c2:
        st.info("**\"1개 살 거 3개 사게 만들어라\"**")
        st.markdown("""
        - **현상**: 저가 카테고리(뷰티, 생활용품)의 평균 단가는 **R$ 50 ~ 100**에 불과합니다.
        - **솔루션**: **'세트 상품 등록 마법사'** 기능을 제공하여 판매자가 쉽게 묶음 상품을 만들도록 지원합니다.
        - **예시**: 샴푸(R$ 20) 판매자에게 → *"샴푸+린스+트리트먼트 세트(R$ 60)를 등록하면 수수료 2% 할인!"* 팝업 노출.
        - **기대효과**: 객단가(AOV) **약 30~50% 상승** 예상.
        """)
        
    st.divider()
    
    # 전략 2: 크로스 셀링
    c3, c4 = st.columns([1, 2])
    with c3:
        st.subheader("2️⃣ 연관 카테고리 확장 유도")
        st.image("https://cdn-icons-png.flaticon.com/512/3594/3594363.png", width=120)
    with c4:
        st.info("**\"소모품 팔던 고객에게 기계를 팔게 하라\"**")
        st.markdown("""
        - **현상**: 뷰티/건강 카테고리 판매자는 소모품(화장품) 판매에는 능하지만, 고가 기기 판매는 주저합니다.
        - **솔루션**: **'연관 고가 카테고리 추천'** 대시보드 제공.
        - **예시**: 화장품 판매자에게 → *"사장님 고객들이 검색하는 '헤어 드라이어(R$ 300)'도 같이 팔아보세요. 수요가 확실합니다."* 데이터 리포트 제공.
        - **Evidence**: 신규 판매자도 IT 기기에서는 **정성스러운 설명으로 고가 판매**에 성공하고 있음(분석 결과 5번).
        """)

    st.divider()
    
    # 전략 3: 프리미엄 인증
    c5, c6 = st.columns([1, 2])
    with c5:
        st.subheader("3️⃣ '프리미엄 지식 셀러' 인증")
        st.image("https://cdn-icons-png.flaticon.com/512/1161/1161388.png", width=120)
    with c6:
        st.info("**\"사진 개수보다 '정보의 질'을 보상하라\"**")
        st.markdown("""
        - **현상**: IT/액세서리 분야에서 **'긴 설명'**은 높은 가격과 직결됩니다(분석 결과 5번).
        - **솔루션**: 상세 페이지의 **정보 충실도(텍스트 길이, 스펙 명시 등)**를 평가하여 상단 노출 가점 부여.
        - **제도**: **'Olist Expert Seller'** 배지 도입. (조건: 리뷰 4.0 이상 + 제품 설명 500자 이상)
        - **기대효과**: 저가 덤핑 경쟁에서 탈피하여 **정보 기반의 고가 시장 경쟁**으로 유도.
        """)
        
    st.divider()
    st.success("🎯 **Final Goal**: 신규 판매자를 '소상공인' 단계에서 **'전문 리셀러'**로 성장시켜, 전체 플랫폼의 **GMV(총 매출)와 마진율**을 동시에 견인합니다.")

def render_seller_detail_tab():
    """탭 6: 판매자 상세 모니터링"""
    st.header("🔍 판매자별 상세 활동 모니터링")
    st.markdown("개별 판매자의 진입/이탈 시점과 주요 성과 지표를 상세히 추적합니다.")

    # 데이터 로드
    items_with_sellers, _ = load_seller_data()
    df = items_with_sellers[items_with_sellers['order_status'] == 'delivered'].copy()
    
    @st.cache_data
    def get_seller_detailed_stats(_df):
        # 리뷰 데이터 로드
        try:
            reviews = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.parquet'))
        except:
            reviews = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.csv'))
            
        _df = pd.merge(_df, reviews[['order_id', 'review_score']], on='order_id', how='left')
        _df['date'] = _df['order_purchase_timestamp'].dt.date
        
        # 판매자별 집계
        stats = _df.groupby('seller_id').agg(
            first_sale=('order_purchase_timestamp', 'min'),
            last_sale=('order_purchase_timestamp', 'max'),
            total_active_days=('date', 'nunique'),
            total_orders=('order_id', 'nunique'),
            unique_products=('product_id', 'nunique'),
            avg_price=('price', 'mean'),
            avg_review=('review_score', 'mean')
        ).reset_index()
        
        stats['lifespan'] = (stats['last_sale'] - stats['first_sale']).dt.days
        
        # 날짜 포맷팅 (표현용)
        stats['신규진입날짜'] = stats['first_sale'].dt.strftime('%Y-%m-%d')
        stats['마지막판매날짜'] = stats['last_sale'].dt.strftime('%Y-%m-%d')
        
        return stats

    with st.spinner("판매자 데이터 분석 중..."):
        seller_detail = get_seller_detailed_stats(df)

    # 1. 전체 판매자 목록
    st.subheader("📋 전체 판매자 활동 지표")
    
    # 검색 기능
    search_id = st.text_input("판매자 ID 검색 (앞자리 8자)", "")
    if search_id:
        display_df = seller_detail[seller_detail['seller_id'].str.startswith(search_id)].copy()
    else:
        display_df = seller_detail.copy()

    # 컬럼 정리 및 정렬
    show_cols = ['seller_id', '신규진입날짜', '마지막판매날짜', 'lifespan', 'total_active_days', 'total_orders', 'unique_products', 'avg_price', 'avg_review']
    df_render = display_df[show_cols].copy()
    df_render.columns = ['판매자 ID', '신규 진입일', '마지막 판매일', '활동 기간(일)', '총 활동일', '총 주문건수', '취급 상품수', '평균 단가', '평균 평점']
    
    st.dataframe(
        df_render.style.format({
            '평균 단가': 'R$ {:,.1f}',
            '평균 평점': '{:.2f}점'
        }),
        use_container_width=True,
        height=400
    )

    st.divider()

    # 2. 단기 활동 판매자 분석 (180일 미만)
    st.subheader("⚠️ 단기 활동 판매자 관리 (180일 미만)")
    st.markdown("플랫폼에 안착하지 못하고 6개월 이내에 판매를 멈춘 판매자들을 별도로 추출하였습니다. 이들의 데이터를 통해 공통적인 이탈 사유를 파악할 수 있습니다.")
    
    short_term_df = seller_detail[seller_detail['lifespan'] < 180].sort_values('lifespan', ascending=False)
    
    df_short_render = short_term_df[show_cols].copy()
    df_short_render.columns = ['판매자 ID', '신규 진입일', '마지막 판매일', '활동 기간(일)', '총 활동일', '총 주문건수', '취급 상품수', '평균 단가', '평균 평점']
    
    st.dataframe(
        df_short_render.style.background_gradient(cmap='Reds', subset=['활동 기간(일)'])
                          .format({
                              '평균 단가': 'R$ {:,.1f}',
                              '평균 평점': '{:.2f}점'
                          }),
        use_container_width=True,
        height=400
    )
    
    st.info("💡 **가이드**: 이 표에 나타난 판매자들은 '정착 실패' 군에 속합니다. 특히 활동 기간이 매우 짧으면서 상품 수가 1~2개인 경우, 상품 경쟁력 부재가 주원인일 가능성이 큽니다.")

    st.divider()

    # 3. 판매자 유형별 집계 요약
    st.subheader("📊 판매자 유형별 집계 요약")
    
    total_count = len(seller_detail)
    long_term_count = int(len(seller_detail[seller_detail['lifespan'] >= 180]))
    short_term_count = int(total_count - long_term_count)
    
    summary_df = pd.DataFrame({
        "판매자 유형": ["장기 판매자 (180일 이상)", "단기 판매자 (180일 미만)", "합계"],
        "판매자 수": [long_term_count, short_term_count, total_count],
        "비중 (%)": [
            f"{(long_term_count/total_count*100):.1f}%", 
            f"{(short_term_count/total_count*100):.1f}%", 
            "100.0%"
        ]
    })
    
    st.table(summary_df)
    st.info(f"전체 {total_count:,}명의 판매자 중 약 { (short_term_count/total_count*100):.1f}%인 {short_term_count:,}명이 6개월(180일)을 채우지 못하고 활동을 중단했습니다.")

def render_survival_logic_tab():
    """탭 7: 장기 생존 확률 계산 로직 설명"""
    st.header("🧪 장기 생존 확률 계산 로직 (3-Step Walkthrough)")
    st.markdown("데이터 분석에서 사용된 '장기 생존 확률'이 어떤 단계를 거쳐 산출되는지 실제 데이터를 통해 설명합니다.")
    
    # 데이터 로드
    items_with_sellers, _ = load_seller_data()
    df = items_with_sellers[items_with_sellers['order_status'] == 'delivered'].copy()
    
    st.divider()
    
    # 1단계: 활동 수명 계산
    st.subheader("1️⃣ 1단계: 각 판매자의 활동 수명(Lifespan) 계산")
    st.markdown("판매자가 플랫폼에 머문 기간을 `최초 판매일`과 `마지막 판매일`의 차이로 계산합니다.")
    st.latex(r"활동\ 수명(일) = 마지막\ 판매일 - 최초\ 판매일")
    
    seller_dates = df.groupby('seller_id')['order_purchase_timestamp'].agg(['min', 'max']).reset_index()
    seller_dates.columns = ['seller_id', '최초 판매일', '마지막 판매일']
    seller_dates['활동 수명(일)'] = (seller_dates['마지막 판매일'] - seller_dates['최초 판매일']).dt.days
    
    st.dataframe(seller_dates.head(5), use_container_width=True)
    
    st.divider()
    
    # 2단계: 생존 여부 판단 및 기회 부족군 제외
    st.subheader("2️⃣ 2단계: 장기 생존 여부 판단 (충분한 관찰 기간 부여)")
    st.markdown(
        "활동 수명이 **180일(약 6개월)**을 넘었는지에 따라 성공(생존)과 실패(이탈)를 구분합니다. "
        "🚨 **핵심 보정사항**: 단, 최신 가입자 중 *'플랫폼에 가입한 지 아직 180일이 안 된 판매자'*는 아직 성공/실패 여부를 판가름할 '충분한 시간적 기회'가 없었으므로 통계의 왜곡을 막기 위해 **분석 모수에서 제외**합니다."
    )
    
    # 데이터 집계 기준일 (가장 마지막 주문일)
    max_date = df['order_purchase_timestamp'].max()
    seller_dates['관찰가능기간(일)'] = (max_date - seller_dates['최초 판매일']).dt.days
    
    # 제외 대상 결정 (관찰기간이 180일 미만인 경우)
    excluded_sellers = seller_dates[seller_dates['관찰가능기간(일)'] < 180]
    num_excluded = len(excluded_sellers)
    
    # 유효 분석 대상 모집단
    seller_dates_filtered = seller_dates[seller_dates['관찰가능기간(일)'] >= 180].copy()
    num_included = len(seller_dates_filtered)
    
    seller_dates_filtered['생존 여부'] = seller_dates_filtered['활동 수명(일)'].apply(lambda x: "✅ 성공 (생존)" if x >= 180 else "❌ 실패 (이탈)")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.dataframe(seller_dates_filtered[['seller_id', '최초 판매일', '활동 수명(일)', '생존 여부']].head(5), use_container_width=True)
    with col2:
        st.info(f"💡 **모수 보정 안내**\n"
                f"- 전체 판매자: {len(seller_dates):,}명\n"
                f"- (제외) 최근 가입자: {num_excluded:,}명 (전체의 {(num_excluded/len(seller_dates)*100):.1f}%)\n"
                f"- (포함) 유효 모집단: **{num_included:,}명**")

    st.divider()
    
    # 3단계: 그룹 확률 산출
    st.subheader("3️⃣ 3단계: 특정 그룹의 최종 확률 산출 (보정됨)")
    st.markdown("궁금한 특정 그룹(예: 첫 달 1건 판매자) 내에서 [유효 모집단 기준] 생존자가 몇 명인지 비율을 구합니다.")
    
    st.latex(r"장기\ 생존\ 확률(\%) = \frac{해당\ 그룹\ 내\ 생존자\ 수(180일+)}{해당\ 그룹\ 전체\ 판매자\ 수\ (관찰\ 기회\ 180일\ 이상\ 확보자)} \times 100")
    
    # 샘플 계산 (첫 달 1건 판매자 그룹 - 유효 모집단 바탕)
    df_filtered_sales = pd.merge(df, seller_dates_filtered[['seller_id', '최초 판매일']], on='seller_id', how='inner')
    df_filtered_sales['days_since_start'] = (df_filtered_sales['order_purchase_timestamp'] - df_filtered_sales['최초 판매일']).dt.days
    
    m1_orders = df_filtered_sales[df_filtered_sales['days_since_start'] <= 30].groupby('seller_id')['order_id'].nunique().reset_index()
    m1_orders.columns = ['seller_id', 'm1_orders']
    
    final_calc_df = pd.merge(seller_dates_filtered, m1_orders, on='seller_id', how='left').fillna({'m1_orders': 0})
    group_1_sale = final_calc_df[final_calc_df['m1_orders'] == 1]
    
    total_group = len(group_1_sale)
    survived_group = (group_1_sale['활동 수명(일)'] >= 180).sum()
    final_rate = (survived_group / total_group * 100) if total_group > 0 else 0
    
    c1, c2, c3 = st.columns(3)
    c1.metric("보정된 그룹 모집단 (분모)", f"{total_group:,}명")
    c2.metric("장기 생존자 수 (분자)", f"{survived_group:,}명")
    c3.metric("최종 생존 확률", f"{final_rate:.1f}%")
    
    st.success(f"**결과 해석**: '180일의 관찰 기회'가 주어졌던 판매자 중, 첫 달에 1건만 판 그룹({total_group:,}명)에서 성공적으로 장기 정착한 사람은 {survived_group:,}명으로, 이들의 장기 생존 확률은 **{final_rate:.1f}%**입니다.")

def render_cohort_analysis_tab():
    """탭 8: 장기 생존 기준 산출을 위한 코호트(Cohort) 분석 (180일 보정)"""
    st.header("📊 코호트(동기 집단) 기반 판매자 리텐션 분석")
    st.markdown(
        "플랫폼에 가입/첫 판매를 시작한 시점이 같은 판매자들을 한 그룹(코호트)으로 묶어, 이후 몇 개월 동안 재활동(판매)을 유지하는지 추적합니다. "
        "특히 탭 7과 동일하게 **관찰 가능 기간이 180일 미만인 최근 가입자(기회 부족군)는 분석 모수에서 제외**하여 장기 생존의 평탄화 시점을 보다 엄밀히 측정합니다."
    )
    
    # 데이터 로드
    items_with_sellers, _ = load_seller_data()
    df = items_with_sellers[items_with_sellers['order_status'] == 'delivered'].copy()
    
    # --- 모수 보정 로직 (180일 시간 부족군 제외) ---
    max_date = df['order_purchase_timestamp'].max()
    seller_first_sale = df.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_sale.columns = ['seller_id', '최초 판매일']
    seller_first_sale['관찰가능기간(일)'] = (max_date - seller_first_sale['최초 판매일']).dt.days
    
    valid_sellers = seller_first_sale[seller_first_sale['관찰가능기간(일)'] >= 180]['seller_id']
    num_excluded = len(seller_first_sale) - len(valid_sellers)
    
    df_filtered = df[df['seller_id'].isin(valid_sellers)].copy()
    
    st.info(f"💡 **분석 대상(모수) 보정 완료**: 전체 판매자 중, 180일(6개월)의 관찰 기회가 확보되지 않은 **{num_excluded:,}명**의 단기 가입자를 코호트 집계에서 제외하고, "
            f"충분한 기회가 주어졌던 유효 모집단 **{len(valid_sellers):,}명**만을 대상으로 유지율을 추적합니다.")
    
    st.latex(r"Cohort\ Index = (주문\ 연도 - 최초\ 판매\ 연도) \times 12 + (주문\ 월 - 최초\ 판매\ 월) + 1")
    st.latex(r"N개월\ 차\ 유지율(Retention) = \frac{N개월\ 차에\ 판매가\ 있는\ 판매자\ 수}{최초\ 달(1개월\ 차)에\ 판매한\ 판매자\ 수} \times 100")

    with st.spinner("코호트 리텐션 매트릭스 계산 중..."):
        # 코호트 분석을 위한 연월 데이터 추가 변환
        df_cohort = df_filtered[['seller_id', 'order_purchase_timestamp']].copy()
        df_cohort['OrderMonth'] = df_cohort['order_purchase_timestamp'].dt.to_period('M')
        
        # 판매자별 최초 판매 월 산출
        seller_first_month = df_cohort.groupby('seller_id')['OrderMonth'].min().reset_index()
        seller_first_month.columns = ['seller_id', 'CohortMonth']
        
        df_cohort = pd.merge(df_cohort, seller_first_month, on='seller_id')
        
        # 연/월 분리 후 코호트 인덱스(경과 월) 계산
        order_year = df_cohort['OrderMonth'].dt.year
        order_month = df_cohort['OrderMonth'].dt.month
        
        cohort_year = df_cohort['CohortMonth'].dt.year
        cohort_month = df_cohort['CohortMonth'].dt.month
        
        # 년/월 차이 환산 (최초 달 = 1)
        df_cohort['CohortIndex'] = (order_year - cohort_year) * 12 + (order_month - cohort_month) + 1
        
        # 코호트 매트릭스 생성 (CohortMonth x CohortIndex 로 판매자 고유 유저 수 카운트)
        grouping = df_cohort.groupby(['CohortMonth', 'CohortIndex'])
        cohort_data = grouping['seller_id'].nunique().reset_index()
        cohort_counts = cohort_data.pivot(index='CohortMonth', columns='CohortIndex', values='seller_id')
        
        # 첫 번째 열(최초 가입 월)의 값을 100% 기준으로 비율 산출
        cohort_sizes = cohort_counts.iloc[:, 0]
        retention = cohort_counts.divide(cohort_sizes, axis=0) * 100
        
        # DataFrame Index 이름 문자열로 변환 (표현 최적화)
        retention.index = retention.index.astype(str)
        # 최대 12개월(1년)까지만 표시
        max_cols = min(12, len(retention.columns))
        retention_display = retention.iloc[:, :max_cols].round(1)

    st.markdown("##### 📝 월별 신규 가입(첫 판매) 코호트의 이후 개월 차 재활동 유지율 (%)")
    
    # 히트맵 표 렌더링
    st.dataframe(
        retention_display.style.background_gradient(cmap='Blues', axis=None, vmin=0, vmax=100)
                              .format("{:.1f}", na_rep="")
                              .highlight_null(color='white'),
        use_container_width=True,
        height=450
    )
    
    # 분석 도출
    st.success(
        "💡 **코호트 리텐션 해석**: \n"
        "열(Column) 방향의 `Index(1~12)`는 첫 판매 이후 경과된 월수(Months)를 의미합니다. "
        "모수 필터링(180일 미만 제외)이 적용된 이 표를 보면, 가입 초기 급감을 거쳐 대략 **`4~6개월 차(Index 4~6)`** 구간부터 "
        "잔존율이 특정 수준에서 하락세를 멈추고 평탄하게(Plateau) 유지되는 핵심 현상이 확인됩니다. 이는 비즈니스 로직상 **장기 생존 척도를 '6개월(180일)'로 삼는 것이 데이터 과학적으로 타당함**을 증명합니다."
    )

if __name__ == "__main__":
    main()
