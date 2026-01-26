# ... (기존 import 및 함수 정의는 동일하게 유지하거나 main 위로 이동)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests
import json

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

    return region_stats

def main():
    # 페이지 설정
    st.set_page_config(page_title="브라질 지역별 배송 지연 분석", layout="wide")

    # 데이터 로딩
    data = load_and_process_data()
    brazil_geojson = get_brazil_geojson()

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
        use_container_width=True
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
        st.plotly_chart(fig_bar_delay, use_container_width=True)

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
        st.plotly_chart(fig_bar_vol, use_container_width=True)

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
    st.plotly_chart(fig_scatter, use_container_width=True)

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
    st.plotly_chart(fig_intensity, use_container_width=True)
    st.info("💡 **가설 검증**: 7일 이상 지연 비중이 높은 지역은 물류 프로세스의 전면적인 재검토가 필요합니다.")

if __name__ == "__main__":
    main()
