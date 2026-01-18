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
    except:
        orders = pd.read_csv(os.path.join(BASE_PATH, 'olist_orders_dataset.csv'))
        customers = pd.read_csv(os.path.join(BASE_PATH, 'olist_customers_dataset.csv'))

    # 날짜 컬럼 변환
    date_columns = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in date_columns:
        orders[col] = pd.to_datetime(orders[col])

    # 데이터 병합
    df = pd.merge(orders, customers[['customer_id', 'customer_state']], on='customer_id', how='left')

    # 스냅샷 기준일
    snapshot_date = df['order_purchase_timestamp'].max()

    # 지연 및 시간 계산
    df['is_success'] = df['order_status'] == 'delivered'
    df['is_success_delay'] = (df['is_success']) & (df['order_delivered_customer_date'] > df['order_estimated_delivery_date'])
    df['is_failure_delay'] = (~df['is_success']) & (df['order_estimated_delivery_date'] < snapshot_date)
    
    df['delivery_time_success'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
    df['delivery_time_failure'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.days

    # 지역별 집계
    region_stats = df.groupby('customer_state').agg(
        total_orders=('order_id', 'count'),
        success_cnt=('is_success', 'sum'),
        failure_cnt=('is_success', lambda x: (~x).sum()),
        success_delay_cnt=('is_success_delay', 'sum'),
        failure_delay_cnt=('is_failure_delay', 'sum'),
        avg_time_success=('delivery_time_success', 'mean'),
        avg_time_failure=('delivery_time_failure', 'mean')
    ).reset_index()

    # 비율 계산
    region_stats['Success Delay Ratio (%)'] = (region_stats['success_delay_cnt'] / region_stats['success_cnt'] * 100).fillna(0).round(2)
    region_stats['Failure Delay Ratio (%)'] = (region_stats['failure_delay_cnt'] / region_stats['failure_cnt'] * 100).fillna(0).round(2)
    region_stats['Total Delay Ratio (%)'] = ((region_stats['success_delay_cnt'] + region_stats['failure_delay_cnt']) / region_stats['total_orders'] * 100).round(2)

    return region_stats

def main():
    # 페이지 설정
    st.set_page_config(page_title="브라질 지역별 배송 지연 분석", layout="wide")

    # 데이터 로딩
    data = load_and_process_data()
    brazil_geojson = get_brazil_geojson()

    # 사이드바
    st.sidebar.title("🚚 배송 지연 분석 필터")
    selected_states = st.sidebar.multiselect("분석할 지역(State) 선택", options=data['customer_state'].unique(), default=['SP', 'RJ', 'MG'])

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

    # 하단 상세 데이터 및 평균 시간
    st.subheader("📍 지역별 상세 지표 및 배송 시간")
    display_df = filtered_data.rename(columns={
        'customer_state': '지역',
        'total_orders': '총 주문',
        'Success Delay Ratio (%)': '성공 지연율(%)',
        'Failure Delay Ratio (%)': '실패 지연율(%)',
        'Total Delay Ratio (%)': '통합 지연율(%)',
        'avg_time_success': '평균 성공 시간(일)',
        'avg_time_failure': '평균 실패 시간(일)'
    })

    st.dataframe(display_df[['지역', '총 주문', '성공 지연율(%)', '실패 지연율(%)', '통합 지연율(%)', '평균 성공 시간(일)', '평균 실패 시간(일)']].sort_values('통합 지연율(%)', ascending=False), width="stretch")

    # 배송 시간 산점도
    st.subheader("📈 배송 시간과 지연율의 상관관계")
    fig_scatter = px.scatter(
        data,
        x='avg_time_success',
        y='Total Delay Ratio (%)',
        size='total_orders',
        color='customer_state',
        hover_name='customer_state',
        labels={'avg_time_success': '평균 배송 성공 시간 (일)', 'Total Delay Ratio (%)': '통합 지연율 (%)'},
        title="배송 소요 시간 대비 지연율 분포"
    )
    st.plotly_chart(fig_scatter, width="stretch")

if __name__ == "__main__":
    main()
