import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# 페이지 설정
st.set_page_config(page_title="Olist 배송 지연 분석 대시보드", layout="wide")

# 경로 설정 (상대 경로 적용: 로컬 및 배포 서버 공용)
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

@st.cache_data
def load_raw_data():
    # Parquet 포맷으로 변환된 원본 데이터 로드 (용량 감소 및 속도 향상)
    orders = pd.read_parquet(os.path.join(BASE_PATH, 'olist_orders_dataset.parquet'))
    order_items = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_items_dataset.parquet'))
    payments = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_payments_dataset.parquet'))
    customers = pd.read_parquet(os.path.join(BASE_PATH, 'olist_customers_dataset.parquet'))
    products = pd.read_parquet(os.path.join(BASE_PATH, 'olist_products_dataset.parquet'))
    
    # 날짜 자동 변환 (Parquet에서 이미 처리된 경우를 위해 확인 후 처리)
    date_cols = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in date_cols:
        if not pd.api.types.is_datetime64_any_dtype(orders[col]):
            orders[col] = pd.to_datetime(orders[col])
        
    return orders, order_items, payments, customers, products

orders_raw, items_raw, payments_raw, customers_raw, products_raw = load_raw_data()

# 한글 매핑 사전
KOR_COLUMNS = {
    "customer_state": "지역(State)",
    "seller_id": "판매자 ID",
    "product_category_name": "상품 카테고리",
    "success_delayed_cnt": "성공건 중 지연 건수",
    "cancel_delayed_cnt": "취소 건수",
    "total_payment_value": "총 거래 금액",
    "buyer_cnt": "구매자 수",
    "avg_payment": "평균 거래 금액",
    "delay_rate": "지연율",
    "value_per_buyer": "고객당 거래액"
}

# 사이드바: 필터링 섹션
st.sidebar.title("🔍 검색 및 필터")

# 1. 날짜 필터
min_date = orders_raw['order_purchase_timestamp'].min().date()
max_date = orders_raw['order_purchase_timestamp'].max().date()

st.sidebar.subheader("📅 기간 선택")
date_range = st.sidebar.date_input(
    "주문 기간을 선택하세요",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# 2. 검색 기준 필터
search_type = st.sidebar.radio("검색 기준 선택", ["지역(State)", "판매자 (Seller ID)", "상품 카테고리"])

# 데이터 필터링 및 집계 로직
def get_filtered_metrics(start_date, end_date, group_type):
    # 기간 필터링
    mask = (orders_raw['order_purchase_timestamp'].dt.date >= start_date) & \
           (orders_raw['order_purchase_timestamp'].dt.date <= end_date)
    filtered_orders = orders_raw.loc[mask].copy()
    
    # 지연 여부 판단
    filtered_orders['is_delayed'] = (filtered_orders['order_delivered_customer_date'] > filtered_orders['order_estimated_delivery_date']).astype(int)
    
    # 데이터 병합
    df = pd.merge(items_raw, filtered_orders, on='order_id', how='inner')
    order_pay = payments_raw.groupby('order_id')['payment_value'].sum().reset_index()
    df = pd.merge(df, order_pay, on='order_id', how='left')
    df = pd.merge(df, customers_raw[['customer_id', 'customer_unique_id', 'customer_state']], on='customer_id', how='left')
    df = pd.merge(df, products_raw[['product_id', 'product_category_name']], on='product_id', how='left')

    # 그룹화 기준 설정
    if group_type == "지역(State)":
        group_col = 'customer_state'
    elif group_type == "판매자 (Seller ID)":
        group_col = 'seller_id'
    else:
        group_col = 'product_category_name'

    # 동적 집계
    metrics = df.groupby(group_col).agg(
        success_delayed_cnt=('order_id', lambda x: ((df.loc[x.index, 'order_status'] == 'delivered') & (df.loc[x.index, 'is_delayed'] == 1)).sum()),
        cancel_delayed_cnt=('order_id', lambda x: (df.loc[x.index, 'order_status'] == 'canceled').sum()),
        total_payment_value=('payment_value', 'sum'),
        buyer_cnt=('customer_unique_id', 'nunique')
    ).reset_index()
    
    return metrics, group_col

# 필터 적용된 데이터 산출
if len(date_range) == 2:
    start_date, end_date = date_range
    raw_metrics, group_col = get_filtered_metrics(start_date, end_date, search_type)
    
    # 키워드 선택 필터 (집계된 데이터 내에서)
    keywords = st.sidebar.multiselect(f"비교할 {search_type}를 선택하세요", raw_metrics[group_col].dropna().unique())
    filtered_df = raw_metrics[raw_metrics[group_col].isin(keywords)] if keywords else raw_metrics
else:
    st.error("시작일과 종료일을 모두 선택해 주세요.")
    st.stop()

# 표 표시용 데이터프레임 한글화
display_df = filtered_df.rename(columns=KOR_COLUMNS)
kor_group_col = KOR_COLUMNS.get(group_col, group_col)

# 메인 타이틀
st.title("📊 Olist 이커머스 배송 지연 분석 대시보드")
st.markdown("브라질 Olist 데이터를 기반으로 한 배송 지연 현황 및 기초 EDA 결과입니다.")

# 탭 구성
tabs = st.tabs(["⭐ 핵심 지표", "🚚 지연 분석", "🏷️ 용어 사전 (한글)"])

# --- 탭 1: 핵심 지표 ---
with tabs[0]:
    st.header("대시보드 요약")
    
    # KPI 카드
    col1, col2, col3, col4 = st.columns(4)
    total_tx = filtered_df['success_delayed_cnt'].sum() + filtered_df['cancel_delayed_cnt'].sum()
    col1.metric("총 거래 건수", f"{total_tx:,}")
    col2.metric("총 거래 금액", f"R$ {filtered_df['total_payment_value'].sum():,.0f}")
    col3.metric("지연 건수 (성공건)", f"{filtered_df['success_delayed_cnt'].sum():,}")
    col4.metric("총 구매자 수", f"{filtered_df['buyer_cnt'].sum():,}")

    st.divider()
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("그래프 1: 그룹별 거래 금액 비중")
        fig1 = px.pie(display_df, values='총 거래 금액', names=kor_group_col, hole=0.4,
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig1, width='stretch')
        
        st.subheader("표 1: 상위 거래 지표 요약")
        st.table(display_df.sort_values(by='총 거래 금액', ascending=False).head(5))

    with c2:
        st.subheader("그래프 2: 성공건 지연 vs 취소건 비교")
        fig2 = go.Figure(data=[
            go.Bar(name='성공건 지연', x=display_df[kor_group_col], y=display_df['성공건 중 지연 건수']),
            go.Bar(name='취소건', x=display_df[kor_group_col], y=display_df['취소 건수'])
        ])
        fig2.update_layout(barmode='group', xaxis_title=kor_group_col, yaxis_title="건수")
        st.plotly_chart(fig2, width='stretch')

        st.subheader("표 2: 지연 건수 상세 정보")
        st.dataframe(display_df[[kor_group_col, '성공건 중 지연 건수', '취소 건수']].head(10), width='stretch')

# --- 탭 2: 지연 분석 ---
with tabs[1]:
    st.header("상세 EDA 분석")
    
    # 데이터 계산
    filtered_df['avg_payment'] = filtered_df['total_payment_value'] / (filtered_df['success_delayed_cnt'] + 1)
    filtered_df['delay_rate'] = filtered_df['success_delayed_cnt'] / (filtered_df['success_delayed_cnt'] + filtered_df['cancel_delayed_cnt'] + 1)
    top_efficiency = filtered_df.copy()
    top_efficiency['value_per_buyer'] = top_efficiency['total_payment_value'] / top_efficiency['buyer_cnt']
    
    # 한글화된 분석용 데이터프레임
    analysis_df = filtered_df.rename(columns=KOR_COLUMNS)

    # 3번째 그래프: 거래 금액 분포
    st.subheader("그래프 3: 그룹별 평균 거래 금액")
    fig3 = px.bar(analysis_df, x=kor_group_col, y='평균 거래 금액', color='평균 거래 금액', 
                  labels={'평균 거래 금액': '평균 금액'})
    st.plotly_chart(fig3, width='stretch')

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("그래프 4: 구매자 수 대비 지연 건수 (산점도)")
        fig4 = px.scatter(analysis_df, x='구매자 수', y='성공건 중 지연 건수', size='총 거래 금액', 
                          hover_name=kor_group_col, color=kor_group_col)
        st.plotly_chart(fig4, width='stretch')
        
        st.subheader("표 3: 고효율 판매 지역/카테고리")
        eff_display = top_efficiency.rename(columns=KOR_COLUMNS)
        st.dataframe(eff_display.sort_values(by='고객당 거래액', ascending=False).head(10), width='stretch')

    with c4:
        st.subheader("그래프 5: 지연 비율 현황")
        fig5 = px.line(analysis_df.sort_values(by='지연율'), x=kor_group_col, y='지연율', markers=True)
        st.plotly_chart(fig5, width='stretch')

        st.subheader("표 4: 지연율 상위 리스트")
        st.write(analysis_df.sort_values(by='지연율', ascending=False)[[kor_group_col, '지연율']].head(10))

    st.subheader("표 5: 전체 데이터 요약 통계")
    st.write(analysis_df.describe())
    st.caption("※ 참고: 'count'는 분석 대상(예: 선택된 지역 수)의 총 개수입니다. 모든 지표가 동일한 분석 그룹을 대상으로 계산되었으므로 값이 똑같이 나타납니다.")

    st.divider()
    st.header("💡 지역별 유의미한 정보 (Insights)")
    
    insight_text = """
    본 데이터 분석을 통해 도출된 지역별 주요 인사이트는 다음과 같습니다:
    
    1. **SP(상파울루)**: 압도적인 거래량과 매출을 기록하고 있으나, 배송 지연 건수 절대치 또한 가장 높습니다. 물류 인프라가 집중되어 있음에도 불구하고 주문 밀집으로 인한 지연 관리가 핵심 과제입니다.
    2. **RJ(리오데자네이로)**: 거래 건수 대비 지연 비율(Delay Rate)이 타 지역에 비해 상대적으로 높은 경향을 보입니다. 지리적 특성이나 특정 지역 물류 센터의 효율성 개선이 필요할 수 있습니다.
    3. **외곽 지역 (AC, AM 등)**: 거래 금액은 적지만 배송 예정일 자체가 길게 설정되어 있으며, 예정일을 초과하는 지연이 발생할 경우 고객 불만이 매우 클 수 있으므로 주의 깊은 모니터링이 필요합니다.
    4. **판매자 분포**: 특정 지역에 편중된 판매자 배치는 물류 비용과 지연에 직접적인 영향을 줍니다. 판매자 기반을 전국적으로 확장하는 것이 지연 감소의 한 방법이 될 수 있습니다.
    5. **효율성**: 구매자 수 대비 거래 금액이 높은 지역은 VIP 고객층이 두터운 곳으로, 이 지역의 배송 품질(지연 감소)은 고객 유지율(Retention)에 큰 영향을 미칩니다.
    """
    st.info(insight_text)

# --- 탭 3: 용어 사전 ---
with tabs[2]:
    st.header("📖 데이터 용어 사전 및 한글 가이드")
    
    # 컬럼 정보 정리
    col_mapping = {
        "order_id": "주문 ID",
        "customer_id": "고객 ID",
        "order_status": "주문 상태 (delivered: 배송완료, canceled: 취소 등)",
        "order_purchase_timestamp": "주문 시점",
        "order_delivered_customer_date": "실제 배송 완료일",
        "order_estimated_delivery_date": "배송 예정일",
        "customer_state": "고객 거주 주(State)",
        "product_category_name": "상품 카테고리명",
        "payment_value": "결제 금액",
        "success_delayed_cnt": "성공건 중 지연 건수",
        "cancel_delayed_cnt": "취소 건수",
        "total_payment_value": "총 결제 금액",
        "buyer_cnt": "구매자 수"
    }
    
    col_data = []
    for eng, kor in col_mapping.items():
        col_data.append({"영문 컬럼명": eng, "한글 설명": kor})
    
    st.subheader("1. 주요 컬럼 설명")
    st.table(pd.DataFrame(col_data))
    
    st.divider()
    
    st.subheader("2. 주요 상태값 및 지역 정보 (브라질 주 코드 매핑)")
    
    # 브라질 주(State) 코드 및 한글 명칭 데이터
    brazil_states = {
        "코드": ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
        "한글 명칭": ["아크레", "알라고아스", "아마조나스", "아마파", "바이아", "세아라", "연방구 (브라질리아)", "에스피리투산투", "고이아스", "마라냥", "미나스제라이스", "마투그로수두술", "마투그로수", "파라", "파라이바", "페르남부쿠", "피아우이", "파라나", "리오데자네이로", "리오그란데두노르치", "혼도니아", "호라이마", "리오그란데두술", "산타카타리나", "세르지피", "상파울루", "토칸칭스"],
        "원문 명칭 (State)": ["Acre", "Alagoas", "Amazonas", "Amapá", "Bahia", "Ceará", "Distrito Federal", "Espírito Santo", "Goiás", "Maranhão", "Minas Gerais", "Mato Grosso do Sul", "Mato Grosso", "Pará", "Paraíba", "Pernambuco", "Piauí", "Paraná", "Rio de Janeiro", "Rio Grande do Norte", "Rondônia", "Roraima", "Rio Grande do Sul", "Santa Catarina", "Sergipe", "São Paulo", "Tocantins"]
    }
    st.table(pd.DataFrame(brazil_states))

    st.divider()

    st.subheader("3. 기타 고유 데이터 요약 (중복 제거)")
    c5, c6 = st.columns(2)
    with c5:
        st.write("**주문 상태 (order_status) 고유값**")
        st.write(orders_raw['order_status'].unique())
        
        st.write("**분석 데이터 내 포함된 지역 고유값**")
        st.write(customers_raw['customer_state'].unique())

    with c6:
        st.write("**상품 카테고리 (일부) 고유값**")
        st.write(products_raw['product_category_name'].dropna().unique()[:20]) # 너무 많아서 일부만 표시
        st.caption("등 총 70여 개의 카테고리가 존재합니다.")

st.sidebar.markdown("---")
st.sidebar.info("Developed by Antigravity AI")
