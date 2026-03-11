import pandas as pd
import os

# 데이터 및 출력 경로 설정
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

print("데이터 로딩 중...")
# 데이터 로드
orders = pd.read_csv(os.path.join(DATA_PATH, 'olist_orders_dataset.csv'))
order_items = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_items_dataset.csv'))
products = pd.read_csv(os.path.join(DATA_PATH, 'olist_products_dataset.csv'))
customers = pd.read_csv(os.path.join(DATA_PATH, 'olist_customers_dataset.csv'))
payments = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_payments_dataset.csv'))
sellers = pd.read_csv(os.path.join(DATA_PATH, 'olist_sellers_dataset.csv'))

# 날짜 컬럼 변환
date_columns = [
    'order_purchase_timestamp', 'order_approved_at', 
    'order_delivered_carrier_date', 'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]
for col in date_columns:
    orders[col] = pd.to_datetime(orders[col])

print("데이터 전처리 및 병합 중...")
# 배송 지연 여부 판단 (성공건 기준)
# 지연 = 실제 배송일 > 예정 배송일
orders['is_delayed'] = (orders['order_delivered_customer_date'] > orders['order_estimated_delivery_date']).astype(int)

# 데이터 병합
# 주문 + 품목
df = pd.merge(order_items, orders, on='order_id', how='left')
# + 결제 (주문당 총 결제액 합산해서 결합)
order_payments = payments.groupby('order_id')['payment_value'].sum().reset_index()
df = pd.merge(df, order_payments, on='order_id', how='left')
# + 고객 정보 (지역 확인용)
df = pd.merge(df, customers[['customer_id', 'customer_unique_id', 'customer_state', 'customer_city']], on='customer_id', how='left')
# + 상품 정보 (카테고리 확인용)
df = pd.merge(df, products[['product_id', 'product_category_name']], on='product_id', how='left')

# 배송 지연 지표 보완 (취소건 내 지연)
# 주문 상태가 'canceled'이면서, 현재 시간이 또는 분석 시점이 예정일을 넘긴 경우를 배송지연 취소로 간주할 수 있으나,
# 여기서는 단순하게 order_status가 canceled인 경우와 delivered인 경우를 나누어 분석
df['is_canceled'] = (df['order_status'] == 'canceled').astype(int)

# 지표 계산 함수
def aggregate_metrics(group_cols):
    grouped = df.groupby(group_cols).agg(
        # 거래성공건 중 배송지연 건수 (delivered 상태이면서 지연된 경우)
        success_delayed_cnt=('order_id', lambda x: ((df.loc[x.index, 'order_status'] == 'delivered') & (df.loc[x.index, 'is_delayed'] == 1)).sum()),
        # 거래취소건 중 배송지연 (canceled 상태인 모든 건을 일단 포함, 상세 로직은 데이터에 따라 조정 가능)
        cancel_delayed_cnt=('order_id', lambda x: ((df.loc[x.index, 'order_status'] == 'canceled')).sum()),
        # 총 거래금액
        total_payment_value=('payment_value', 'sum'),
        # 구매자 수 (고유 ID 기준)
        buyer_cnt=('customer_unique_id', 'nunique')
    ).reset_index()
    return grouped

print("지역별 지표 산출 중...")
# 1. 지역별 (customer_state)
state_metrics = aggregate_metrics(['customer_state'])
state_metrics.to_csv(os.path.join(REPORTS_PATH, 'metrics_by_state.csv'), index=False, encoding='utf-8-sig')

print("판매자별 지표 산출 중...")
# 2. 판매자별 (seller_id)
# olist_order_items_dataset에 seller_id가 들어있음
seller_metrics = aggregate_metrics(['seller_id'])
seller_metrics.to_csv(os.path.join(REPORTS_PATH, 'metrics_by_seller.csv'), index=False, encoding='utf-8-sig')

print("상품 카테고리별 지표 산출 중...")
# 3. 상품 카테고리별 (product_category_name)
category_metrics = aggregate_metrics(['product_category_name'])
category_metrics.to_csv(os.path.join(REPORTS_PATH, 'metrics_by_category.csv'), index=False, encoding='utf-8-sig')

print(f"분석 완료! 결과가 {REPORTS_PATH} 디렉토리에 저장되었습니다.")
print("\n--- 지역별 상위 5개 결과 ---")
print(state_metrics.sort_values(by='success_delayed_cnt', ascending=False).head())
