import pandas as pd
import os

# 데이터 경로 설정
base_path = r'c:\Users\dlstj\OneDrive\Desktop\ICB6\miniProject'

print("데이터 로딩 중...")
# 데이터 로드 (Parquet 파일이 있으면 Parquet을 우선 사용)
try:
    orders = pd.read_parquet(os.path.join(base_path, 'olist_orders_dataset.parquet'))
    customers = pd.read_parquet(os.path.join(base_path, 'olist_customers_dataset.parquet'))
except:
    orders = pd.read_csv(os.path.join(base_path, 'olist_orders_dataset.csv'))
    customers = pd.read_csv(os.path.join(base_path, 'olist_customers_dataset.csv'))

# 날짜 컬럼 변환
date_columns = [
    'order_purchase_timestamp', 'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]
for col in date_columns:
    orders[col] = pd.to_datetime(orders[col])

# 데이터 병합 (주문 + 고객)
df = pd.merge(orders, customers[['customer_id', 'customer_state']], on='customer_id', how='left')

# 데이터 스냅샷 기준일 (실패 지연 판단용)
snapshot_date = df['order_purchase_timestamp'].max()

# 1. 성공건(delivered)과 실패건(그 외) 분리
df['is_success'] = df['order_status'] == 'delivered'

# 2. 지연 정의
# 성공 지연: 실제 배송 완료일 > 배송 예정일
df['is_success_delay'] = (df['is_success']) & (df['order_delivered_customer_date'] > df['order_estimated_delivery_date'])

# 실패 지연: 아직 배송 안 됨(성공 아님) & 현재 시점이 배송 예정일보다 지남
df['is_failure_delay'] = (~df['is_success']) & (df['order_estimated_delivery_date'] < snapshot_date)

# 3. 소요 시간 계산 (일 단위)
# 성공건: 구매 시점 ~ 배송 완료 시점
df['delivery_time_success'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days

# 실패건: 구매 시점 ~ 마지막 업데이트(또는 예정일) - 여기서는 보수적으로 예정일까지로 계산하거나 데이터상의 마지막 시점 활용
# 여기서는 '구매 시점'부터 '예정일'까지를 일종의 실패까지 걸린 시간(또는 지연 시간의 기준)으로 사용
df['delivery_time_failure'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.days

# 지역별 통계 계산
# 주요 지역(SP, RJ, MG)을 포함한 모든 지역에 대해 집계
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

# 컬럼명 정리
region_stats = region_stats.rename(columns={
    'customer_state': 'State',
    'total_orders': 'Total Orders',
    'avg_time_success': 'Avg Time Success (Days)',
    'avg_time_failure': 'Avg Time Failure (Days)'
})

# 출력용 데이터프레임 (주요 3개 지역 우선 표시)
target_states = ['SP', 'RJ', 'MG']
final_display = region_stats[region_stats['State'].isin(target_states)].copy()

print("\n--- 지역별 상세 배송 통계 (성공/실패 지연 분석) ---")
columns_to_show = [
    'State', 'Total Orders', 
    'Success Delay Ratio (%)', 'Failure Delay Ratio (%)', 
    'Total Delay Ratio (%)', 'Avg Time Success (Days)', 'Avg Time Failure (Days)'
]
print(final_display[columns_to_show].to_string(index=False))

# 전체 상위 10개 지역 (지연율 기준)
print("\n--- 전체 지역 중 지연율(Total) 상위 10개 지역 ---")
print(region_stats.sort_values(by='Total Delay Ratio (%)', ascending=False)[columns_to_show].head(10).to_string(index=False))
