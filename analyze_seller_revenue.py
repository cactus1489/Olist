import pandas as pd
import os

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# 데이터 로드
try:
    orders = pd.read_parquet(os.path.join(BASE_PATH, 'olist_orders_dataset.parquet'))
    order_items = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_items_dataset.parquet'))
except:
    orders = pd.read_csv(os.path.join(BASE_PATH, 'olist_orders_dataset.csv'))
    order_items = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_items_dataset.csv'))

orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

# 주문 아이템에 주문 정보 병합
items_with_orders = pd.merge(order_items, orders[['order_id', 'order_purchase_timestamp', 'order_status']], on='order_id')

# 배송 완료된 주문만 필터링
delivered = items_with_orders[items_with_orders['order_status'] == 'delivered'].copy()

# 월별 집계
delivered['year_month'] = delivered['order_purchase_timestamp'].dt.to_period('M')
delivered['year'] = delivered['order_purchase_timestamp'].dt.year
delivered['month'] = delivered['order_purchase_timestamp'].dt.month

monthly_stats = delivered.groupby('year_month').agg(
    active_sellers=('seller_id', 'nunique'),
    total_revenue=('price', 'sum'),
    total_orders=('order_id', 'nunique'),
    total_items=('order_item_id', 'count')
).reset_index()

# 판매자당 평균 매출 계산
monthly_stats['avg_revenue_per_seller'] = monthly_stats['total_revenue'] / monthly_stats['active_sellers']
monthly_stats['avg_revenue_per_order'] = monthly_stats['total_revenue'] / monthly_stats['total_orders']

# 결과를 파일로 저장
with open('analysis_result.txt', 'w', encoding='utf-8') as f:
    f.write("=" * 100 + "\n")
    f.write("월별 판매자 수 vs 매출 추이 분석\n")
    f.write("=" * 100 + "\n")
    f.write(monthly_stats.to_string(index=False) + "\n")

    f.write("\n" + "=" * 100 + "\n")
    f.write("주요 인사이트\n")
    f.write("=" * 100 + "\n")

    # 데이터 기간
    f.write(f"\n📅 데이터 기간: {delivered['order_purchase_timestamp'].min()} ~ {delivered['order_purchase_timestamp'].max()}\n")

    # 판매자 수 증가율
    first_sellers = monthly_stats.iloc[0]['active_sellers']
    last_sellers = monthly_stats.iloc[-1]['active_sellers']
    seller_growth = ((last_sellers - first_sellers) / first_sellers * 100)
    f.write(f"\n👥 판매자 수 변화: {first_sellers} → {last_sellers} ({seller_growth:+.1f}%)\n")

    # 매출 변화율
    first_revenue = monthly_stats.iloc[0]['total_revenue']
    last_revenue = monthly_stats.iloc[-1]['total_revenue']
    revenue_change = ((last_revenue - first_revenue) / first_revenue * 100)
    f.write(f"💰 매출 변화: R$ {first_revenue:,.0f} → R$ {last_revenue:,.0f} ({revenue_change:+.1f}%)\n")

    # 판매자당 평균 매출 변화
    first_avg = monthly_stats.iloc[0]['avg_revenue_per_seller']
    last_avg = monthly_stats.iloc[-1]['avg_revenue_per_seller']
    avg_change = ((last_avg - first_avg) / first_avg * 100)
    f.write(f"📊 판매자당 평균 매출: R$ {first_avg:,.0f} → R$ {last_avg:,.0f} ({avg_change:+.1f}%)\n")

    # 최고 매출 월
    max_month = monthly_stats.loc[monthly_stats['total_revenue'].idxmax()]
    f.write(f"\n🏆 최고 매출 월: {max_month['year_month']} (R$ {max_month['total_revenue']:,.0f})\n")

    # 2018년 데이터 확인
    monthly_2018 = monthly_stats[monthly_stats['year_month'].astype(str).str.startswith('2018')]
    f.write(f"\n⚠️  2018년 데이터:\n")
    f.write(monthly_2018[['year_month', 'active_sellers', 'total_revenue', 'total_orders']].to_string(index=False) + "\n")

    # 마지막 3개월 상세 분석
    f.write("\n📉 마지막 3개월 상세:\n")
    f.write(monthly_stats.tail(3)[['year_month', 'active_sellers', 'total_revenue', 'total_orders', 'avg_revenue_per_seller']].to_string(index=False) + "\n")

    f.write("\n" + "=" * 100 + "\n")
    f.write("결론\n")
    f.write("=" * 100 + "\n")
    f.write("""
🔍 분석 결과:

1. **데이터 불완전성**: 2018년 8월 이후 데이터가 급격히 감소하는 것은 데이터셋이
   2018년 8월까지만 완전하게 수집되었을 가능성이 높습니다.

2. **판매자 증가 vs 매출 감소 역설**: 
   - 판매자 수는 증가하지만 매출이 감소하는 것은 신규 판매자들이 아직 본격적인
     판매를 시작하지 않았거나, 기존 판매자들의 매출이 감소했기 때문입니다.
   
3. **판매자당 평균 매출 감소**: 이는 시장 경쟁이 심화되었거나, 신규 소규모 
   판매자들이 대거 유입되어 평균이 하락했을 가능성을 시사합니다.

4. **실제 비즈니스 vs 데이터 한계**: 마지막 몇 개월의 급격한 변화는 실제 
   비즈니스 성과보다는 데이터 수집 기간의 한계로 보는 것이 타당합니다.
""")

print("Analysis completed. Check analysis_result.txt")
