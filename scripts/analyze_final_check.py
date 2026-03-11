import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

def analyze_top_seller_and_photos():
    # 데이터 로드
    try:
        orders = pd.read_parquet(os.path.join(DATA_PATH, 'olist_orders_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(DATA_PATH, 'olist_order_items_dataset.parquet'))
        products = pd.read_parquet(os.path.join(DATA_PATH, 'olist_products_dataset.parquet'))
    except:
        orders = pd.read_csv(os.path.join(DATA_PATH, 'olist_orders_dataset.csv'))
        order_items = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_items_dataset.csv'))
        products = pd.read_csv(os.path.join(DATA_PATH, 'olist_products_dataset.csv'))

    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

    # 데이터 병합
    df = pd.merge(order_items, orders[['order_id', 'order_purchase_timestamp']], on='order_id')
    df = pd.merge(df, products[['product_id', 'product_photos_qty', 'product_category_name']], on='product_id', how='left')

    # 판매자 유형 정의 (전체 기간 기준 가입일)
    seller_join_date = df.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_join_date.rename(columns={'order_purchase_timestamp': 'join_date'}, inplace=True)
    
    # 2018년 매출 기준 Top Seller 선정
    df_2018 = df[df['order_purchase_timestamp'].dt.year == 2018].copy()
    seller_sales = df_2018.groupby('seller_id').agg(
        total_revenue=('price', 'sum'),
        total_items=('order_item_id', 'count')
    ).reset_index()
    
    seller_sales = pd.merge(seller_sales, seller_join_date, on='seller_id')
    seller_sales['Seller Type'] = seller_sales['join_date'].dt.year.apply(lambda x: '신규(2018)' if x==2018 else '기존(~2017)')
    
    top_seller = seller_sales.sort_values('total_revenue', ascending=False).head(1)
    top_seller_id = top_seller['seller_id'].values[0]
    
    # Top Seller의 사진 사용 습관
    top_seller_products = df_2018[df_2018['seller_id'] == top_seller_id]
    avg_photos = top_seller_products.groupby('product_id')['product_photos_qty'].first().mean()
    
    # 사진 개수 vs 판매량 상관관계 (전체 상품 대상)
    product_stats = df_2018.groupby('product_id').agg(
        sales_volume=('order_item_id', 'count'),
        photos_qty=('product_photos_qty', 'first')
    ).dropna()
    
    correlation = product_stats[['photos_qty', 'sales_volume']].corr().iloc[0, 1]

    with open(os.path.join(REPORTS_PATH, 'analysis_final_check.txt'), 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("매출 1위 판매자 & 사진-판매량 상관관계 분석\n")
        f.write("="*80 + "\n\n")
        
        f.write("[1] 2018년 매출 1위 판매자는 누구인가?\n")
        f.write("-" * 50 + "\n")
        f.write(f"▶ Seller ID: {top_seller_id}\n")
        f.write(f"▶ 유형: {top_seller['Seller Type'].values[0]}\n")
        f.write(f"▶ 가입일: {top_seller['join_date'].dt.strftime('%Y-%m-%d').values[0]}\n")
        f.write(f"▶ 총 매출: R$ {top_seller['total_revenue'].values[0]:,.0f}\n")
        f.write(f"▶ 평균 사진 개수: {avg_photos:.2f}장\n")
        f.write("\n해설: 매출 1위가 신규인지 기존인지, 그리고 사진을 많이 쓰는지 적게 쓰는지 확인.\n\n")
        
        f.write("[2] 사진을 많이 넣을수록 판매량이 늘어나는가?\n")
        f.write("-" * 50 + "\n")
        f.write(f"▶ 상관계수 (Corr): {correlation:.4f}\n")
        f.write("\n해설:\n")
        f.write("- 0.0 ~ 0.2: 사실상 상관없음\n")
        f.write("- 0.2 ~ 0.4: 약한 상관관계 (조금 영향 있음)\n")
        f.write("- 0.4+: 강한 상관관계 (사진 많으면 많이 팔림)\n\n")
        
        # 사진 개수별 평균 판매량
        avg_sales_by_photo = product_stats.groupby('photos_qty')['sales_volume'].mean()
        f.write("▶ 사진 개수별 평균 판매량:\n")
        f.write(avg_sales_by_photo.to_string(float_format="%.1f건"))

if __name__ == "__main__":
    analyze_top_seller_and_photos()
    print("Analysis saved to analysis_final_check.txt")
