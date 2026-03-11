import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

def load_data():
    try:
        orders = pd.read_parquet(os.path.join(DATA_PATH, 'olist_orders_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(DATA_PATH, 'olist_order_items_dataset.parquet'))
        products = pd.read_parquet(os.path.join(DATA_PATH, 'olist_products_dataset.parquet'))
        reviews = pd.read_parquet(os.path.join(DATA_PATH, 'olist_order_reviews_dataset.parquet'))
    except:
        orders = pd.read_csv(os.path.join(DATA_PATH, 'olist_orders_dataset.csv'))
        order_items = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_items_dataset.csv'))
        products = pd.read_csv(os.path.join(BASE_PATH, 'olist_products_dataset.csv'))
        reviews = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.csv'))

    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    return orders, order_items, products, reviews

def analyze_new_seller_behavior():
    orders, order_items, products, reviews = load_data()
    
    # 데이터 병합
    df = pd.merge(order_items, orders[['order_id', 'order_purchase_timestamp', 'order_status']], on='order_id')
    df = pd.merge(df, products[['product_id', 'product_category_name']], on='product_id', how='left')
    df = pd.merge(df, reviews[['order_id', 'review_score']], on='order_id', how='left') # 리뷰 추가
    
    # 판매자 유형 정의
    seller_first_sale = df.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_sale['is_new_2018'] = seller_first_sale['order_purchase_timestamp'].dt.year == 2018
    seller_first_sale['Seller Type'] = seller_first_sale['is_new_2018'].map({True: '신규 판매자', False: '기존 판매자'})
    
    # 2018년 데이터만 추출
    df_2018 = df[df['order_purchase_timestamp'].dt.year == 2018].copy()
    df_2018 = pd.merge(df_2018, seller_first_sale[['seller_id', 'Seller Type']], on='seller_id')
    
    with open(os.path.join(REPORTS_PATH, 'analysis_new_seller_strategy.txt'), 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("🔍 신규 판매자(2018년 진입)의 저가 판매 전략 분석\n")
        f.write("="*80 + "\n\n")
        
        # 1. 평균 판매 단가 비교
        avg_price = df_2018.groupby('Seller Type')['price'].mean().reset_index()
        f.write("[1] 평균 판매 단가 비교\n")
        f.write("-" * 50 + "\n")
        f.write(avg_price.to_string(index=False))
        f.write("\n\n")
        
        # 2. 주력 판매 카테고리 비교 (TOP 5)
        f.write("[2] 주력 판매 카테고리 TOP 5 (판매량 기준)\n")
        f.write("-" * 50 + "\n")
        
        new_top5 = df_2018[df_2018['Seller Type']=='신규 판매자']['product_category_name'].value_counts().head(5)
        old_top5 = df_2018[df_2018['Seller Type']=='기존 판매자']['product_category_name'].value_counts().head(5)
        
        f.write(f"▶ 신규 판매자 TOP 5 카테고리:\n{new_top5.to_string()}\n\n")
        f.write(f"▶ 기존 판매자 TOP 5 카테고리:\n{old_top5.to_string()}\n\n")
        
        # 3. 같은 카테고리 내 가격 비교 (진짜 싸게 파나?)
        # 공통적으로 많이 파는 카테고리 추출
        common_cats = set(new_top5.index) & set(old_top5.index)
        f.write(f"[3] 공통 인기 카테고리 내 가격 비교\n")
        f.write("-" * 50 + "\n")
        
        cat_price_comp = df_2018[df_2018['product_category_name'].isin(common_cats)].groupby(
            ['product_category_name', 'Seller Type']
        )['price'].mean().unstack()
        
        cat_price_comp['신규가 더 저렴한가?'] = cat_price_comp['신규 판매자'] < cat_price_comp['기존 판매자']
        cat_price_comp['가격 차이(신규-기존)'] = cat_price_comp['신규 판매자'] - cat_price_comp['기존 판매자']
        
        f.write(cat_price_comp.to_string())
        f.write("\n\n👉 해석: 주요 카테고리에서 신규 판매자의 평균 가격이 더 낮다면 '저가 침투 전략'을 쓰고 있는 것.\n\n")
        
        # 4. 리뷰 점수 비교 (싼 게 비지떡?)
        avg_review = df_2018.groupby('Seller Type')['review_score'].mean().reset_index()
        f.write("[4] 평균 고객 만족도 (리뷰 점수 5점 만점)\n")
        f.write("-" * 50 + "\n")
        f.write(avg_review.to_string(index=False))
        f.write("\n\n👉 해석: 신규 판매자의 평점이 더 낮다면 품질/배송/서비스 미숙.\n")

if __name__ == "__main__":
    analyze_new_seller_behavior()
    print("Analysis saved to analysis_new_seller_strategy.txt")
