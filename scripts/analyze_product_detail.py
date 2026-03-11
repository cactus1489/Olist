import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

def analyze_product_quality():
    try:
        orders = pd.read_parquet(os.path.join(DATA_PATH, 'olist_orders_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(DATA_PATH, 'olist_order_items_dataset.parquet'))
        products = pd.read_parquet(os.path.join(DATA_PATH, 'olist_products_dataset.parquet'))
    except:
        # csv fallback은 생략
        orders = pd.read_csv(os.path.join(DATA_PATH, 'olist_orders_dataset.csv'))
        order_items = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_items_dataset.csv'))
        products = pd.read_csv(os.path.join(BASE_PATH, 'olist_products_dataset.csv'))

    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

    # 데이터 병합
    df = pd.merge(order_items, orders[['order_id', 'order_purchase_timestamp']], on='order_id')
    df = pd.merge(df, products, on='product_id', how='left')

    # 2018년 데이터 필터링
    df_2018 = df[df['order_purchase_timestamp'].dt.year == 2018].copy()

    # 판매자 유형 정의
    seller_first_sale = df.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
    seller_first_sale['is_new_2018'] = seller_first_sale['order_purchase_timestamp'].dt.year == 2018
    seller_first_sale['Seller Type'] = seller_first_sale['is_new_2018'].map({True: '신규 판매자', False: '기존 판매자'})

    df_2018 = pd.merge(df_2018, seller_first_sale[['seller_id', 'Seller Type']], on='seller_id')

    # 분석 대상 카테고리
    target_cats = ['beleza_saude', 'informatica_acessorios']
    cat_labels = {'beleza_saude': '뷰티/건강', 'informatica_acessorios': 'IT/액세서리'}

    df_target = df_2018[df_2018['product_category_name'].isin(target_cats)].copy()
    df_target['Category'] = df_target['product_category_name'].map(cat_labels)

    # 지표 비교 (설명 길이, 사진 개수)
    result = df_target.groupby(['Category', 'Seller Type']).agg(
        avg_price=('price', 'mean'),
        avg_desc_len=('product_description_lenght', 'mean'),
        avg_photos=('product_photos_qty', 'mean'),
        item_count=('order_item_id', 'count')
    ).reset_index()

    with open(os.path.join(REPORTS_PATH, 'analysis_product_detail.txt'), 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("🔍 신규 판매자의 상품 상세 정보(품질) 분석 (뷰티 vs IT)\n")
        f.write("="*80 + "\n\n")
        f.write(result.to_string(index=False))
        f.write("\n\n")

        # 해석 로직
        it_new = result[(result['Category'] == 'IT/액세서리') & (result['Seller Type'] == '신규 판매자')]
        it_old = result[(result['Category'] == 'IT/액세서리') & (result['Seller Type'] == '기존 판매자')]
        
        if not it_new.empty and not it_old.empty:
            desc_diff = it_new['avg_desc_len'].values[0] - it_old['avg_desc_len'].values[0]
            photo_diff = it_new['avg_photos'].values[0] - it_old['avg_photos'].values[0]
            
            f.write("👉 [IT/액세서리] 심층 분석:\n")
            f.write(f"- 신규 판매자가 비싸게 파는 대신, 상품 설명이 {desc_diff:+.1f}자 더 {('길고' if desc_diff > 0 else '짧고')}, ")
            f.write(f"사진을 {photo_diff:+.1f}장 더 {('많이' if photo_diff > 0 else '적게')} 씁니다.\n")
            
            if desc_diff > 0 or photo_diff > 0:
                f.write("✅ 결론: 역시! 비싼 이유가 있었습니다. 신규 판매자는 '상세 정보 고급화'로 신뢰를 얻어 비싼 값을 받고 있습니다.\n")
            else:
                f.write("❓ 결론: 상품 정보 양에는 큰 차이가 없네요. 다른 이유(틈새 시장, 희귀템 등)일 수 있습니다.\n")

if __name__ == "__main__":
    analyze_product_quality()
    print("Analysis saved to analysis_product_detail.txt")
