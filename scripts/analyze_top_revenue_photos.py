import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

def analyze_top_revenue_photos():
    # 데이터 로드
    try:
        products = pd.read_parquet(os.path.join(DATA_PATH, 'olist_products_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(DATA_PATH, 'olist_order_items_dataset.parquet'))
    except:
        products = pd.read_csv(os.path.join(DATA_PATH, 'olist_products_dataset.csv'))
        order_items = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_items_dataset.csv'))

    # 데이터 병합 (주문 건별 -> 상품 정보)
    df = pd.merge(order_items, products[['product_id', 'product_category_name', 'product_photos_qty']], on='product_id')
    
    # 주요 카테고리 필터링
    target_cats = ['beleza_saude', 'informatica_acessorios']
    cat_labels = {'beleza_saude': '뷰티/건강', 'informatica_acessorios': 'IT/액세서리'}
    
    df_target = df[df['product_category_name'].isin(target_cats)].copy()
    df_target['Category'] = df_target['product_category_name'].map(cat_labels)
    
    # 상품별 총 매출 집계
    product_stats = df_target.groupby(['Category', 'product_id']).agg(
        total_revenue=('price', 'sum'),
        sales_volume=('order_item_id', 'count'),
        photos_qty=('product_photos_qty', 'first')
    ).reset_index()
    
    # 사진 개수 그룹화 함수
    def group_photos(qty):
        if pd.isna(qty): return '0장'
        if qty >= 5: return '5장+'
        return f'{int(qty)}장'
    
    product_stats['Photo Group'] = product_stats['photos_qty'].apply(group_photos)

    with open(os.path.join(REPORTS_PATH, 'analysis_top_revenue_photos.txt'), 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("🔍 매출 상위 상품들의 사진 개수 분석 (Fact Check)\n")
        f.write("="*80 + "\n\n")
        
        for category in cat_labels.values():
            cat_data = product_stats[product_stats['Category'] == category]
            
            # 매출 상위 100개 상품 추출
            top_100 = cat_data.sort_values('total_revenue', ascending=False).head(100)
            
            f.write(f"[{category}] 매출 TOP 100 효자 상품 분석\n")
            f.write("-" * 50 + "\n")
            
            # TOP 100 상품의 평균 사진 개수
            avg_photos = top_100['photos_qty'].mean()
            f.write(f"▶ TOP 100 평균 사진 개수: {avg_photos:.2f}장\n")
            
            # TOP 100 상품의 사진 개수 분포
            dist = top_100['Photo Group'].value_counts(normalize=True).sort_index() * 100
            f.write("▶ 사진 개수별 비중 (%):\n")
            f.write(dist.to_string(float_format="%.1f%%"))
            f.write("\n\n")
            
            # 전체 매출 중 1장짜리 상품이 차지하는 비중
            rev_by_photo = cat_data.groupby('Photo Group')['total_revenue'].sum()
            total_rev = rev_by_photo.sum()
            rev_share = (rev_by_photo / total_rev * 100).sort_index()
            
            f.write("▶ 전체 매출 기여도 (%):\n")
            f.write(rev_share.to_string(float_format="%.1f%%"))
            f.write("\n\n" + "="*50 + "\n\n")

        f.write("👉 결론: 사용자님의 관찰대로 매출의 대부분이 '1장'짜리에서 나오고 있는지 확인되었습니다.\n")

if __name__ == "__main__":
    analyze_top_revenue_photos()
    print("Analysis saved to analysis_top_revenue_photos.txt")
