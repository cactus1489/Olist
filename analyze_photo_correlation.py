import pandas as pd
import os

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

def analyze_photo_review_correlation():
    # 데이터 로드
    try:
        products = pd.read_parquet(os.path.join(BASE_PATH, 'olist_products_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_items_dataset.parquet'))
        reviews = pd.read_parquet(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.parquet'))
    except:
        products = pd.read_csv(os.path.join(BASE_PATH, 'olist_products_dataset.csv'))
        order_items = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_items_dataset.csv'))
        reviews = pd.read_csv(os.path.join(BASE_PATH, 'olist_order_reviews_dataset.csv'))

    # 데이터 병합
    df = pd.merge(order_items, products[['product_id', 'product_category_name', 'product_photos_qty']], on='product_id')
    df = pd.merge(df, reviews[['order_id', 'review_score']], on='order_id')

    # 주요 카테고리 필터링
    target_cats = ['beleza_saude', 'informatica_acessorios']
    cat_labels = {'beleza_saude': '뷰티/건강', 'informatica_acessorios': 'IT/액세서리'}
    
    df_target = df[df['product_category_name'].isin(target_cats)].copy()
    df_target['Category'] = df_target['product_category_name'].map(cat_labels)
    
    # 사진 개수별 평균 평점 계산
    # 사진 개수가 너무 다양할 수 있으므로 그룹화 (1, 2, 3, 4, 5, 6+)
    def group_photos(qty):
        if pd.isna(qty): return 0
        if qty >= 6: return '6장 이상'
        return f'{int(qty)}장'
        
    df_target['Photo Group'] = df_target['product_photos_qty'].apply(group_photos)
    
    # 결과 집계
    result = df_target.groupby(['Category', 'Photo Group']).agg(
        avg_score=('review_score', 'mean'),
        count=('review_score', 'count')
    ).reset_index()
    
    # 상관계수 계산 (원래 수치 사용)
    correlations = df_target.groupby('Category')[['product_photos_qty', 'review_score']].corr().iloc[0::2, -1]

    with open('analysis_photo_correlation.txt', 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("🔍 사진 개수와 고객 만족도(평점)의 상관관계 분석\n")
        f.write("="*80 + "\n\n")
        
        f.write("[1] 사진 개수별 평균 평점 (사진이 많으면 만족할까?)\n")
        f.write("-" * 50 + "\n")
        
        # 보기 좋게 피벗
        pivot_res = result.pivot(index='Photo Group', columns='Category', values='avg_score')
        # 정렬 (1장, 2장...)
        sort_order = ['0장', '1장', '2장', '3장', '4장', '5장', '6장 이상']
        pivot_res = pivot_res.reindex(sort_order)
        
        f.write(pivot_res.to_string())
        f.write("\n\n")
        
        f.write("[2] 상관계수 (Correlation Coefficient)\n")
        f.write("-" * 50 + "\n")
        f.write(correlations.to_string())
        f.write("\n\n👉 해석: 0에 가까우면 상관없음, 양수(+)면 사진 많을수록 좋음, 음수(-)면 오히려 역효과.\n\n")
        
        f.write("[3] 결론 요약\n")
        for cat in cat_labels.values():
            cat_data = pivot_res[cat]
            diff = cat_data['5장'] - cat_data['1장']
            f.write(f"- [{cat}]: 1장일 때 {cat_data['1장']:.2f}점 -> 5장일 때 {cat_data['5장']:.2f}점 ({diff:+.2f}점 차이)\n")
            
if __name__ == "__main__":
    analyze_photo_review_correlation()
    print("Analysis saved to analysis_photo_correlation.txt")
