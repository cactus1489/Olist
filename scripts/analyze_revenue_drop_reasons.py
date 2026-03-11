import pandas as pd
import os
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'reports')

def load_data():
    try:
        orders = pd.read_parquet(os.path.join(DATA_PATH, 'olist_orders_dataset.parquet'))
        order_items = pd.read_parquet(os.path.join(DATA_PATH, 'olist_order_items_dataset.parquet'))
    except:
        orders = pd.read_csv(os.path.join(DATA_PATH, 'olist_orders_dataset.csv'))
        order_items = pd.read_csv(os.path.join(DATA_PATH, 'olist_order_items_dataset.csv'))
    
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    return orders, order_items

def analyze_reasons():
    orders, order_items = load_data()
    
    # 데이터 병합
    df = pd.merge(order_items, orders[['order_id', 'order_purchase_timestamp', 'order_status']], on='order_id')
    delivered = df[df['order_status'] == 'delivered'].copy()
    
    # 월 생성
    delivered['year_month'] = delivered['order_purchase_timestamp'].dt.to_period('M')
    
    # 2018년 데이터만 집중 분석 (트렌드 변화 구간)
    df_2018 = delivered[delivered['order_purchase_timestamp'].dt.year == 2018].copy()
    
    with open(os.path.join(REPORTS_PATH, 'analysis_reasons.txt'), 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("🔍 판매자당 매출 하락 원인 심층 분석 (2018년 기준)\n")
        f.write("="*80 + "\n\n")
        
        # 1. 신규 vs 기존 판매자 성과 비교
        # 각 판매자의 첫 판매일 계산
        seller_first_sale = delivered.groupby('seller_id')['order_purchase_timestamp'].min().reset_index()
        seller_first_sale['is_new_2018'] = seller_first_sale['order_purchase_timestamp'].dt.year == 2018
        
        df_2018 = pd.merge(df_2018, seller_first_sale[['seller_id', 'is_new_2018']], on='seller_id')
        
        new_vs_old = df_2018.groupby('is_new_2018').agg(
            seller_count=('seller_id', 'nunique'),
            total_revenue=('price', 'sum'),
            avg_revenue_per_seller=('price', lambda x: x.sum() / df_2018.loc[x.index, 'seller_id'].nunique())
        ).reset_index()
        
        new_vs_old['type'] = new_vs_old['is_new_2018'].map({True: '신규 진입 (2018년~)', False: '기존 판매자 (~2017년)'})
        
        f.write("[1] 신규 vs 기존 판매자 성과 비교\n")
        f.write("-" * 50 + "\n")
        f.write(new_vs_old[['type', 'seller_count', 'avg_revenue_per_seller']].to_string(index=False))
        f.write("\n\n👉 해석: 신규 진입 판매자들의 평균 매출이 기존 판매자에 비해 현저히 낮은지 확인 필요.\n\n")
        
        # 2. 가격대별 판매 비중 변화 (저가 상품 비중 증가 여부)
        # 가격 구간 설정
        bins = [0, 50, 100, 200, 500, 10000]
        labels = ['매우 저가(0-50)', '저가(50-100)', '중가(100-200)', '고가(200-500)', '초고가(500+)']
        df_2018['price_range'] = pd.cut(df_2018['price'], bins=bins, labels=labels)
        
        price_trend = df_2018.groupby(['year_month', 'price_range']).size().unstack(fill_value=0)
        # 비중으로 변환
        price_ratio = price_trend.div(price_trend.sum(axis=1), axis=0) * 100
        
        f.write("[2] 월별 가격대 상품 판매 비중 (%)\n")
        f.write("-" * 50 + "\n")
        f.write(price_ratio.iloc[:, :3].to_string()) # 저가/중가 위주 출력
        f.write("\n\n👉 해석: '매우 저가' 상품의 비중이 시간이 갈수록 늘어나고 있다면 객단가 하락이 원인.\n\n")
        
        # 3. 상위 독점도 변화 (Gini 계수 약식)
        # 월별 상위 10% 판매자가 차지하는 매출 비중
        f.write("[3] 상위 20% 판매자의 매출 점유율 변화\n")
        f.write("-" * 50 + "\n")
        
        months = sorted(df_2018['year_month'].unique())
        for m in months:
            monthly_data = df_2018[df_2018['year_month'] == m]
            seller_rev = monthly_data.groupby('seller_id')['price'].sum().sort_values(ascending=False)
            
            top_20_count = int(len(seller_rev) * 0.2)
            if top_20_count > 0:
                top_20_revenue = seller_rev.iloc[:top_20_count].sum()
                total_rev = seller_rev.sum()
                ratio = (top_20_revenue / total_rev) * 100
                f.write(f"{m}: 상위 20%가 전체 매출의 {ratio:.1f}% 점유 (판매자 수: {len(seller_rev)}명)\n")
            
        f.write("\n👉 해석: 점유율이 높아진다면 '빈익빈 부익부' 심화로 대다수 하위 판매자의 평균이 깎인 것.\n")

if __name__ == "__main__":
    analyze_reasons()
    print("Analysis saved to analysis_reasons.txt")
