import pandas as pd
import os

# 경로 설정
BASE_PATH = r'c:\Users\dlstj\OneDrive\Desktop\ICB6\miniProject'
files_to_convert = [
    'olist_orders_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_products_dataset.csv'
]

print("🚀 데이터 변환 시작 (CSV -> Parquet)...")

for file in files_to_convert:
    csv_path = os.path.join(BASE_PATH, file)
    parquet_path = csv_path.replace('.csv', '.parquet')
    
    if os.path.exists(csv_path):
        print(f"📦 변환 중: {file}...")
        df = pd.read_csv(csv_path)
        # 생성된 날짜 컬럼 등 미리 변환하여 저장하면 로딩 시 더 빠름
        if 'timestamp' in file or 'date' in file or 'order_purchase_timestamp' in df.columns:
            for col in df.columns:
                if 'timestamp' in col or 'date' in col:
                    df[col] = pd.to_datetime(df[col])
        
        df.to_parquet(parquet_path, index=False)
        print(f"✅ 완료: {parquet_path} (용량 대폭 감소)")
    else:
        print(f"⚠️ 파일을 찾을 수 없음: {file}")

print("\n✨ 모든 변환 작업이 완료되었습니다.")
