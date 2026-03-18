import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
import numpy as np
# 현재 디렉토리(apps/)를 Python Path에 추가하여 형제 모듈 임포트 가능하게 함
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 기존 파일에서 데이터 로더 함수 재사용
from app_region_stats import load_seller_data, load_and_process_data

def main():
    st.set_page_config(page_title="Olist 비즈니스 딥다이브 (스토리라인)", layout="wide", initial_sidebar_state="expanded")
    
    st.title("💡 Olist 성장 정체 심층 분석 및 액션플랜")
    st.markdown("단건 판매의 함정: 판매자는 폭증하는데, 이탈률은 왜 높은가?")
    
    # 데이터 로드
    with st.spinner("데이터를 불러오고 분석하는 중 (약 5초 소요)..."):
        items_with_sellers, _ = load_seller_data()
        df = items_with_sellers[items_with_sellers['order_status'] == 'delivered'].copy()
        
        # 2018년 8월 31일 이전의 데이터만 유지 (마지막 달 데이터 절단 및 이탈 왜곡 방지)
        df = df[df['order_purchase_timestamp'] <= '2018-08-31'].copy()
        
        # 기준일(집계 마감일) 계산
        max_date = df['order_purchase_timestamp'].max()
        
        # 1. 판매자별 기본 생애주기 프로필 계산
        seller_profile = df.groupby('seller_id')['order_purchase_timestamp'].agg(['min', 'max']).reset_index()
        seller_profile.columns = ['seller_id', '최초 판매일', '마지막 판매일']
        seller_profile['가입월'] = seller_profile['최초 판매일'].dt.to_period('M')
        seller_profile['관찰가능기간(일)'] = (max_date - seller_profile['최초 판매일']).dt.days
        seller_profile['활동수명(일)'] = (seller_profile['마지막 판매일'] - seller_profile['최초 판매일']).dt.days
        
        # 180일 기준 생존 (180일 모수 제외 조건은 개별 탭에서 적용)
        df = pd.merge(df, seller_profile, on='seller_id', how='left')
        df['주문월'] = df['order_purchase_timestamp'].dt.to_period('M')
        
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "1️⃣ 문제정의", 
        "2️⃣ 원인분석", 
        "3️⃣ 분석결과", 
        "4️⃣ 액션플랜", 
        "5️⃣ 시나리오(기대효과)"
    ])
    
    with tab1:
        render_step1(df)
    with tab2:
        render_step2(df, seller_profile)
    with tab3:
        render_step3(df, seller_profile)
    with tab4:
        render_step4()
    with tab5:
        render_step5()

def render_step1(df):
    st.header("1. 문제 정의: 판매자 수는 증가하는데 매출은 정체한다")
    st.markdown(
        "최근 1~2년 간 플랫폼의 신규 판매자 유입 및 활동은 폭발적으로 증가했습니다. "
        "하지만 **전체 거래액(매출) 트렌드는 이에 비례해 성장하지 못하고 박스권에 갇혀 정체**된 모습을 보입니다."
    )
    
    # 월별 활성 판매자수 & 총 매출액 집계
    monthly_stats = df.groupby('주문월').agg(
        revenue=('price', 'sum'),
        active_sellers=('seller_id', 'nunique')
    ).reset_index()
    monthly_stats['주문월_dt'] = monthly_stats['주문월'].dt.to_timestamp()
    # 노이즈를 줄이기 위해 2017년 1월 ~ 2018년 8월 데이터만 사용
    monthly_stats = monthly_stats[(monthly_stats['주문월_dt'] >= '2017-01-01') & (monthly_stats['주문월_dt'] <= '2018-08-31')]

    # 듀얼축 그래프
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 판매자 수 (Bar)
    fig.add_trace(
        go.Bar(x=monthly_stats['주문월_dt'], y=monthly_stats['active_sellers'], name="월 활성 판매자 수", marker_color='rgba(135, 206, 250, 0.6)'),
        secondary_y=False,
    )
    # 매출액 (Line)
    fig.add_trace(
        go.Scatter(x=monthly_stats['주문월_dt'], y=monthly_stats['revenue'], name="총 매출액 (R$)", line=dict(color='#EF5350', width=3)),
        secondary_y=True,
    )
    
    fig.update_layout(
        title_text="월별 <b>활성 판매자 수</b> vs <b>플랫폼 총 매출액</b> 추이",
        hovermode="x unified",
        height=500
    )
    fig.update_yaxes(title_text="판매자 수(명)", secondary_y=False)
    fig.update_yaxes(title_text="매출액 (R$)", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 추가: 월별 상세 데이터 표
    with st.expander("📊 월별 상세 데이터 확인하기"):
        df_step1_show = monthly_stats[['주문월', 'active_sellers', 'revenue']].copy()
        df_step1_show.columns = ['주문월', '활성 판매자 수(명)', '매출액 (R$)']
        df_step1_show['주문월'] = df_step1_show['주문월'].astype(str)
        st.dataframe(
            df_step1_show.style.format({
                '활성 판매자 수(명)': '{:,}',
                '매출액 (R$)': '{:,.0f}'
            }).background_gradient(subset=['매출액 (R$)'], cmap='Reds')
              .background_gradient(subset=['활성 판매자 수(명)'], cmap='Blues'),
            use_container_width=True
        )

    st.error("💡 **인사이트**: 2018년 들어 활성 판매자 수(파란 막대)는 계속 우상향하고 있으나, 동기간 총 매출액(빨간 선) 상승률은 눈에 띄게 둔화되어 있습니다. 판매자 규모 확대가 실제 매출 볼륨으로 이어지지 않는 본질적인 병목이 존재합니다.")

def render_step2(df, seller_profile):
    st.header("2. 원인 분석: 엄청난 신규 가입, 그러나 그만큼 많은 이탈")
    st.markdown(
        "왜 매출이 정체될까요? 이를 알아보기 위해 판매자를 **신규/기존**으로 나누고, 나아가 "
        "신규 판매자 중 가입 초기(첫 달)에 단 1건만 팔고 관둬버리는 **'단건 판매자' 비율**을 추적했습니다."
    )
    
    # --- 데이터 통합 계산 (그래프 및 표 공용) ---
    # 월별 신규/기존/총 활성 집계
    monthly_type = df.groupby(['주문월', 'seller_id', '가입월']).size().reset_index()
    monthly_type['is_new'] = monthly_type['주문월'] == monthly_type['가입월']
    
    summary_stats = monthly_type.groupby('주문월').agg(
        총_활성_판매자=('seller_id', 'nunique'),
        신규_판매자=('is_new', 'sum'),
        기존_판매자=('is_new', lambda x: (~x).sum())
    ).reset_index()
    
    # 월별 이탈자 집계
    seller_last_month = seller_profile[['seller_id', '마지막 판매일']].copy()
    seller_last_month['마지막_거래월'] = seller_last_month['마지막 판매일'].dt.to_period('M')
    
    active_with_last = pd.merge(monthly_type[['주문월', 'seller_id']], seller_last_month, on='seller_id', how='left')
    active_with_last['is_churned'] = active_with_last['주문월'] == active_with_last['마지막_거래월']
    
    churn_counts = active_with_last.groupby('주문월').agg(
        이탈자=('is_churned', 'sum')
    ).reset_index()
    
    # 병합 및 필터링
    trend_df = pd.merge(summary_stats, churn_counts, on='주문월')
    trend_df['월_dt'] = trend_df['주문월'].dt.to_timestamp()
    trend_df = trend_df[(trend_df['월_dt'] >= '2017-01-01') & (trend_df['월_dt'] <= '2018-08-31')]
    # ------------------------------------------

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📉 신규 유입 vs 이탈자 추이")
        fig1 = go.Figure()
        # 신규 유입 (Bar)
        fig1.add_trace(go.Bar(
            x=trend_df['월_dt'], 
            y=trend_df['신규_판매자'], 
            name="신규 유입", 
            marker_color='#81C784'
        ))
        # 이탈자 (Bar)
        fig1.add_trace(go.Bar(
            x=trend_df['월_dt'], 
            y=trend_df['이탈자'], 
            name="이탈(마지막 거래)", 
            marker_color='#E53935'
        ))
        
        fig1.update_layout(
            barmode='group', 
            hovermode="x unified", 
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("**현상**: 신규 유입(초록)이 늘어나는 만큼 이탈자(빨강)도 가파르게 따라붙고 있습니다. 특히 2018년 들어 그 간격이 좁혀지며 '순증' 효과가 미비해지고 있습니다.")

    with col2:
        st.subheader("⚠️ 신규 집단 내 '가입 1개월 차 단건 판매' 비중")
        # 가입 후 첫 30일(1달) 이내에 판매한 주문 건수(order_id nunique)
        df_m1 = df[(df['order_purchase_timestamp'] - df['최초 판매일']).dt.days <= 30]
        m1_counts = df_m1.groupby('seller_id')['order_id'].nunique().reset_index()
        m1_counts.columns = ['seller_id', 'm1_orders']
        
        sp_m1 = pd.merge(seller_profile, m1_counts, on='seller_id', how='left').fillna({'m1_orders': 0})
        sp_m1['is_single_sale_m1'] = sp_m1['m1_orders'] == 1
        
        single_monthly = sp_m1.groupby('가입월').agg(
            total_new=('seller_id', 'count'),
            single_sale=('is_single_sale_m1', 'sum')
        ).reset_index()
        single_monthly['single_ratio'] = (single_monthly['single_sale'] / single_monthly['total_new']) * 100
        single_monthly['월_dt'] = single_monthly['가입월'].dt.to_timestamp()
        single_monthly = single_monthly[(single_monthly['월_dt'] >= '2017-01-01') & (single_monthly['월_dt'] <= '2018-08-31')]
        
        fig2 = px.line(single_monthly, x='월_dt', y='single_ratio', markers=True, color_discrete_sequence=['#FFB300'])
        fig2.update_yaxes(title="단건 판매 집단 비중 (%)", range=[0, 100])
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
        
        # 추가: 단건 판매 비중 상세 데이터 표
        with st.expander("📂 월별 단건 판매 비중 상세 데이터 보기"):
            df_single_show = single_monthly[['가입월', 'total_new', 'single_sale', 'single_ratio']].copy()
            df_single_show.columns = ['가입월', '신규 유입', '단건 판매자', '비중 (%)']
            df_single_show['가입월'] = df_single_show['가입월'].astype(str)
            st.dataframe(
                df_single_show.style.format({
                    '신규 유입': '{:,}',
                    '단건 판매자': '{:,}',
                    '비중 (%)': '{:.1f}%'
                }).background_gradient(subset=['비중 (%)'], cmap='YlOrRd'),
                use_container_width=True
            )

        st.markdown("**현상**: 신규 가입자 10명 중 약 4~5명(40~50% 내외)은 **가입 첫 달에 단 1건만 팔고 맙니다.**")

    # 3) 월별 종합 활동 현황 (신규/기존/이탈/매출)
    st.divider()
    st.subheader("📊 월별 판매자 활동 및 매출 종합 현황")
    st.markdown("매월 플랫폼 내 판매자 유입, 활동 유지, 이탈 및 그에 따른 매출 기여도를 종합적으로 분석합니다.")
    
    # 매출액 데이터 별도 계산 후 병합
    monthly_rev = df.groupby('주문월')['price'].sum().reset_index().rename(columns={'price': '매출액'})
    final_monthly_stats = pd.merge(trend_df, monthly_rev, on='주문월')
    
    final_monthly_stats['이탈 비중 (%)'] = (final_monthly_stats['이탈자'] / final_monthly_stats['총_활성_판매자'] * 100).round(1)
    final_monthly_stats['주문월_str'] = final_monthly_stats['주문월'].astype(str)
    
    df_final_show = final_monthly_stats[['주문월_str', '매출액', '총_활성_판매자', '신규_판매자', '기존_판매자', '이탈자', '이탈 비중 (%)']].copy()
    df_final_show.columns = ['월', '매출액 (R$)', '총 활성 판매자', '신규 판매자', '기존 판매자', '이탈자', '이탈 비중 (%)']
    
    st.dataframe(
        df_final_show.style.format({
            '매출액 (R$)': '{:,.0f}',
            '총 활성 판매자': '{:,}',
            '신규 판매자': '{:,}',
            '기존 판매자': '{:,}',
            '이탈자': '{:,}',
            '이탈 비중 (%)': '{:.1f}%'
        }).background_gradient(subset=['이탈 비중 (%)'], cmap='OrRd')
          .background_gradient(subset=['매출액 (R$)'], cmap='Greens'),
        use_container_width=True
    )
    
    st.caption("※ 마지막 달(2018-08)은 데이터 수집 종료 시점으로 인해 이탈자 수가 실제보다 높게 나타납니다.")
        
def render_step3(df, seller_profile):
    st.header("3. 분석 결과 (장기 생존 스위트스팟 파악)")
    st.markdown(
        "그렇다면 첫 달(M1)에 몇 건 이상을 판매해야 이탈하지 않고 **장기 판매자**로 자리 잡을 수 있을까요? "
        "활동수명(마지막 판매일 - 최초 판매일)을 기준으로 180일 이상 생존한 인원을 파악합니다. \n\n"
        "*(📌 **모수 보정 적용**: 아직 가입한지 180일이 지나지 않아 생존 여부를 단정할 수 없는 기회 부족군은 전체 모수에서 배제했습니다.)*"
    )
    
    # 180일 미만 기회 부족군 제외
    valid_profiles = seller_profile[seller_profile['관찰가능기간(일)'] >= 180].copy()
    num_excluded = len(seller_profile) - len(valid_profiles)
    
    st.info(f"💡 180일 미만 모수 필터링 처리로 총 {len(seller_profile):,}명 중 {num_excluded:,}명 변환 제외 -> **분석 유효 모수: {len(valid_profiles):,}명**")

    # M1 건수 집계 (유효 모수에 대해서만)
    df_valid = df[df['seller_id'].isin(valid_profiles['seller_id'])]
    df_m1 = df_valid[(df_valid['order_purchase_timestamp'] - df_valid['최초 판매일']).dt.days <= 30]
    
    # 판매자별 결론 도출 (첫달 N건, 180일 이상 생존 여부, 총 발생 매출액)
    seller_stats = df_m1.groupby('seller_id').agg(
        m1_orders=('order_id', 'nunique'),
        m1_revenue=('price', 'sum')
    ).reset_index()
    
    # 병합
    final_stats = pd.merge(valid_profiles, seller_stats, on='seller_id', how='left').fillna({'m1_orders': 0, 'm1_revenue': 0})
    final_stats['장기생존여부'] = final_stats['활동수명(일)'] >= 180
    
    # 구간화 (1건, 2건, 3~5건, 6~10건, 11건 이상)
    def bucketize_orders(x):
        if x == 0: return '0건'
        if x == 1: return '1건'
        elif x == 2: return '2건'
        elif 3 <= x <= 5: return '3~5건'
        elif 6 <= x <= 10: return '6~10건'
        else: return '11건 이상'
        
    final_stats['M1 판매건수 구간'] = final_stats['m1_orders'].apply(bucketize_orders)
    
    # 요약 표 만들기
    summary = final_stats.groupby('M1 판매건수 구간').agg(
        전체_판매자_수=('seller_id', 'count'),
        장기_생존자_수=('장기생존여부', 'sum'),
        총_매출=('m1_revenue', 'sum')
    )
    
    summary['장기_생존율_전환율(%)'] = (summary['장기_생존자_수'] / summary['전체_판매자_수'] * 100).round(1)
    summary['판매자당_첫달평균매출(R$)'] = (summary['총_매출'] / summary['전체_판매자_수']).round(1)
    
    # 정렬
    order_cat = ['0건', '1건', '2건', '3~5건', '6~10건', '11건 이상']
    summary = summary.reindex(order_cat).reset_index()
    summary = summary.rename(columns={'전체_판매자_수': '전체 유효판매자 수', '장기_생존자_수': '장기(180일+) 생존자 수'})
    
    # UI 표시
    st.subheader("📊 신규 진입 후 첫 달(M1) 판매 건수 구간별 생존 성과")
    st.dataframe(
        summary.style.background_gradient(subset=['장기_생존율_전환율(%)'], cmap='Blues', vmin=0, vmax=100)
                     .format({
                         '전체 유효판매자 수': '{:,}', 
                         '장기(180일+) 생존자 수': '{:,}', 
                         '장기_생존율_전환율(%)': '{:.1f}%',
                         '총_매출': '{:,.0f}',
                         '판매자당_첫달평균매출(R$)': '{:,.1f}'
                     }), 
        use_container_width=True
    )
    
    st.success("🔥 **분석 결론 (Sweet Spot)**: 표를 보면, M1에 **딱 1건만 팔았을 때의 생존율은 약 34%**에 불과하나, **2건을 팔면 56%**, **3건 이상부터는 60% 후반대로 생존율이 수직 상승**함을 알 수 있습니다. 즉, 한 달 간 최소 **2~3건 이상(N=3)** 팔게 만드는 것이 장기 생존의 열쇠입니다.")

    # --- 추가: 리텐션 코호트 분석 섹션 ---
    st.divider()
    st.subheader("📊 데이터적 근거: 코호트 리텐션 매트릭스 (180일 보정)")
    st.markdown(
        "위에서 정의한 '180일(6개월) 생존'이라는 기준이 왜 타당한지 코호트 분석으로 증명합니다. "
        "가입 시점(동기 집단)이 같은 판매자들이 월별로 얼마나 재활동하는지 추적해 보면, 리텐션이 평탄화(Plateau)되는 지점을 알 수 있습니다."
    )

    with st.spinner("코호트 리텐션 매트릭스 계산 중..."):
        # 코호트 분석을 위한 데이터 준비 (유효 모수 대상)
        df_cohort = df_valid[['seller_id', 'order_purchase_timestamp', '최초 판매일']].copy()
        df_cohort['OrderMonth'] = df_cohort['order_purchase_timestamp'].dt.to_period('M')
        df_cohort['CohortMonth'] = df_cohort['최초 판매일'].dt.to_period('M')
        
        # 코호트 인덱스 계산 (경과 월)
        order_year = df_cohort['OrderMonth'].dt.year
        order_month = df_cohort['OrderMonth'].dt.month
        cohort_year = df_cohort['CohortMonth'].dt.year
        cohort_month = df_cohort['CohortMonth'].dt.month
        
        df_cohort['CohortIndex'] = (order_year - cohort_year) * 12 + (order_month - cohort_month) + 1
        
        # 매트릭스 생성
        cohort_counts = df_cohort.groupby(['CohortMonth', 'CohortIndex'])['seller_id'].nunique().unstack(fill_value=0)
        
        # 첫 달 대비 비율 산출
        cohort_sizes = cohort_counts.iloc[:, 0]
        retention = cohort_counts.divide(cohort_sizes, axis=0) * 100
        
        # 표시 최적화 (12개월까지, 문자열 인덱스)
        retention.index = retention.index.astype(str)
        max_cols = min(12, len(retention.columns))
        retention_display = retention.iloc[:, :max_cols].round(1)

    st.markdown("##### 📝 가입 월별 코호트의 개월 차 재활동 유지율 (%)")
    st.dataframe(
        retention_display.style.background_gradient(cmap='Blues', axis=None, vmin=0, vmax=100)
                              .format("{:.1f}%", na_rep="")
                              .highlight_null(color='white'),
        use_container_width=True,
        height=400
    )
    
    st.info(
        "💡 **차트 해석**: 가입 초기(Index 1~3)에는 리텐션이 급격히 낮아지지만, **Index 6(6개월 차)** 전후를 기점으로 "
        "유지율 하락이 멈추고 일정 수준으로 안정화되는 양상을 보입니다. 이는 180일(6개월)을 버틴 판매자가 "
        "플랫폼의 안정적인 '장기 파트너'로 정착했음을 시각적으로 증명합니다."
    )

def render_step4():
    st.header("4. 액션 플랜 (결론 플랫폼 정책 제언)")
    st.markdown("분석를 바탕으로, 매출 정체 파훼를 위한 '스위트스팟 도달(M1 내 3건 이상 판매)' 집중 육성 액션 플랜을 제안합니다.")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("🎯 **액션 플랜 1: 신규 입점 부스팅**\n\n"
                "가입 후 최초 30일 동안 신규 판매자의 상품이 노출 알고리즘 상위에 자리잡도록 **'루키 태그(Rookie Tag)'**를 부여하고 검색 랭킹 가중치를 단기적으로 높입니다.")
    with col2:
        st.warning("🎁 **액션 플랜 2: 목표 달성 인센티브**\n\n"
                   "첫 달 안에 **3건 이상의 판매**를 달성한 판매자에게는 익월 수수료를 한시적으로 인하해주거나, 자체 프로모션 참가 자격을 부여하여 리텐션 모멘텀을 이어가도록 합니다.")
    with col3:
        st.success("🛒 **액션 플랜 3: 구매자향 쿠폰 발행**\n\n"
                   "신규 판매자가 자신의 첫 1~3번째 구매자에게 발급할 수 있는 **'스타터 할인 쿠폰(플랫폼 50% 부담)'**을 제공하여, 단건 판매자가 두 번째, 세 번째 고객을 유치할 허들을 극도로 낮춥니다.")

def render_step5():
    st.header("5. 시나리오 및 기대효과 시뮬레이션")
    st.markdown("위 액션플랜을 실행하여 **'단건 판매 후 이탈자'를 3건 이상 판매자로 전환했을 때** 플랫폼에 발생하는 재무적 기대효과 계산기입니다.")
    
    st.divider()
    
    c1, c2 = st.columns([1, 2])
    
    with c1:
        st.subheader("⚙️ 시나리오 파라미터 조작")
        total_1_sale_sellers = 1500 # 분석 결과 1건 판매자의 예시(모수)
        avg_ltv_1_sale = 250    # R$
        avg_ltv_success = 4500  # R$ (장기생존자 평균 발생 매출)
        
        conversion_rate = st.slider("단건 판매자 -> 3건 이상(장기생존) 전환 성공 목표율 (%)", min_value=1, max_value=50, value=15, step=1)
        
        converted_sellers = int(total_1_sale_sellers * (conversion_rate / 100))
        
        added_revenue_per_seller = avg_ltv_success - avg_ltv_1_sale
        total_impact = converted_sellers * added_revenue_per_seller
        
        st.write(f"- 대상이 되는 월평균 1건 이탈 판매자 모수: **{total_1_sale_sellers}명**")
        st.write(f"- 전환 성공으로 장기 생존할 유저: **{converted_sellers}명**")
        
    with c2:
        st.subheader("📈 누적(생애 가치) 기대 효과")
        
        cc1, cc2 = st.columns(2)
        cc1.metric(label="월 생존 판매자 추가 확보 수", value=f"+ {converted_sellers}명")
        cc2.metric(label="플랫폼 월 누적 총매출(LTV) 증가액", value=f"+ R$ {total_impact:,.0f}", delta=f"({conversion_rate}% 성공 시)")
        
        st.markdown("\n\n* **시뮬레이션 로직**: (단건 이탈자의 LTV vs 장기 생존자의 LTV 차액) × 전환된 판매자 수. "
                    "첫 달에 1건만 팔고 포기하는 판매자를 단 **15%**만 부스팅시켜 장기 판매자로 전환해도 플랫폼 매출 볼륨에 막대한 복리적 임팩트가 발생합니다.")

if __name__ == "__main__":
    main()
