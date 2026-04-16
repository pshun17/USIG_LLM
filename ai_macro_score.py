"""
ai_macro_score.py
─────────────────
현재 매크로 환경(2026년 4월) 기반 정성적 AI 점수 산출

[배경 논리]
- Fed 인하 사이클 진행 중이나 트럼프 관세로 인플레이션 재점화 우려 공존
- IG 스프레드 역사적으로 타이트 → 캐리 중심, 방어적 접근 유효
- 커브 스티프닝 압력 (재정적자 우려) → 장기물 불리, 5-10년 유리
- 관세 직격 섹터 (자동차, 소매, 소비재) 마진 압박, 공급망 불확실성

[3개 세부 점수]
1. Subgroup_Score  : Industry Subgroup 기반 세부 업종 점수 (-1 ~ +1)
2. Maturity_Score  : OAD 기반 만기 포지셔닝 (-1 ~ +1)
3. RatingBuf_Score : Fallen Angel 위험도 (-1 ~ +1)

[최종]
AI_Macro_Score = Subgroup×0.40 + Maturity×0.35 + RatingBuf×0.25 → [-1, +1]
"""

import numpy as np
import pandas as pd

# ─── 1. Industry Subgroup 기반 세부 업종 점수 ─────────────────────────────────
# 현재 매크로 환경 기반 정성 점수
# 각 점수 근거 주석 포함

SUBGROUP_SCORE_MAP = {

    # ══ UTILITIES ══════════════════════════════════════════════════════
    'Electric-Integrated':        0.85,  # 규제 수익, 관세 무관, 방어적 캐리
    'Electric-Distribution':      0.85,  # 동일 논리
    'Electric-Transmission':      0.80,  # 인프라 투자 수혜 (에너지 전환)
    'Electric-Generation':        0.75,  # 약간 더 노출, 연료 비용 변동
    'Gas-Distribution':           0.80,  # 규제 유틸리티, 안정적 CF
    'Water':                      0.85,  # 가장 방어적, 경기 무관
    'Non-hazardous Waste Disp':   0.70,  # 방어적 서비스, 규제 수익

    # ══ HEALTHCARE ═════════════════════════════════════════════════════
    'Medical-Drugs':              0.70,  # 방어적, 관세 일부 위험(원료) 있으나 우선순위 구매
    'Medical-Hospitals':          0.75,  # 내수 서비스, 관세 무관
    'Medical-HMO':                0.65,  # 보험료 인상 압박 vs 방어적 수익
    'Medical-Biomedical/Gene':    0.60,  # 성장성 있으나 금리 민감
    'Medical Products':           0.60,  # 관세 일부(의료기기 수입) 위험
    'Medical Instruments':        0.55,
    'Medical Labs&Testing Srv':   0.65,  # 방어적 서비스
    'Pharmacy Services':          0.60,
    'Medical-Whsle Drug Dist':    0.55,
    'Medical Imaging Systems':    0.55,
    'Diagnostic Equipment':       0.55,
    'Drug Delivery Systems':      0.55,
    'Medical-Generic Drugs':      0.50,
    'Phys Practice Mgmnt':        0.60,
    'Medical-Outptnt/Home Med':   0.60,

    # ══ BANKING / FINANCIAL ════════════════════════════════════════════
    'Diversified Banking Inst':   0.55,  # 커브 스티프닝 수혜 (NIM 개선), 자본 충분
    'Super-Regional Banks-US':    0.60,  # 미국 내수 강점, 관세 무관
    'Commer Banks-Eastern US':    0.55,
    'Commer Banks-Southern US':   0.55,
    'Commer Banks-Central US':    0.50,
    'Commer Banks-Western US':    0.50,
    'Commer Banks Non-US':        0.30,  # 달러 강세, 지정학 리스크
    'Money Center Banks':         0.45,  # 대형은행 규제 불확실성
    'Fiduciary Banks':            0.65,  # NTRS, STT — 수탁 사업, 금리 수혜
    'Life/Health Insurance':      0.50,  # 금리 상승 투자수익 개선 vs 언더라이팅
    'Property/Casualty Ins':      0.55,  # 보험료 인상 사이클, 견조
    'Reinsurance':                0.50,
    'Multi-line Insurance':       0.45,
    'Insurance Brokers':          0.55,
    'Financial Guarantee Ins':    0.20,
    'Finance-Invest Bnkr/Brkr':  0.35,  # 변동성 수혜 가능 vs 딜 감소
    'Invest Mgmnt/Advis Serv':   0.40,
    'Private Equity':             0.20,  # 유동성 리스크
    'Finance-Credit Card':        0.30,  # 소비자 신용 악화 우려
    'Finance-Leasing Compan':     0.25,
    'Finance-Auto Loans':         0.10,  # 관세 → 차량 가격 상승 → 연체율 우려
    'Finance-Other Services':     0.30,
    'Finance-Mtge Loan/Banker':   0.20,  # 부동산 시장 불확실
    'Finance-Consumer Loans':     0.10,
    'Venture Capital':            0.00,
    'Investment Companies':       0.15,  # BDC 등 — 레버리지 리스크

    # ══ ENERGY — 파이프라인 vs E&P 구분 ═══════════════════════════════
    'Pipelines':                  0.30,  # 규제형 중간 처리 — 유가 덜 민감, 안정적
    'Oil Comp-Integrated':        0.10,  # 유가 방향성 베팅, 단기 지정학 지지
    'Oil Comp-Explor&Prodtn':    -0.10,  # 유가 하락 시 스프레드 급확대 위험
    'Oil Refining&Marketing':    -0.05,  # 마진 변동성
    'Oil-Field Services':        -0.20,  # 가장 변동성 큼, 사이클 말단
    'Oil&Gas Drilling':          -0.25,
    'Agricultural Chemicals':     0.20,  # 식품 안보 테마, 관세 수혜 가능

    # ══ CONSUMER CYCLICAL — 세분화 핵심 ═══════════════════════════════
    # 자동차: 관세 직격탄 (25% 수입차 관세, 부품 공급망 타격)
    'Auto-Cars/Light Trucks':    -0.90,  # Ford(F), Honda(HNDA), Toyota — 관세 최대 타격
    'Auto-Med&Heavy Duty Trks':  -0.70,  # 상용차, 간접 타격
    'Auto/Trk Prts&Equip-Orig':  -0.80,  # APTV 등 부품 — 공급망 붕괴 우려
    'Retail-Automobile':         -0.60,  # 딜러 — 수요 위축
    # 소매: 관세로 매입 원가 상승 → 마진 압박
    'Retail-Discount':           -0.20,  # WMT, TGT — 가격 전가력 있으나 볼륨 리스크
    'Retail-Building Products':  -0.30,  # LOW, HD — 주택 경기 민감
    'Retail-Major Dept Store':   -0.50,  # 구조적 쇠퇴 + 관세
    'Retail-Apparel/Shoe':       -0.60,  # 중국산 의존도 높음
    'Retail-Auto Parts':         -0.40,  # AZO — 관세 원가 상승
    'Retail-Sporting Goods':     -0.40,
    'Retail-Consumer Electron':  -0.50,  # 중국 생산 의존
    'Retail-Restaurants':        -0.10,  # MCD, DRI — 내수 서비스, 관세 덜 직접
    'Retail-Gardening Prod':     -0.30,
    # 호텔/카지노/크루즈: 경기 민감 but 관세 덜 직접
    'Hotels&Motels':             -0.15,  # MAR, H — 여행 수요 관련
    'Casino Hotels':             -0.20,
    'Cruise Lines':              -0.25,  # 달러 강세, 여행 심리
    # 온라인/이커머스
    'E-Commerce/Products':       -0.20,  # AMZN은 관세 전가력 있으나 볼륨 우려
    'E-Commerce/Services':       -0.10,
    'Internet Content-Entmnt':    0.00,  # 내수 콘텐츠, 관세 무관
    # 의류
    'Apparel Manufacturers':     -0.55,  # NKE — 아시아 생산 의존
    'Athletic Footwear':         -0.55,  # NKE 동일
    # 레저
    'Recreational Vehicles':     -0.40,

    # ══ CONSUMER NON-CYCLICAL — 세분화 ════════════════════════════════
    # 식품/음료: 방어적이나 원자재 비용 + 관세 일부 영향
    'Beverages-Non-alcoholic':    0.55,  # KO, KDP — 강한 가격 전가력
    'Beverages-Wine/Spirits':     0.45,  # 관세 리스크 있으나 고급품 수요 견조
    'Brewery':                    0.45,  # 국내 생산 多
    'Food-Misc/Diversified':      0.50,  # GIS — 방어적
    'Food-Retail':                0.40,  # KR — 방어적 but 마진 압박
    'Food-Confectionery':         0.50,
    'Food-Meat Products':         0.45,
    'Food-Baking':                0.45,
    'Food-Wholesale/Distrib':     0.40,
    'Poultry':                    0.40,
    'Coffee':                     0.45,
    # 개인위생/생활용품: 가격 전가력 우수
    'Cosmetics&Toiletries':       0.55,  # PG — 가격 전가력 최강
    'Soap&Cleaning Prepar':       0.50,
    'Consumer Products-Misc':     0.40,
    # 담배: 방어적 캐리, 규제 리스크
    'Tobacco':                    0.60,  # MO — 높은 캐리, 방어적 현금흐름
    # 농업/화학
    'Agricultural Operations':    0.35,

    # ══ TECHNOLOGY ═════════════════════════════════════════════════════
    # 반도체: 관세 + 수출 통제 + 고밸류에이션 위험
    'Electronic Compo-Semicon':  -0.30,  # 중국 수출 통제, 재고 사이클
    'Semicon Compo-Intg Circu':  -0.25,
    'Semiconductor Equipment':   -0.35,  # ASML 등 수출 통제 직격
    # 소프트웨어: 금리 민감 but 관세 무관, 경기 방어적
    'Enterprise Software/Serv':   0.10,  # 구독 모델, 관세 무관
    'Applications Software':      0.05,
    'Computer Services':          0.15,
    'Data Processing/Mgmt':       0.20,  # Visa/MA 류 — 안정적
    'Computer Aided Design':      0.00,
    'Software Tools':             0.00,
    'E-Marketing/Info':           0.00,
    'Decision Support Softwar':   0.05,
    # 하드웨어: 공급망 중국 의존
    'Computers':                 -0.15,  # 관세 원가 상승
    'Computers-Memory Devices':  -0.20,
    'Computers-Other':           -0.10,
    'Electronic Connectors':     -0.20,
    'Electronic Compo-Misc':     -0.15,
    'Electronic Parts Distrib':  -0.10,
    'Electronic Measur Instr':   -0.10,
    'Electronic Secur Devices':  -0.05,
    'Electronic Forms':           0.00,
    'Networking Products':       -0.10,
    'Wireless Equipment':        -0.15,
    'Telecom Eq Fiber Optics':   -0.10,
    'Telecommunication Equip':   -0.10,
    'Industrial Automat/Robot':  -0.10,  # 관세 but 리쇼어링 수혜 측면도
    'Instruments-Controls':      -0.05,

    # ══ COMMUNICATIONS ════════════════════════════════════════════════
    'Telephone-Integrated':       0.10,  # AT&T류 — 방어적 but 부채 많음
    'Cellular Telecom':           0.05,
    'Cable/Satellite TV':         0.00,  # 코드컷팅 구조적 압박
    'Multimedia':                 0.00,
    'Broadcast Serv/Program':     0.00,
    'Advertising Agencies':      -0.15,  # 경기 민감 광고 집행
    'Advertising Services':      -0.15,
    'Telecom Services':           0.10,
    'Web Portals/ISP':            0.00,

    # ══ INDUSTRIAL / CAPITAL GOODS ════════════════════════════════════
    'Aerospace/Defense':          0.40,  # 국방예산 증가, 지정학 수혜
    'Aerospace/Defense-Equip':    0.35,
    'Machinery-Farm':             0.20,  # 식품 안보, 농업 투자
    'Machinery-General Indust':  -0.10,  # 관세 공급망 불확실
    'Machinery-Constr&Mining':   -0.10,
    'Machinery-Pumps':           -0.05,
    'Machinery-Electric Util':    0.10,  # 전력망 투자 수혜
    'Tools-Hand Held':           -0.10,
    'Diversified Manufact Op':   -0.05,  # CMI 등 — 혼재
    'Industrial Gases':           0.30,  # 에너지 전환 인프라, 안정적
    'Chemicals-Diversified':     -0.10,  # 원자재 비용 변동
    'Chemicals-Specialty':       -0.05,
    'Coatings/Paint':            -0.10,
    'Agricultural Chemicals':     0.20,
    'Containers-Paper/Plastic':  -0.20,  # 중국산 경쟁 + 원가 압박
    'Paper&Related Products':    -0.15,
    'Commercial Serv-Finance':    0.10,
    'Commercial Services':        0.05,
    'Consulting Services':        0.10,
    'Non-Profit Charity':         0.00,
    'Distribution/Wholesale':    -0.10,
    'Office Automation&Equip':   -0.15,
    'Office Supplies&Forms':     -0.15,
    'Bldg Prod-Cement/Aggreg':   -0.10,  # 건설 경기 민감
    'Bldg Prod-Air&Heating':     -0.10,
    'Bldg Prod-Wood':            -0.15,
    'Bldg&Construct Prod-Misc':  -0.10,
    'Bldg-Residential/Commer':   -0.20,  # 금리 민감
    'Building-Maint&Service':    -0.05,
    'Shipbuilding':               0.20,  # 국방/해운 수요
    'Power Conv/Supply Equip':    0.10,

    # ══ TRANSPORTATION ════════════════════════════════════════════════
    'Transport-Rail':             0.30,  # 관세 → 물류 재편 수혜 (국내 철도)
    'Transport-Services':         0.00,  # 혼재
    'Transport-Equip&Leasng':    -0.10,  # 항공기 리스 — 여행 수요 연동
    'Transport-Truck':           -0.10,  # 연료 비용, 경기 민감
    'Transport-Marine':          -0.10,
    'Airlines':                  -0.40,  # 수요 불확실, 연료 변동성

    # ══ MATERIALS ═════════════════════════════════════════════════════
    'Steel-Producers':            0.20,  # 관세 보호 수혜 (국내 철강 25% 관세)
    'Steel Pipe&Tube':            0.15,
    'Metal-Iron':                 0.10,
    'Metal-Aluminum':             0.10,  # 관세 수혜 but 수요 우려
    'Metal-Copper':               0.00,  # 전기차/전력망 수요 vs 경기 둔화
    'Metal-Diversified':          0.00,
    'Diversified Minerals':       0.00,
    'Gold Mining':                0.30,  # 불확실성 헤지 자산
    'Metal Processors&Fabrica':   0.05,

    # ══ 기타 ══════════════════════════════════════════════════════════
    'Schools':                    0.40,  # 교육 — 방어적 내수
    'Toys':                      -0.40,  # 관세 직격 (중국 생산)
    'Entertainment Software':     0.00,  # 내수 콘텐츠, 중립
    'Engineering/R&D Services':   0.15,  # 국방/인프라 연계 가능
    'Vitamins&Nutrition Prod':    0.30,  # 헬스케어 방어적
    'Rental Auto/Equipment':     -0.10,
    'Mach Tools&Rel Products':   -0.10,
    'Motorcycle/Motor Scooter':  -0.50,  # 관세 타격
    'Chemicals-Plastics':        -0.15,
    'Miscellaneous Manufactur':  -0.10,

    # ══ REAL ESTATE (REITs) ════════════════════════════════════════════
    'REITS-Industrial':           0.10,  # 물류/이커머스 수요
    'REITS-Warehouse/Industr':    0.20,  # 리쇼어링 수혜 가능
    'REITS-Diversified':         -0.10,  # 금리 민감
    'REITS-Apartments':          -0.05,  # 임대 수요 견조 but 금리 부담
    'REITS-Shopping Centers':    -0.30,  # 소매 구조적 쇠퇴
    'REITS-Regional Malls':      -0.40,  # 동일
    'REITS-Office Property':     -0.35,  # 재택근무 구조적 수요 감소
    'REITS-Health Care':          0.30,  # 고령화, 방어적
    'REITS-Storage':              0.10,
    'REITS-Single Tenant':        0.00,
    'REITS-Hotels':              -0.20,
    'REITS-Manufactured Homes':   0.00,
    'REITS-Mortgage':            -0.20,  # 금리 민감
    'Real Estate Mgmnt/Servic':  -0.10,
}

# ─── 2. 만기(OAD) 기반 점수 ──────────────────────────────────────────────────
def maturity_score(oad):
    """
    OAD 기반 만기 포지셔닝 점수
    커브 스티프닝 환경: 재정적자 우려로 장기 프리미엄 상승
    Sweet spot: 5-10년 (OAD 4-9)
    """
    if pd.isna(oad):
        return 0.0
    oad = float(oad)
    if oad < 2:       return  0.00   # 초단기: 중립
    elif oad < 4:     return  0.30   # 단기: 금리 리스크 낮음
    elif oad < 7:     return  1.00   # 5-7년: 최선호
    elif oad < 10:    return  0.70   # 7-10년: 선호
    elif oad < 13:    return  0.00   # 10-13년: 중립
    elif oad < 16:    return -0.50   # 13-16년: 비선호
    else:             return -1.00   # 16년+: 비선호

# ─── 3. 등급 안전마진 점수 ───────────────────────────────────────────────────
RATING_BUFFER_MAP = {
    'AAA':  0.50,
    'AA1':  0.55,
    'AA2':  0.55,
    'AA3':  0.50,
    'A1':   0.70,   # A등급 sweet spot: 안전 + 타이트하지 않음
    'A2':   0.70,
    'A3':   0.65,
    'BAA1': 0.25,
    'BAA2': 0.05,
    'BAA3':-0.65,   # BBB-: Fallen Angel 경계선
    'BA1': -0.85,
    'BA2': -1.00,
    'BA3': -1.00,
    'B1':  -1.00,
    'B2':  -1.00,
    'B3':  -1.00,
}

# ─── 4. 점수 산출 ─────────────────────────────────────────────────────────────
def compute_ai_macro_score(df):
    df = df.copy()

    # Subgroup Score (BCLASS4 또는 Industry Subgroup)
    subgroup_col = 'Industry Subgroup' if 'Industry Subgroup' in df.columns else 'BCLASS3'
    df['AI_Sector_Score'] = df[subgroup_col].map(SUBGROUP_SCORE_MAP)
    # 매핑 안 된 값은 0 (중립)
    unmapped = df['AI_Sector_Score'].isna().sum()
    if unmapped > 0:
        print(f"    Unmapped subgroups ({unmapped} bonds): "
              f"{df.loc[df['AI_Sector_Score'].isna(), subgroup_col].value_counts().head(5).to_dict()}")
    df['AI_Sector_Score'] = df['AI_Sector_Score'].fillna(0.0)

    # Maturity Score
    df['AI_Maturity_Score'] = df['OAD'].apply(maturity_score)

    # Rating Buffer Score
    df['AI_RatingBuf_Score'] = df['DPFundamentalRating'].map(RATING_BUFFER_MAP)
    fallback_mask = df['AI_RatingBuf_Score'].isna()
    df.loc[fallback_mask, 'AI_RatingBuf_Score'] = (
        df.loc[fallback_mask, 'Issuer Rtg'].map(RATING_BUFFER_MAP)
    )
    df['AI_RatingBuf_Score'] = df['AI_RatingBuf_Score'].fillna(0.0)

    # Final AI_Macro_Score
    df['AI_Macro_Score'] = (
        df['AI_Sector_Score']    * 0.40 +
        df['AI_Maturity_Score']  * 0.35 +
        df['AI_RatingBuf_Score'] * 0.25
    ).clip(-1.0, 1.0)

    n = df['AI_Macro_Score'].notna().sum()
    print(f"  AI_Macro_Score: non-null={n}, "
          f"range=[{df['AI_Macro_Score'].min():.3f}, {df['AI_Macro_Score'].max():.3f}]")
    print(f"    Sector  [{df['AI_Sector_Score'].min():.2f}, {df['AI_Sector_Score'].max():.2f}]  "
          f"Maturity [{df['AI_Maturity_Score'].min():.2f}, {df['AI_Maturity_Score'].max():.2f}]  "
          f"RatingBuf [{df['AI_RatingBuf_Score'].min():.2f}, {df['AI_RatingBuf_Score'].max():.2f}]")

    return df
