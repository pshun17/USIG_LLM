"""
ai_macro_score.py
─────────────────
현재 매크로 환경(2026년 6월) 기반 정성적 AI 점수 산출

[배경 논리 — 4월 대비 주요 변화]
① 미중 무역 휴전 (5월 제네바 합의)
   - 미국 대중 관세 145% → 30%, 중국 대미 관세 125% → 10% (90일 한시)
   - 자동차 부품·전자·의류 등 관세 직격 섹터 공급망 압박 대폭 완화
   - 단, 완성차 25% 관세는 유지 → 자동차 완전 회복은 아님

② Fed 동결 유지 + 재정적자 우려 지속
   - 인하 기대 후퇴 (시장: 2026년 1~2회 → 1회 이하)
   - 커브 스티프닝 테마 지속, 10년+ 장기물 불리 유효
   - 5~10년 구간 여전히 Sweet Spot

③ IG 스프레드: 4월 급확대 후 재타이트닝
   - OAS 4월 고점 대비 30~40bp 축소, 역사적 하위 25% 수준으로 복귀
   - 캐리보다 선별적 섹터 알파 전략 중요

④ 달러 약세 전환 (DXY -6% from April peak)
   - 비미국 발행사(유럽·아시아 은행) 헤지비용 감소 → 상대적 우위

⑤ 유가 하락 (WTI $75 → $65 구간)
   - E&P, 오일서비스 스프레드 부담
   - 파이프라인(수수료 구조) 상대적 방어

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
    'Electric-Integrated':        0.85,  # 규제 수익·관세 무관, 방어적 캐리 최상위
    'Electric-Distribution':      0.85,  # 동일 — 타이트 스프레드 환경서도 상대 우위
    'Electric-Transmission':      0.80,  # 전력망 인프라 투자 수혜 지속
    'Electric-Generation':        0.75,  # 연료 비용 변동 있으나 방어적
    'Gas-Distribution':           0.80,  # 규제 유틸리티, 안정적 CF
    'Water':                      0.85,  # 가장 방어적, 경기·관세 완전 무관
    'Non-hazardous Waste Disp':   0.70,  # 방어적 서비스, 규제 수익

    # ══ HEALTHCARE ═════════════════════════════════════════════════════
    'Medical-Drugs':              0.70,  # 방어적, 원료 관세 부담 있으나 가격 전가 가능
    'Medical-Hospitals':          0.75,  # 내수 서비스, 관세 무관, 스프레드 안정
    'Medical-HMO':                0.60,  # 메디케어 요율 협상 불확실 → 소폭 하향
    'Medical-Biomedical/Gene':    0.55,  # 금리 고착화로 밸류에이션 부담 지속
    'Medical Products':           0.60,  # 중국산 부품 관세 완화 → 소폭 개선
    'Medical Instruments':        0.55,
    'Medical Labs&Testing Srv':   0.65,  # 방어적 내수 서비스
    'Pharmacy Services':          0.60,
    'Medical-Whsle Drug Dist':    0.55,
    'Medical Imaging Systems':    0.55,
    'Diagnostic Equipment':       0.55,
    'Drug Delivery Systems':      0.55,
    'Medical-Generic Drugs':      0.50,
    'Phys Practice Mgmnt':        0.60,
    'Medical-Outptnt/Home Med':   0.60,

    # ══ BANKING / FINANCIAL ════════════════════════════════════════════
    'Diversified Banking Inst':   0.60,  # 커브 스티프닝 NIM 수혜, 자본비율 견조
    'Super-Regional Banks-US':    0.65,  # 미국 내수 경기 상대적 견조 → 소폭 상향
    'Commer Banks-Eastern US':    0.55,
    'Commer Banks-Southern US':   0.55,
    'Commer Banks-Central US':    0.50,
    'Commer Banks-Western US':    0.50,
    'Commer Banks Non-US':        0.45,  # 달러 약세 전환 → 헤지비용 감소, 상향
    'Money Center Banks':         0.50,  # 규제 불확실성 완화 기대 + 딜 활성화 기대
    'Fiduciary Banks':            0.65,  # NTRS, STT — 수탁·금리 이중 수혜
    'Life/Health Insurance':      0.50,  # 투자수익 개선 vs 언더라이팅 비용
    'Property/Casualty Ins':      0.55,  # 보험료 인상 사이클 지속
    'Reinsurance':                0.50,
    'Multi-line Insurance':       0.45,
    'Insurance Brokers':          0.55,
    'Financial Guarantee Ins':    0.20,
    'Finance-Invest Bnkr/Brkr':  0.40,  # 무역 정상화 → M&A/IPO 회복 기대
    'Invest Mgmnt/Advis Serv':   0.40,
    'Private Equity':             0.25,  # 스프레드 타이트닝 → 출구 여건 개선
    'Finance-Credit Card':        0.25,  # 소비자 신용 스트레스 지속, 중립 하향
    'Finance-Leasing Compan':     0.25,
    'Finance-Auto Loans':         0.20,  # 미중 관세 완화 → 차량가격 안정 기대, 상향
    'Finance-Other Services':     0.30,
    'Finance-Mtge Loan/Banker':   0.20,  # 금리 고착화로 부동산 부담 지속
    'Finance-Consumer Loans':     0.15,
    'Venture Capital':            0.00,
    'Investment Companies':       0.15,

    # ══ ENERGY ═════════════════════════════════════════════════════════
    # 유가 WTI $65 수준으로 하락 → E&P·서비스 스프레드 부담 증가
    'Pipelines':                  0.35,  # 수수료 구조, 유가 무관 — 소폭 상향
    'Oil Comp-Integrated':        0.00,  # 유가 하락 중립화 → 하향
    'Oil Comp-Explor&Prodtn':    -0.25,  # 유가 $65 → 잉여현금흐름 축소, 하향
    'Oil Refining&Marketing':    -0.15,  # 크랙스프레드 마진 압박
    'Oil-Field Services':        -0.35,  # 유가 하락 → Capex 축소 우려, 하향
    'Oil&Gas Drilling':          -0.40,  # 동일 논리
    'Agricultural Chemicals':     0.20,  # 식품 안보 테마 유지

    # ══ CONSUMER CYCLICAL ══════════════════════════════════════════════
    # 미중 관세 90% → 30%로 대폭 인하 → 공급망 압박 완화
    # 단, 완성차 25% 관세 유지 → 자동차 완전 회복은 아님
    'Auto-Cars/Light Trucks':    -0.45,  # 부품 공급망 완화 but 완성차 관세 잔존, 대폭 상향
    'Auto-Med&Heavy Duty Trks':  -0.30,  # 상용차 공급망 개선
    'Auto/Trk Prts&Equip-Orig':  -0.25,  # 중국산 부품 관세 30%로 정상화, 대폭 상향
    'Retail-Automobile':         -0.20,  # 차량 공급 개선 → 딜러 재고 정상화
    'Retail-Discount':           -0.10,  # 수입 원가 하락 → 마진 부담 완화
    'Retail-Building Products':  -0.25,  # 주택 경기 민감도 잔존
    'Retail-Major Dept Store':   -0.35,  # 구조적 쇠퇴, 관세 완화로 소폭 개선
    'Retail-Apparel/Shoe':       -0.20,  # 중국산 소싱 비용 급락, 대폭 상향
    'Retail-Auto Parts':         -0.15,  # 관세 완화 수혜
    'Retail-Sporting Goods':     -0.20,  # 동일
    'Retail-Consumer Electron':  -0.10,  # 중국산 전자제품 관세 완화 대폭 수혜
    'Retail-Restaurants':        -0.05,  # 내수 서비스, 관세 거의 무관
    'Retail-Gardening Prod':     -0.20,
    'Hotels&Motels':             -0.10,  # 달러 약세 → 인바운드 관광 개선
    'Casino Hotels':             -0.15,
    'Cruise Lines':              -0.15,  # 달러 약세 긍정적, 소폭 상향
    'E-Commerce/Products':       -0.05,  # 관세 완화 → 상품 원가 정상화
    'E-Commerce/Services':       -0.05,
    'Internet Content-Entmnt':    0.05,  # 내수 콘텐츠 견조, 소폭 긍정
    'Apparel Manufacturers':     -0.10,  # NKE 등 아시아 소싱 비용 대폭 완화
    'Athletic Footwear':         -0.10,  # 동일
    'Recreational Vehicles':     -0.25,  # 경기 민감, 금리 부담 잔존

    # ══ CONSUMER NON-CYCLICAL ══════════════════════════════════════════
    'Beverages-Non-alcoholic':    0.60,  # KO, KDP — 가격 전가력 + 관세 완화
    'Beverages-Wine/Spirits':     0.50,  # 무역 정상화 → 수입 주류 부담 완화
    'Brewery':                    0.50,  # 국내 생산 多, 방어적
    'Food-Misc/Diversified':      0.55,  # GIS — 방어적, 원자재 부담 완화
    'Food-Retail':                0.45,  # KR — 방어적
    'Food-Confectionery':         0.50,
    'Food-Meat Products':         0.45,
    'Food-Baking':                0.45,
    'Food-Wholesale/Distrib':     0.40,
    'Poultry':                    0.40,
    'Coffee':                     0.45,
    'Cosmetics&Toiletries':       0.60,  # PG — 가격 전가력 + 관세 완화로 원가 개선
    'Soap&Cleaning Prepar':       0.50,
    'Consumer Products-Misc':     0.40,
    'Tobacco':                    0.60,  # MO — 방어적 CF, 캐리 매력
    'Agricultural Operations':    0.35,

    # ══ TECHNOLOGY ═════════════════════════════════════════════════════
    # 미중 관세 대폭 완화 → 하드웨어·반도체 공급망 정상화
    # 단, 첨단 반도체 수출 통제는 유지 → 장비주 제한적 회복
    'Electronic Compo-Semicon':   0.05,  # 관세 완화, 재고 사이클 바닥 확인, 대폭 상향
    'Semicon Compo-Intg Circu':   0.05,  # 동일
    'Semiconductor Equipment':   -0.10,  # 수출 통제 유지로 완전 회복 아님
    'Enterprise Software/Serv':   0.20,  # 구독 모델, 관세 무관, AI 수요 견조
    'Applications Software':      0.15,  # AI 통합 모멘텀
    'Computer Services':          0.20,
    'Data Processing/Mgmt':       0.25,  # Visa/MA — 거래 회복 기대
    'Computer Aided Design':      0.10,
    'Software Tools':             0.10,
    'E-Marketing/Info':           0.05,
    'Decision Support Softwar':   0.10,
    'Computers':                  0.05,  # 관세 완화 수혜, 상향
    'Computers-Memory Devices':  -0.05,  # 공급과잉 우려 잔존
    'Computers-Other':            0.00,
    'Electronic Connectors':      0.00,  # 관세 완화로 중립
    'Electronic Compo-Misc':      0.00,
    'Electronic Parts Distrib':   0.00,
    'Electronic Measur Instr':   -0.05,
    'Electronic Secur Devices':   0.00,
    'Electronic Forms':           0.00,
    'Networking Products':        0.00,
    'Wireless Equipment':        -0.05,
    'Telecom Eq Fiber Optics':   -0.05,
    'Telecommunication Equip':   -0.05,
    'Industrial Automat/Robot':   0.00,  # 리쇼어링 수혜 + 관세 완화 균형
    'Instruments-Controls':       0.00,

    # ══ COMMUNICATIONS ════════════════════════════════════════════════
    'Telephone-Integrated':       0.15,  # AT&T — 부채 관리 진전, 소폭 상향
    'Cellular Telecom':           0.10,
    'Cable/Satellite TV':         0.00,  # 코드컷팅 구조적 압박 지속
    'Multimedia':                 0.00,
    'Broadcast Serv/Program':     0.00,
    'Advertising Agencies':      -0.05,  # 경기 우려 완화로 광고비 소폭 회복
    'Advertising Services':      -0.05,
    'Telecom Services':           0.15,
    'Web Portals/ISP':            0.05,

    # ══ INDUSTRIAL / CAPITAL GOODS ════════════════════════════════════
    'Aerospace/Defense':          0.50,  # NATO 지출 증가 지속, 백로그 사상 최대
    'Aerospace/Defense-Equip':    0.45,  # 동일
    'Machinery-Farm':             0.20,  # 농업 투자 유지
    'Machinery-General Indust':   0.00,  # 관세 완화로 공급망 부담 경감, 상향
    'Machinery-Constr&Mining':   -0.05,
    'Machinery-Pumps':            0.00,
    'Machinery-Electric Util':    0.20,  # 전력망·AI 데이터센터 투자 수혜
    'Tools-Hand Held':           -0.05,
    'Diversified Manufact Op':    0.00,  # 관세 완화로 중립
    'Industrial Gases':           0.35,  # 에너지 전환·수소 인프라 수혜
    'Chemicals-Diversified':     -0.05,  # 원자재 비용 안정화
    'Chemicals-Specialty':        0.00,
    'Coatings/Paint':            -0.05,
    'Agricultural Chemicals':     0.20,
    'Containers-Paper/Plastic':  -0.10,  # 관세 완화로 부담 일부 경감
    'Paper&Related Products':    -0.10,
    'Commercial Serv-Finance':    0.15,
    'Commercial Services':        0.10,
    'Consulting Services':        0.15,
    'Non-Profit Charity':         0.00,
    'Distribution/Wholesale':    -0.05,  # 관세 완화 수혜
    'Office Automation&Equip':   -0.05,
    'Office Supplies&Forms':     -0.10,
    'Bldg Prod-Cement/Aggreg':   -0.10,
    'Bldg Prod-Air&Heating':     -0.05,
    'Bldg Prod-Wood':            -0.10,
    'Bldg&Construct Prod-Misc':  -0.05,
    'Bldg-Residential/Commer':   -0.15,  # 금리 고착화로 부동산 부담 지속
    'Building-Maint&Service':    -0.05,
    'Shipbuilding':               0.25,  # 국방·해운 발주 증가
    'Power Conv/Supply Equip':    0.20,  # 데이터센터 전력 수요 폭증 수혜

    # ══ TRANSPORTATION ════════════════════════════════════════════════
    'Transport-Rail':             0.30,  # 국내 물류 재편 수혜 유지
    'Transport-Services':         0.05,  # 무역 회복 기대
    'Transport-Equip&Leasng':    -0.05,  # 여행 수요 회복 부분 상쇄
    'Transport-Truck':           -0.05,  # 유가 하락 연료비 절감 → 소폭 개선
    'Transport-Marine':          -0.05,  # 무역 회복 기대 vs 운임 하락
    'Airlines':                  -0.25,  # 달러 약세·유가 하락 긍정, 수요 회복 기대

    # ══ MATERIALS ═════════════════════════════════════════════════════
    'Steel-Producers':            0.15,  # 관세 보호 유지 but 수요 우려로 소폭 하향
    'Steel Pipe&Tube':            0.15,
    'Metal-Iron':                 0.10,
    'Metal-Aluminum':             0.10,
    'Metal-Copper':               0.10,  # 전력망·AI 인프라 수요 기대 상향
    'Metal-Diversified':          0.05,
    'Diversified Minerals':       0.05,
    'Gold Mining':                0.25,  # 불확실성 헤지 수요 일부 감소 → 소폭 하향
    'Metal Processors&Fabrica':   0.05,

    # ══ 기타 ══════════════════════════════════════════════════════════
    'Schools':                    0.40,  # 방어적 내수, 변동 없음
    'Toys':                      -0.10,  # 관세 대폭 완화 → 중국산 압박 경감, 대폭 상향
    'Entertainment Software':     0.10,  # 내수 콘텐츠, 소폭 긍정
    'Engineering/R&D Services':   0.20,  # 국방·AI 인프라 연계 상향
    'Vitamins&Nutrition Prod':    0.30,
    'Rental Auto/Equipment':     -0.05,
    'Mach Tools&Rel Products':   -0.05,
    'Motorcycle/Motor Scooter':  -0.20,  # 관세 완화로 대폭 상향
    'Chemicals-Plastics':        -0.10,
    'Miscellaneous Manufactur':  -0.05,

    # ══ REAL ESTATE (REITs) ════════════════════════════════════════════
    'REITS-Industrial':           0.20,  # 리쇼어링·이커머스 물류 수요 상향
    'REITS-Warehouse/Industr':    0.25,  # 동일, 데이터센터 수요 추가
    'REITS-Diversified':         -0.10,  # 금리 고착화
    'REITS-Apartments':          -0.05,  # 임대 수요 견조
    'REITS-Shopping Centers':    -0.25,  # 소매 구조적 쇠퇴, 관세 완화로 소폭 개선
    'REITS-Regional Malls':      -0.35,  # 구조적 쇠퇴 지속
    'REITS-Office Property':     -0.30,  # 재택근무 정착, 일부 AI사 오피스 수요는 긍정
    'REITS-Health Care':          0.35,  # 고령화 테마 강화, 상향
    'REITS-Storage':              0.10,
    'REITS-Single Tenant':        0.05,
    'REITS-Hotels':              -0.10,  # 달러 약세 인바운드 기대
    'REITS-Manufactured Homes':   0.05,
    'REITS-Mortgage':            -0.20,  # 금리 고착화
    'Real Estate Mgmnt/Servic':  -0.10,
}

# ─── 2. 만기(OAD) 기반 점수 ──────────────────────────────────────────────────
def maturity_score(oad):
    """
    OAD 기반 만기 포지셔닝 점수 (2026년 6월 업데이트)

    4월 대비 변화:
    - One Big Beautiful Bill 하원 통과 → 향후 10년 재정적자 +$3~4T
    - 무디스 미국 신용등급 강등 (Aaa → Aa1) → 장기 프리미엄 재부각
    - 30년물 5.2~5.3% 고압 유지 → 커브 스티프닝 심화
    - 10~13년 구간: 중립 → 소폭 비선호로 하향
    - Sweet spot 4~7년 유지, 7~10년 선호 유지

    [OAD 구간별]
    <2    : 0.00  초단기, 중립
    2~4   : 0.35  단기, 금리 리스크 낮음 (소폭 상향 — Fed anchor)
    4~7   : 1.00  최선호 (불변)
    7~10  : 0.65  선호 (소폭 하향 — 장기 프리미엄 영향 시작)
    10~13 : -0.20  비선호 (4월 중립 0.00 → 하향 — 재정 압박)
    13~16 : -0.65  비선호 강화 (4월 -0.50 → 하향)
    16+   : -1.00  최대 비선호 (불변)
    """
    if pd.isna(oad):
        return 0.0
    oad = float(oad)
    if oad < 2:       return  0.00
    elif oad < 4:     return  0.35
    elif oad < 7:     return  1.00
    elif oad < 10:    return  0.65
    elif oad < 13:    return -0.20
    elif oad < 16:    return -0.65
    else:             return -1.00

# ─── 3. 등급 안전마진 점수 ───────────────────────────────────────────────────
RATING_BUFFER_MAP = {
    # 2026년 6월 업데이트
    # IG 스프레드 재타이트닝 (역사적 하위 25%):
    #   → 등급 간 스프레드 차이 축소 → 하위 등급의 리스크 대비 보상 감소
    #   → 90일 관세 휴전 종료 시나리오 재부각되면 BBB 하단 낙폭 확대 위험
    # 무디스 강등 이후 AA·A 등급의 상대적 안전 프리미엄 재부각
    'AAA':  0.40,   # 가장 타이트 → 캐리 매력 희박
    'AA1':  0.55,
    'AA2':  0.55,
    'AA3':  0.50,
    'A1':   0.70,   # sweet spot: 캐리 + 안전마진 + 무디스 강등 이후 상대 매력
    'A2':   0.70,
    'A3':   0.60,   # A3: BBB와의 격차 인식, 소폭 하향
    'BAA1': 0.15,   # 스프레드 좁아진 상태에서 버퍼 부족
    'BAA2':-0.15,   # 타이트 구간서 대칭적 위험 부각
    'BAA3':-0.80,   # BBB-: 스프레드 타이트 + 90일 협상 리스크 → Fallen Angel 위험 최고조
    'BA1': -0.90,
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
