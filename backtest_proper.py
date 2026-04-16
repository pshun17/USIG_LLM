"""
backtest_proper.py
═══════════════════════════════════════════════════════════════════════════════
Bloomberg US IG Corporate Bond — 진짜 월별 리밸런싱 백테스트

방법론:
  - 데이터: FRED (ICE BofA US Corporate Bond Sub-Indices, 일간 → 월말)
  - 유니버스: 9개 버킷 (4 등급 + 5 만기)
      Rating : AAA / AA / A / BBB
      Maturity: 1-3Y / 3-5Y / 5-7Y / 7-10Y / 10Y+
  - 스코어링 (우리 모델과 동일 로직, 매달 재산출):
      1. Carry_Score   : 현재 YTW의 trailing 24M z-score (높을수록 ↑)
      2. OAS_Score     : 현재 OAS의 trailing 24M z-score (넓을수록 저평가 ↑)
      3. Momentum_Score: OAS 3개월 변화 방향 (타이트닝 = 압축 진행 중 ↑)
      Composite = Carry×0.40 + OAS_Level×0.40 + Momentum×0.20
  - 포트폴리오:
      Top3  : 매달 상위 3 버킷 선택 → 등가중
      Top5  : 매달 상위 5 버킷 선택 → 등가중
      Score-Wt: 스코어 비례 가중
  - 벤치마크: ICE BofA US Corporate Index Total Return (= LUACTRUU 동일 방법론)
  - 리밸런싱: 월말 기준, 다음 달 초 적용 (신호→1개월 딜레이 적용)

⚠ 편향 최소화:
  - Survivorship Bias: 없음 (인덱스 레벨 사용 → 구성종목 변동 반영)
  - Look-ahead Bias: 없음 (당월 말 스코어 → 익월 수익률)
  - Data-snooping: 스코어 공식은 현재 모델 기준 사전 정의 (직접 fitting 없음)
═══════════════════════════════════════════════════════════════════════════════
"""

import warnings, os, ssl, io
import numpy as np
import pandas as pd
import requests
import urllib3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.gridspec as gridspec

warnings.filterwarnings('ignore')
urllib3.disable_warnings()

ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''

SESSION = requests.Session()
SESSION.verify = False

OUT = 'C:/Users/sh.park/Documents/USIG_LLM/backtest_proper.png'

# ═══════════════════════════════════════════════════════════════════════════════
# 1. FRED 시리즈 정의
# ═══════════════════════════════════════════════════════════════════════════════

# 총수익률 인덱스 (TR Index Value → 수익률 계산용)
TR_SERIES = {
    # 벤치마크
    'BENCH'    : 'BAMLCC0A0CMTRIV',   # ICE BofA US Corp All (LUACTRUU 동일)
    # 등급별
    'AAA'      : 'BAMLCC0A1AAATRIV',
    'AA'       : 'BAMLCC0A2CATRIV',
    'A'        : 'BAMLCC0A3CATRIV',
    'BBB'      : 'BAMLCC0A4CBBBTRIV',
    # 만기별
    '1_3Y'     : 'BAMLCC1A013YTRIV',
    '3_5Y'     : 'BAMLCC2A035YTRIV',
    '5_7Y'     : 'BAMLCC3A057YTRIV',
    '7_10Y'    : 'BAMLCC4A0710YTRIV',
    '10Y_PLUS' : 'BAMLCC8A015PYTRIV',
}

# OAS (Option-Adjusted Spread, bp)
OAS_SERIES = {
    'AAA'      : 'BAMLC0A1CAAA',
    'AA'       : 'BAMLC0A2CA',
    'A'        : 'BAMLC0A3CA',
    'BBB'      : 'BAMLC0A4CBBB',
    '1_3Y'     : 'BAMLC1A0C13Y',
    '3_5Y'     : 'BAMLC2A0C35Y',
    '5_7Y'     : 'BAMLC3A0C57Y',
    '7_10Y'    : 'BAMLC4A0C710Y',
    '10Y_PLUS' : 'BAMLC8A0C15PY',
}

# YTW (Yield to Worst, %)
YTW_SERIES = {
    'AAA'      : 'BAMLC0A1CAAAY',
    'AA'       : 'BAMLC0A2CAY',
    'A'        : 'BAMLC0A3CAY',
    'BBB'      : 'BAMLC0A4CBBBY',
    '1_3Y'     : 'BAMLC1A0C13YEY',
    '3_5Y'     : 'BAMLC2A0C35YEY',
    '5_7Y'     : 'BAMLC3A0C57YEY',
    '7_10Y'    : 'BAMLC4A0C710YEY',
    '10Y_PLUS' : 'BAMLC8A0C15PYEY',
}

BUCKETS = ['AAA', 'AA', 'A', 'BBB', '1_3Y', '3_5Y', '5_7Y', '7_10Y', '10Y_PLUS']
BUCKET_LABELS = {
    'AAA': 'AAA 등급', 'AA': 'AA 등급', 'A': 'A 등급', 'BBB': 'BBB 등급',
    '1_3Y': '만기 1-3Y', '3_5Y': '만기 3-5Y', '5_7Y': '만기 5-7Y',
    '7_10Y': '만기 7-10Y', '10Y_PLUS': '만기 10Y+'
}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. FRED 데이터 다운로드
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_fred(series_id, start='2012-01-01'):
    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        # FRED CSV: 첫 컬럼명이 'observation_date'
        df = pd.read_csv(io.StringIO(r.text))
        date_col = df.columns[0]
        val_col  = df.columns[1]
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col]).set_index(date_col)
        df = df.rename(columns={val_col: series_id})
        df = df.replace('.', np.nan)
        df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
        return df.loc[start:]
    except Exception as e:
        print(f"    WARN {series_id} failed: {e}")
        return None

print("=" * 65)
print("Downloading FRED data (ICE BofA Corp Bond Sub-Indices)...")
print("  This may take ~30 seconds...")

tr_data  = {}
oas_data = {}
ytw_data = {}

# 벤치마크
bench_raw = fetch_fred(TR_SERIES['BENCH'])
if bench_raw is not None:
    print(f"  [OK] Benchmark (LUACTRUU equivalent): {len(bench_raw)} obs")

# 버킷별
for bkt in BUCKETS:
    t = fetch_fred(TR_SERIES[bkt])
    o = fetch_fred(OAS_SERIES[bkt])
    y = fetch_fred(YTW_SERIES[bkt])
    if t is not None: tr_data[bkt] = t
    if o is not None: oas_data[bkt] = o
    if y is not None: ytw_data[bkt] = y
    status = 'OK' if (t is not None and o is not None and y is not None) else 'WARN'
    print(f"  [{status}] {BUCKET_LABELS[bkt]:<12}: TR={t is not None} OAS={o is not None} YTW={y is not None}")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. 월말 기준 리샘플링
# ═══════════════════════════════════════════════════════════════════════════════

def to_monthly_last(raw_dict, buckets):
    frames = {}
    for bkt in buckets:
        if bkt in raw_dict:
            s = raw_dict[bkt].iloc[:, 0].dropna()
            frames[bkt] = s.resample('M').last()
    return pd.DataFrame(frames)

tr_m   = to_monthly_last(tr_data,  BUCKETS)
oas_m  = to_monthly_last(oas_data, BUCKETS)
ytw_m  = to_monthly_last(ytw_data, BUCKETS)

bench_m = bench_raw.iloc[:, 0].dropna().resample('M').last()

# 월간 총수익률 계산
tr_ret_m  = tr_m.pct_change()
bench_ret = bench_m.pct_change()

valid_buckets = [b for b in BUCKETS
                 if b in tr_m.columns and b in oas_m.columns and b in ytw_m.columns]
print(f"\n  Valid buckets for backtest: {len(valid_buckets)} / {len(BUCKETS)}")
print(f"  Buckets: {valid_buckets}")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. 월별 스코어링 함수
# ═══════════════════════════════════════════════════════════════════════════════

LOOKBACK = 24   # z-score 산출 윈도우 (개월)
MOM_WIN  = 3    # OAS 모멘텀 윈도우 (개월)

def zscore_at_t(series, t, window=LOOKBACK):
    """t 시점 기준 trailing window z-score"""
    hist = series.iloc[:t].dropna().tail(window)
    if len(hist) < 6:
        return np.nan
    return (series.iloc[t] - hist.mean()) / (hist.std() + 1e-9)

def score_buckets_at(t_idx, oas_df, ytw_df, valid):
    """
    t_idx: 정수 인덱스 (월말)
    반환: {버킷명: composite_score}
    """
    scores = {}
    for bkt in valid:
        try:
            # 1) Carry Score: YTW z-score (높을수록 좋음)
            ytw_ser = ytw_df[bkt]
            carry_z = zscore_at_t(ytw_ser, t_idx)

            # 2) OAS Level Score: OAS z-score (높을수록 저평가 = 좋음)
            oas_ser = oas_df[bkt]
            oas_z   = zscore_at_t(oas_ser, t_idx)

            # 3) OAS Momentum Score: OAS 하락(압축) = 좋음 → 부호 반전
            if t_idx >= MOM_WIN:
                oas_now  = oas_ser.iloc[t_idx]
                oas_prev = oas_ser.iloc[t_idx - MOM_WIN]
                mom_raw  = oas_prev - oas_now   # 압축이면 양수
                # 전체 hist std로 정규화
                hist_std = oas_ser.dropna().iloc[:t_idx].std()
                mom_z    = mom_raw / (hist_std + 1e-9)
            else:
                mom_z = 0.0

            # NaN 처리
            carry_z = 0.0 if np.isnan(carry_z) else carry_z
            oas_z   = 0.0 if np.isnan(oas_z)   else oas_z
            mom_z   = 0.0 if np.isnan(mom_z)   else mom_z

            # Composite (우리 모델 가중치 구조 반영)
            composite = carry_z * 0.40 + oas_z * 0.40 + mom_z * 0.20
            scores[bkt] = composite

        except Exception:
            scores[bkt] = np.nan

    return scores

# ═══════════════════════════════════════════════════════════════════════════════
# 5. 월별 리밸런싱 백테스팅
# ═══════════════════════════════════════════════════════════════════════════════

# 공통 월 인덱스
common_idx = (tr_ret_m[valid_buckets].dropna(how='all').index
              .intersection(bench_ret.dropna().index)
              .intersection(oas_m[valid_buckets].dropna(how='all').index)
              .intersection(ytw_m[valid_buckets].dropna(how='all').index))

# LOOKBACK 기간 이후부터 (2014년 이후 실질 백테스팅)
START_DATE = '2014-01-01'
common_idx = common_idx[common_idx >= START_DATE]

print(f"\nBacktest period: {common_idx[0].strftime('%Y-%m')} ~ {common_idx[-1].strftime('%Y-%m')}")
print(f"  Monthly observations: {len(common_idx)}")

# 스코어 시리즈 인덱스 (스코어링 시점 = t-1, 수익률 측정 = t)
oas_full = oas_m[valid_buckets]
ytw_full = ytw_m[valid_buckets]
ret_full = tr_ret_m[valid_buckets]

# 포트폴리오별 월별 수익률 저장
port_rets = {
    'Top3':    [],
    'Top5':    [],
    'Score_Wt': [],
    'Equal_All': [],  # 전 버킷 등가중 (벤치마크 내 분산)
}
port_dates    = []
monthly_picks = []   # 매달 선택 버킷 기록

for dt in common_idx:
    # 스코어링: 전월 말 데이터 기준
    score_date_idx_in_full = oas_full.index.get_loc(dt) - 1
    if score_date_idx_in_full < LOOKBACK:
        continue

    scores = score_buckets_at(score_date_idx_in_full, oas_full, ytw_full, valid_buckets)
    scores_clean = {k: v for k, v in scores.items() if not np.isnan(v)}
    if len(scores_clean) < 3:
        continue

    sorted_bkts = sorted(scores_clean.items(), key=lambda x: x[1], reverse=True)
    all_bkts    = [b for b, _ in sorted_bkts]

    # 당월 수익률
    ret_row = ret_full.loc[dt]

    # Top3
    top3 = all_bkts[:3]
    ret_top3 = ret_row[top3].mean()

    # Top5 (버킷이 5개 이하면 모두)
    top5 = all_bkts[:min(5, len(all_bkts))]
    ret_top5 = ret_row[top5].mean()

    # Score-weighted (softmax 방식)
    scores_arr = np.array([scores_clean[b] for b in all_bkts])
    weights_sw = np.exp(scores_arr) / np.exp(scores_arr).sum()
    ret_sw = sum(w * ret_row[b] for b, w in zip(all_bkts, weights_sw))

    # Equal All
    ret_eq = ret_row[all_bkts].mean()

    port_rets['Top3'].append(ret_top3)
    port_rets['Top5'].append(ret_top5)
    port_rets['Score_Wt'].append(float(ret_sw))
    port_rets['Equal_All'].append(ret_eq)
    port_dates.append(dt)

    monthly_picks.append({
        'date': dt,
        'top3': top3,
        'top_score': sorted_bkts[0][1],
        'scores': scores_clean
    })

port_dates = pd.DatetimeIndex(port_dates)
bench_aligned = bench_ret.loc[port_dates]

port_df = pd.DataFrame(port_rets, index=port_dates)

print(f"\n  Simulation months: {len(port_dates)}")
print(f"  Period: {port_dates[0].strftime('%Y-%m')} ~ {port_dates[-1].strftime('%Y-%m')}")

# ═══════════════════════════════════════════════════════════════════════════════
# 6. 성과 통계
# ═══════════════════════════════════════════════════════════════════════════════

def cum_wealth(ret_series, start=100):
    return start * (1 + ret_series).cumprod()

def perf_stats(ret, label):
    n       = len(ret)
    ann_ret = (1 + ret.mean()) ** 12 - 1
    ann_vol = ret.std() * np.sqrt(12)
    sharpe  = ann_ret / ann_vol if ann_vol > 0 else 0
    cw      = cum_wealth(ret)
    max_dd  = (cw / cw.cummax() - 1).min()
    total   = (1 + ret).prod() - 1
    up_cap  = ret[ret > 0].mean() / bench_aligned[bench_aligned > 0].mean() if bench_aligned[bench_aligned > 0].mean() != 0 else np.nan
    dn_cap  = ret[ret < 0].mean() / bench_aligned[bench_aligned < 0].mean() if bench_aligned[bench_aligned < 0].mean() != 0 else np.nan
    return {
        'Portfolio':  label,
        'Total':      f'{total:.1%}',
        'Ann. Ret':   f'{ann_ret:.2%}',
        'Ann. Vol':   f'{ann_vol:.2%}',
        'Sharpe':     f'{sharpe:.2f}',
        'Max DD':     f'{max_dd:.2%}',
        'Up Capture': f'{up_cap:.0%}' if not np.isnan(up_cap) else 'N/A',
        'Dn Capture': f'{dn_cap:.0%}' if not np.isnan(dn_cap) else 'N/A',
    }

stats_list = [perf_stats(bench_aligned, 'LUACTRUU (벤치마크)')]
for pname in ['Top3', 'Top5', 'Score_Wt', 'Equal_All']:
    label_map = {
        'Top3': 'Model Top3 버킷 (매달 리밸)',
        'Top5': 'Model Top5 버킷 (매달 리밸)',
        'Score_Wt': 'Score-Weighted 포트',
        'Equal_All': '전버킷 등가중',
    }
    stats_list.append(perf_stats(port_df[pname], label_map[pname]))

stats_df = pd.DataFrame(stats_list).set_index('Portfolio')

print("\n" + "=" * 80)
print("PERFORMANCE SUMMARY  (진짜 월별 리밸런싱 백테스팅)")
print(stats_df.to_string())
print("=" * 80)

bench_total = (1 + bench_aligned).prod() - 1
for pname, label in [('Top3','Top3'),('Top5','Top5'),('Score_Wt','Score_Wt')]:
    pt = (1 + port_df[pname]).prod() - 1
    print(f"  {label}: 초과수익 {pt - bench_total:+.1%}")

# ═══════════════════════════════════════════════════════════════════════════════
# 7. 버킷 선택 빈도 분석
# ═══════════════════════════════════════════════════════════════════════════════

from collections import Counter
pick_counter = Counter()
for mp in monthly_picks:
    for bkt in mp['top3']:
        pick_counter[bkt] += 1
total_picks = sum(pick_counter.values())
pick_freq = {k: v / len(monthly_picks) for k, v in pick_counter.items()}
print("\n  Top3 선택 빈도 (버킷별):")
for bkt, freq in sorted(pick_freq.items(), key=lambda x: -x[1]):
    print(f"    {BUCKET_LABELS.get(bkt, bkt):<15}: {freq:.0%} ({pick_counter[bkt]}개월)")

# ═══════════════════════════════════════════════════════════════════════════════
# 8. 차트
# ═══════════════════════════════════════════════════════════════════════════════

COLORS = {
    'LUACTRUU (벤치마크)':       '#1F3864',
    'Model Top3 버킷 (매달 리밸)': '#C00000',
    'Model Top5 버킷 (매달 리밸)': '#E97132',
    'Score-Weighted 포트':       '#70AD47',
    '전버킷 등가중':              '#5B9BD5',
}

fig = plt.figure(figsize=(22, 14))
gs  = gridspec.GridSpec(3, 3, figure=fig,
                         hspace=0.45, wspace=0.32,
                         top=0.91, bottom=0.06)

# ─── Plot 1: 누적 수익 ──────────────────────────────────────────────────────
ax1 = fig.add_subplot(gs[0, :2])

cw_bench = cum_wealth(bench_aligned)
ax1.plot(cw_bench.index, cw_bench.values,
         color=COLORS['LUACTRUU (벤치마크)'], lw=2.5, label='LUACTRUU (벤치마크)', zorder=5)

for pname, label in [
    ('Top3',      'Model Top3 버킷 (매달 리밸)'),
    ('Top5',      'Model Top5 버킷 (매달 리밸)'),
    ('Score_Wt',  'Score-Weighted 포트'),
    ('Equal_All', '전버킷 등가중'),
]:
    cw = cum_wealth(port_df[pname])
    lw   = 2.3 if 'Top3' in label else 1.8
    ls   = '-' if 'Top3' in label or '벤치' in label else '--'
    zord = 7 if 'Top3' in label else 4
    ax1.plot(cw.index, cw.values,
             color=COLORS[label], lw=lw, ls=ls, label=label, zorder=zord)

# 주요 이벤트
EVENTS = [
    ('2016-02', 'EM/Oil\n공포'), ('2018-12', 'Fed 금리쇼크'),
    ('2020-03', 'COVID'), ('2022-06', 'Fed 긴축'), ('2023-03', 'SVB'),
]
for ev_dt, ev_lbl in EVENTS:
    xv = pd.Timestamp(ev_dt)
    if port_dates[0] <= xv <= port_dates[-1]:
        ax1.axvline(xv, color='#AAAAAA', lw=0.9, ls='--', alpha=0.7)
        ax1.text(xv, ax1.get_ylim()[1] * 0.82 if ax1.get_ylim()[1] > 0 else 103,
                 ev_lbl, fontsize=7.5, color='#555555', ha='center')

ax1.axhline(100, color='gray', lw=0.5, ls=':', alpha=0.6)
ax1.set_title('누적 수익률  (시작 $100, 월별 리밸런싱)',
              fontsize=12, fontweight='bold', pad=6)
ax1.set_ylabel('포트폴리오 가치 ($)', fontsize=10)
ax1.yaxis.set_major_formatter(mtick.FormatStrFormatter('$%.0f'))
ax1.legend(fontsize=9, loc='upper left', framealpha=0.9)
ax1.grid(True, alpha=0.2)
ax1.set_xlim(port_dates[0], port_dates[-1])

# ─── Plot 2: 성과 테이블 ──────────────────────────────────────────────────────
ax2 = fig.add_subplot(gs[0, 2])
ax2.axis('off')

rows = [[s['Portfolio'].replace(' (벤치마크)','').replace(' (매달 리밸)',''),
         s['Total'], s['Ann. Ret'], s['Ann. Vol'],
         s['Sharpe'], s['Max DD'], s['Dn Capture']]
        for s in stats_list]
col_labels = ['Portfolio', 'Total', 'Ann.\nRet', 'Vol', 'Sharpe', 'Max\nDD', 'Dn\nCapture']

tbl = ax2.table(cellText=rows, colLabels=col_labels,
                cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
tbl.auto_set_font_size(False)
tbl.set_fontsize(8)
ROW_BG = ['#1F3864', '#EBF3FB', '#FFE8E8', '#FFF0E8', '#F0FFF0', '#F5F5F5']
for (r, c), cell in tbl.get_celld().items():
    cell.set_edgecolor('#CCCCCC')
    cell.set_height(0.125)
    if r == 0:
        cell.set_facecolor('#1F3864')
        cell.set_text_props(color='white', fontweight='bold', fontsize=7.5)
    else:
        cell.set_facecolor(ROW_BG[r] if r < len(ROW_BG) else '#FFFFFF')
        if r == 2:  # Top3 행 강조
            cell.set_text_props(fontweight='bold', color='#C00000', fontsize=8.5)
ax2.set_title('성과 요약\n(월별 리밸런싱, 실제 백테스팅)',
              fontsize=10, fontweight='bold', pad=4)

# ─── Plot 3: 연도별 수익 ─────────────────────────────────────────────────────
ax3 = fig.add_subplot(gs[1, :2])

bench_yr = bench_aligned.resample('A').apply(lambda x: (1+x).prod() - 1) * 100
top3_yr  = port_df['Top3'].resample('A').apply(lambda x: (1+x).prod() - 1) * 100
eq_yr    = port_df['Equal_All'].resample('A').apply(lambda x: (1+x).prod() - 1) * 100

years  = bench_yr.index.year
x      = np.arange(len(years))
w      = 0.28

ax3.bar(x - w,    bench_yr.values, w, label='LUACTRUU', color=COLORS['LUACTRUU (벤치마크)'], alpha=0.85)
ax3.bar(x,        top3_yr.values,  w, label='Model Top3', color=COLORS['Model Top3 버킷 (매달 리밸)'], alpha=0.85)
ax3.bar(x + w,    eq_yr.values,    w, label='전버킷 등가중', color=COLORS['전버킷 등가중'], alpha=0.7)

ax3.axhline(0, color='black', lw=0.8)
ax3.set_xticks(x)
ax3.set_xticklabels([str(y) for y in years], fontsize=9)
ax3.set_title('연도별 수익률 비교  (%)', fontsize=12, fontweight='bold', pad=6)
ax3.set_ylabel('연간 수익률 (%)', fontsize=10)
ax3.yaxis.set_major_formatter(mtick.PercentFormatter())
ax3.legend(fontsize=9)
ax3.grid(True, alpha=0.2, axis='y')

for i, (bv, tv) in enumerate(zip(bench_yr.values, top3_yr.values)):
    ax3.text(x[i] - w, bv + (0.3 if bv >= 0 else -0.8),
             f'{bv:.1f}', ha='center', va='bottom' if bv>=0 else 'top',
             fontsize=6.5, color=COLORS['LUACTRUU (벤치마크)'])
    ax3.text(x[i],      tv + (0.3 if tv >= 0 else -0.8),
             f'{tv:.1f}', ha='center', va='bottom' if tv>=0 else 'top',
             fontsize=6.5, color=COLORS['Model Top3 버킷 (매달 리밸)'], fontweight='bold')

# ─── Plot 4: 월별 선택 버킷 히트맵 ──────────────────────────────────────────
ax4 = fig.add_subplot(gs[1, 2])

# 버킷별 월별 점수 히트맵 (최근 36개월)
n_recent = min(36, len(monthly_picks))
recent_picks = monthly_picks[-n_recent:]
hm_data = np.zeros((len(valid_buckets), n_recent))
for j, mp in enumerate(recent_picks):
    for i, bkt in enumerate(valid_buckets):
        sc = mp['scores'].get(bkt, 0)
        hm_data[i, j] = sc

im = ax4.imshow(hm_data, aspect='auto', cmap='RdYlGn', vmin=-3, vmax=3)
ax4.set_yticks(range(len(valid_buckets)))
ax4.set_yticklabels([BUCKET_LABELS.get(b, b) for b in valid_buckets], fontsize=8)
ax4.set_xlabel('월 (최근 36개월 →)', fontsize=9)
ax4.set_title('버킷별 스코어 히트맵\n(초록=매수선택, 빨강=기피)', fontsize=10, fontweight='bold', pad=5)
plt.colorbar(im, ax=ax4, shrink=0.8, label='Composite Score')

# 선택된 버킷 표시 (Top3)
for j, mp in enumerate(recent_picks):
    for bkt in mp['top3']:
        if bkt in valid_buckets:
            i = valid_buckets.index(bkt)
            ax4.add_patch(plt.Rectangle((j-0.5, i-0.5), 1, 1,
                          fill=False, edgecolor='black', lw=1.5))

# ─── Plot 5: 롤링 초과수익 ───────────────────────────────────────────────────
ax5 = fig.add_subplot(gs[2, :2])

excess = port_df['Top3'] - bench_aligned
roll12 = excess.rolling(12).sum() * 100

ax5.fill_between(roll12.index, roll12.values, 0,
                  where=(roll12.fillna(0).values >= 0),
                  alpha=0.6, color='#C6EFCE', label='Outperform')
ax5.fill_between(roll12.index, roll12.values, 0,
                  where=(roll12.fillna(0).values < 0),
                  alpha=0.6, color='#FFC7CE', label='Underperform')
ax5.plot(roll12.index, roll12.values, color='#C00000', lw=1.5)
ax5.axhline(0, color='gray', lw=0.8)

pct_out = (roll12.dropna() > 0).mean()
ax5.set_title(f'Model Top3 vs LUACTRUU  |  12개월 누적 초과수익  (Outperform: {pct_out:.0%})',
              fontsize=11, fontweight='bold', pad=6)
ax5.set_ylabel('초과수익 (%)', fontsize=10)
ax5.yaxis.set_major_formatter(mtick.PercentFormatter())
ax5.legend(fontsize=9)
ax5.grid(True, alpha=0.2)
ax5.set_xlim(port_dates[0], port_dates[-1])

# ─── Plot 6: 버킷 선택 빈도 ─────────────────────────────────────────────────
ax6 = fig.add_subplot(gs[2, 2])

freq_sorted = sorted(pick_freq.items(), key=lambda x: -x[1])
bkt_names   = [BUCKET_LABELS.get(k, k) for k, _ in freq_sorted]
bkt_vals    = [v * 100 for _, v in freq_sorted]

bar_colors = ['#C00000' if v > 50 else '#E97132' if v > 30 else '#5B9BD5'
              for v in bkt_vals]
bars = ax6.barh(bkt_names, bkt_vals, color=bar_colors, alpha=0.85)
ax6.axvline(100/3, color='gray', lw=1, ls='--', alpha=0.7, label='균등선택 기준')
ax6.set_xlabel('Top3 선택 빈도 (%)', fontsize=10)
ax6.set_title('버킷별 Top3 선택 빈도\n(모델이 자주 선호한 버킷)',
              fontsize=10, fontweight='bold', pad=5)
ax6.legend(fontsize=8)
for bar, val in zip(bars, bkt_vals):
    ax6.text(val + 0.5, bar.get_y() + bar.get_height()/2,
             f'{val:.0f}%', va='center', fontsize=8.5, fontweight='bold')
ax6.grid(True, alpha=0.2, axis='x')

# ─── 메인 타이틀 ────────────────────────────────────────────────────────────
ann_bench = (1 + bench_aligned).prod() ** (12/len(bench_aligned)) - 1
ann_top3  = (1 + port_df['Top3']).prod() ** (12/len(port_df)) - 1

fig.suptitle(
    f'Bloomberg US IG Corporate Bond  —  월별 리밸런싱 백테스팅\n'
    f'스코어링: Carry×0.40 + OAS레벨×0.40 + OAS모멘텀×0.20  │  '
    f'유니버스: ICE BofA 9개 버킷 (등급4 + 만기5)  │  '
    f'기간: {port_dates[0].strftime("%Y-%m")} ~ {port_dates[-1].strftime("%Y-%m")}  │  '
    f'연환산: 벤치 {ann_bench:.2%} vs Model {ann_top3:.2%}',
    fontsize=11, fontweight='bold', y=0.995
)

fig.text(
    0.5, 0.012,
    '✅ 편향 최소화: 인덱스 레벨 사용(생존편향 없음) + 전월 스코어→당월 수익(미래정보 없음)  │  '
    '스코어 공식: Carry z-score + OAS Level z-score + OAS Momentum z-score (trailing 24M window)  │  '
    'Data: FRED (ICE BofA US Corporate Bond Sub-Indices)',
    ha='center', fontsize=8, color='#444444', style='italic'
)

plt.savefig(OUT, dpi=150, bbox_inches='tight', facecolor='white')
print(f"\nChart saved → {OUT}")
print(f"File size: {os.path.getsize(OUT)/1024:.0f} KB")
print("DONE")
