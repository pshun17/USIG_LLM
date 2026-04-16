"""
backtest_simulation.py
═══════════════════════════════════════════════════════════════════════════════
Bloomberg US IG Corporate Bond — Scoring Model 10년 백테스트 시뮬레이션

⚠  편향 주의 (결과 해석 시 반드시 감안)
   1. 생존편향 (Survivorship Bias):
      현재(2026-03-31) 살아있는 채권 기준 → 과거에 부도/조기상환된 채권 미포함
      → 실제보다 성과 과대추산 가능성
   2. 미래정보 (Look-ahead Bias):
      현재 Integrated_Score 순위를 2014년부터 소급 적용
      → 실제 투자 시점엔 이 순위를 알 수 없었음

모델:
   포트폴리오 수익률 = LQD 실제 수익률
                     + ΔCarry  (포트폴리오 YTW - LQD 평균 YTW) / 12
                     + ΔDuration (OAD_LQD - OAD_port) × Δ10Y금리 / 100

   → 신용스프레드 변화(IG 전반)는 동일하게 반영 (LQD 수익률 기반)
   → 포트폴리오의 Alpha는 캐리 프리미엄 + 듀레이션 포지셔닝 차이에서 발생
   → LQD 기준 OAD ≈ 8.5yr, YTW ≈ 4.5% (10년 평균 근사치)

벤치마크: LQD (iShares iBoxx $ Investment Grade Corp Bond ETF)
         LUACTRUU와 상관관계 0.99 이상, 동일 지수 추종
═══════════════════════════════════════════════════════════════════════════════
"""

import warnings
import os
import ssl
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D

warnings.filterwarnings('ignore')

# ── SSL 인증서 우회 (사내 프록시 환경) ────────────────────────────────────────
os.environ['CURL_CA_BUNDLE']    = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['SSL_CERT_FILE']     = ''
ssl._create_default_https_context = ssl._create_unverified_context

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# yfinance import (SSL 패치 후)
import requests
_ses = requests.Session()
_ses.verify = False

import yfinance as yf
try:
    # curl_cffi 기반 yfinance: verify=False 환경변수로 처리
    import curl_cffi.requests as _cffi_req
    _orig_request = _cffi_req.Session.request
    def _patched_request(self, method, url, **kwargs):
        kwargs.setdefault('verify', False)
        return _orig_request(self, method, url, **kwargs)
    _cffi_req.Session.request = _patched_request
except Exception:
    pass

FILE = 'C:/Users/sh.park/Documents/USIG_LLM/LUACSTAT_2026_03_31_SCORED.xlsx'
OUT  = 'C:/Users/sh.park/Documents/USIG_LLM/backtest_result.png'

# ═══════════════════════════════════════════════════════════════════════════════
# 1. 데이터 로드
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 65)
print("Loading scored bond data...")
df = pd.read_excel(FILE, sheet_name='Detail_Scored', header=1)

for col in ['OAD', 'OAS', 'Yield to Worst', 'Integrated_Score',
            'Integrated_Rank_in_Class', 'AI_Macro_Score']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

mask = (
    df['class'].notna() &
    (df['class'].astype(str).str.lower() != 'off') &
    df['OAS'].notna() &
    df['Yield to Worst'].notna() &
    df['OAD'].notna()
)
df_active = df[mask].copy()
print(f"  Active bonds loaded: {len(df_active)}")

# 포트폴리오 구성
max_rank = df_active.groupby('class')['Integrated_Rank_in_Class'].transform('max')

df_top3  = df_active[df_active['Integrated_Rank_in_Class'] <= 3].copy()
df_top10 = df_active[df_active['Integrated_Rank_in_Class'] <= 10].copy()
df_bot3  = df_active[df_active['Integrated_Rank_in_Class'] >= max_rank - 2].copy()

print(f"\n{'Portfolio':<32} {'Bonds':>6} {'Avg YTW':>9} {'Avg OAD':>9} {'Avg OAS':>9}")
print("-" * 68)
for name, d in [('Top 3/Class (Model Pick)', df_top3),
                ('Top 10/Class',             df_top10),
                ('Full Universe (Eq-Wt)',    df_active),
                ('Bottom 3/Class',           df_bot3)]:
    ytw = d['Yield to Worst'].mean()
    oad = d['OAD'].mean()
    oas = d['OAS'].mean()
    print(f"  {name:<30} {len(d):>6} {ytw:>8.3f}% {oad:>9.2f} {oas:>9.1f}bp")

# ═══════════════════════════════════════════════════════════════════════════════
# 2. 시장 데이터 다운로드
# ═══════════════════════════════════════════════════════════════════════════════
START = '2014-01-01'
END   = '2025-03-31'

print(f"\nDownloading market data ({START} ~ {END})...")

# LQD: LUACTRUU 추종 ETF (total return)
lqd_raw   = yf.download('LQD', start=START, end=END, progress=False, auto_adjust=True)
# 멀티인덱스 컬럼 처리 (yfinance 버전에 따라 다름)
if isinstance(lqd_raw.columns, pd.MultiIndex):
    lqd_close = lqd_raw[('Close', 'LQD')].squeeze()
else:
    lqd_close = lqd_raw['Close'].squeeze()

# 10Y 미국국채 수익률
tsy_raw   = yf.download('^TNX', start=START, end=END, progress=False, auto_adjust=True)
if isinstance(tsy_raw.columns, pd.MultiIndex):
    tsy_close = tsy_raw[('Close', '^TNX')].squeeze()
else:
    tsy_close = tsy_raw['Close'].squeeze()

# 월말 기준 리샘플링
lqd_m = lqd_close.resample('M').last()
tsy_m = tsy_close.resample('M').last()

bench_ret = lqd_m.pct_change().dropna()
dtsy      = tsy_m.diff().dropna()            # 월간 금리 변화 (bp 단위 아님, % 단위: 0.25 = 25bp)

# 공통 기간 정렬
common = bench_ret.index.intersection(dtsy.index)
bench_ret = bench_ret.loc[common]
dtsy      = dtsy.loc[common]

print(f"  LQD monthly obs: {len(common)}")
print(f"  Period: {common[0].strftime('%Y-%m')} ~ {common[-1].strftime('%Y-%m')}")
print(f"  10Y Treasury range: {tsy_m.min():.2f}% ~ {tsy_m.max():.2f}%")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. 수익률 시뮬레이션
# ═══════════════════════════════════════════════════════════════════════════════
# LQD 기준 파라미터 (10년 평균 근사치)
LQD_YTW = 4.50   # % (2014~2025 LQD 평균 YTW 근사치 — 저금리기 3%, 고금리기 5.5% 평균)
LQD_OAD = 8.50   # years (LUACTRUU 지수 특성)

def portfolio_monthly_ret(port_df, bench_series, dtsy_series,
                           lqd_ytw=LQD_YTW, lqd_oad=LQD_OAD):
    """
    R_port = R_LQD
           + (YTW_port - YTW_LQD) / 1200          ← 캐리 프리미엄
           + (OAD_LQD - OAD_port) * (Δ10Y / 100)  ← 듀레이션 포지셔닝
    """
    ytw_port = port_df['Yield to Worst'].mean()
    oad_port = port_df['OAD'].mean()

    # 캐리 차이: 월간 수익률 기여
    carry_diff  = (ytw_port - lqd_ytw) / 1200

    # 듀레이션 차이: 금리 변화 시 차별적 반응
    dur_tilt = (lqd_oad - oad_port) * (dtsy_series / 100)

    total = bench_series + carry_diff + dur_tilt
    return total, ytw_port, oad_port

print("\nSimulating portfolios...")

sims = {}
chars = {}

for label, port_df in [
    ('Top 3/Class',          df_top3),
    ('Top 10/Class',         df_top10),
    ('Full Universe (Eq-Wt)', df_active),
    ('Bottom 3/Class',       df_bot3),
]:
    ret, ytw, oad = portfolio_monthly_ret(port_df, bench_ret, dtsy)
    sims[label]  = ret.loc[common]
    chars[label] = {'ytw': ytw, 'oad': oad, 'n': len(port_df)}
    print(f"  {label:<30}  YTW={ytw:.2f}%  OAD={oad:.2f}yr  "
          f"Carry_diff={ytw-LQD_YTW:+.2f}%  Dur_diff={LQD_OAD-oad:+.2f}yr")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. 성과 통계
# ═══════════════════════════════════════════════════════════════════════════════
def cum_wealth(ret, start=100):
    return (start * (1 + ret).cumprod())

def perf_stats(ret, label):
    n        = len(ret)
    ann_ret  = (1 + ret.mean()) ** 12 - 1
    ann_vol  = ret.std() * np.sqrt(12)
    sharpe   = ann_ret / ann_vol if ann_vol > 0 else 0
    cw       = cum_wealth(ret)
    max_dd   = (cw / cw.cummax() - 1).min()
    total    = (1 + ret).prod() - 1
    win_rate = (ret > 0).mean()
    return {
        'Portfolio':   label,
        'Total Ret':   f'{total:.1%}',
        'Ann. Ret':    f'{ann_ret:.2%}',
        'Ann. Vol':    f'{ann_vol:.2%}',
        'Sharpe':      f'{sharpe:.2f}',
        'Max DD':      f'{max_dd:.2%}',
        'Win Rate':    f'{win_rate:.1%}',
    }

stats_list = [perf_stats(bench_ret, 'LUACTRUU (LQD)')]
for label, ret in sims.items():
    stats_list.append(perf_stats(ret, label))

stats_df = pd.DataFrame(stats_list).set_index('Portfolio')

print("\n" + "=" * 70)
print("PERFORMANCE SUMMARY")
print(stats_df.to_string())
print("=" * 70)

# 벤치마크 대비 초과수익
bench_total = (1 + bench_ret).prod() - 1
for label, ret in sims.items():
    port_total = (1 + ret).prod() - 1
    print(f"  {label}: 초과수익 {port_total - bench_total:+.1%} vs LUACTRUU")

# ═══════════════════════════════════════════════════════════════════════════════
# 5. 차트
# ═══════════════════════════════════════════════════════════════════════════════
COLORS = {
    'LUACTRUU (LQD)':          '#1F3864',   # 진남색 — 벤치마크
    'Top 3/Class':             '#C00000',   # 빨강 — 모델 핵심
    'Top 10/Class':            '#E97132',   # 주황
    'Full Universe (Eq-Wt)':   '#2E75B6',   # 파랑
    'Bottom 3/Class':          '#AAAAAA',   # 회색
}
STYLES = {
    'LUACTRUU (LQD)':          ('-',  2.5),
    'Top 3/Class':             ('-',  2.2),
    'Top 10/Class':            ('--', 1.8),
    'Full Universe (Eq-Wt)':   (':',  1.8),
    'Bottom 3/Class':          ('-.', 1.5),
}

# 주요 시장 이벤트
EVENTS = [
    ('2016-01', 'EM/Oil\n공포'),
    ('2018-12', '금리 쇼크'),
    ('2020-03', 'COVID\n급락'),
    ('2022-01', 'Fed 긴축\n사이클'),
    ('2023-03', 'SVB\n사태'),
]

fig = plt.figure(figsize=(20, 13))
gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.42, wspace=0.32,
                         top=0.91, bottom=0.06)

# ─── Plot 1: 누적 수익 ─────────────────────────────────────────────────────
ax1 = fig.add_subplot(gs[0, :2])

# 벤치마크
cw_bench = cum_wealth(bench_ret)
ls, lw = STYLES['LUACTRUU (LQD)']
ax1.plot(cw_bench.index, cw_bench.values,
         color=COLORS['LUACTRUU (LQD)'], lw=lw, ls=ls,
         label='LUACTRUU (LQD proxy)', zorder=6)

for label, ret in sims.items():
    cw = cum_wealth(ret)
    ls, lw = STYLES[label]
    zorder = 7 if 'Top 3' in label else 4
    ax1.plot(cw.index, cw.values,
             color=COLORS[label], lw=lw, ls=ls, label=label, zorder=zorder)

ax1.axhline(100, color='gray', lw=0.6, ls=':', alpha=0.7)

# 이벤트 표시
for evt_date, evt_label in EVENTS:
    try:
        xval = pd.Timestamp(evt_date)
        if xval in cw_bench.index or common[0] <= xval <= common[-1]:
            ax1.axvline(xval, color='gray', lw=0.8, ls='--', alpha=0.5)
            ax1.text(xval, ax1.get_ylim()[0] if ax1.get_ylim()[0] > 0 else 85,
                     evt_label, fontsize=7, color='#555555',
                     ha='center', va='bottom', rotation=0)
    except Exception:
        pass

ax1.set_title('누적 수익률  (시작 $100)', fontsize=12, fontweight='bold', pad=6)
ax1.set_ylabel('포트폴리오 가치 ($)', fontsize=10)
ax1.yaxis.set_major_formatter(mtick.FormatStrFormatter('$%.0f'))
ax1.legend(fontsize=9, loc='upper left', framealpha=0.9)
ax1.grid(True, alpha=0.2)
ax1.set_xlim(common[0], common[-1])

# ─── Plot 2: 성과 테이블 ───────────────────────────────────────────────────
ax2 = fig.add_subplot(gs[0, 2])
ax2.axis('off')

rows = [[row['Portfolio'].replace(' (Eq-Wt)','').replace(' (LQD)',''),
         row['Total Ret'], row['Ann. Ret'],
         row['Ann. Vol'], row['Sharpe'], row['Max DD']]
        for row in stats_list]
col_labels = ['Portfolio', 'Total\nRet', 'Ann.\nRet', 'Ann.\nVol', 'Sharpe', 'Max\nDD']

tbl = ax2.table(cellText=rows, colLabels=col_labels,
                cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
tbl.auto_set_font_size(False)
tbl.set_fontsize(8)
ROW_COLORS = ['#1F3864', '#FFE8E8', '#FFF0E8', '#EBF3FB', '#F5F5F5', '#F5F5F5']
for (r, c), cell in tbl.get_celld().items():
    cell.set_edgecolor('#CCCCCC')
    cell.set_height(0.13)
    if r == 0:
        cell.set_facecolor('#1F3864')
        cell.set_text_props(color='white', fontweight='bold', fontsize=7.5)
    else:
        row_color = ROW_COLORS[r] if r < len(ROW_COLORS) else '#FFFFFF'
        cell.set_facecolor(row_color)
        if r == 1:
            cell.set_text_props(fontsize=8)
        elif r == 2:
            cell.set_text_props(fontweight='bold', color='#C00000', fontsize=8)
ax2.set_title('성과 요약', fontsize=11, fontweight='bold', pad=5)

# ─── Plot 3: 연도별 수익 Bar ──────────────────────────────────────────────
ax3 = fig.add_subplot(gs[1, :2])

bench_yr = bench_ret.resample('A').apply(lambda x: (1+x).prod() - 1) * 100
top3_yr  = sims['Top 3/Class'].resample('A').apply(lambda x: (1+x).prod() - 1) * 100

years = bench_yr.index.year
x     = np.arange(len(years))
w     = 0.35

bars_b = ax3.bar(x - w/2, bench_yr.values, w, label='LUACTRUU',
                  color=COLORS['LUACTRUU (LQD)'], alpha=0.8)
bars_t = ax3.bar(x + w/2, top3_yr.values, w, label='Top 3/Class (Model)',
                  color=COLORS['Top 3/Class'], alpha=0.8)

ax3.axhline(0, color='black', lw=0.8)
ax3.set_xticks(x)
ax3.set_xticklabels([str(y) for y in years], fontsize=9)
ax3.set_title('연도별 수익률 비교  (%)', fontsize=12, fontweight='bold', pad=6)
ax3.set_ylabel('연간 수익률 (%)', fontsize=10)
ax3.yaxis.set_major_formatter(mtick.PercentFormatter())
ax3.legend(fontsize=9)
ax3.grid(True, alpha=0.2, axis='y')

# 값 표시
for bar in bars_b:
    h = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2, h + (0.2 if h >= 0 else -0.8),
             f'{h:.1f}%', ha='center', va='bottom' if h >= 0 else 'top',
             fontsize=6.5, color=COLORS['LUACTRUU (LQD)'])
for bar in bars_t:
    h = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2, h + (0.2 if h >= 0 else -0.8),
             f'{h:.1f}%', ha='center', va='bottom' if h >= 0 else 'top',
             fontsize=6.5, color=COLORS['Top 3/Class'], fontweight='bold')

# ─── Plot 4: 롤링 초과수익 ────────────────────────────────────────────────
ax4 = fig.add_subplot(gs[1, 2])

excess = sims['Top 3/Class'] - bench_ret
roll12 = excess.rolling(12).sum() * 100  # 12개월 누적 초과수익 (%)

ax4.fill_between(roll12.index, roll12.values, 0,
                  where=(roll12.values >= 0), alpha=0.6,
                  color='#C6EFCE', label='Outperform')
ax4.fill_between(roll12.index, roll12.values, 0,
                  where=(roll12.values < 0), alpha=0.6,
                  color='#FFC7CE', label='Underperform')
ax4.plot(roll12.index, roll12.values, color='#C00000', lw=1.2)
ax4.axhline(0, color='gray', lw=0.8)

pct_out = (roll12.dropna() > 0).mean()
ax4.set_title(f'Model Top3 vs LUACTRUU\n12M 누적 초과수익  (Outperform {pct_out:.0%})',
              fontsize=10, fontweight='bold', pad=5)
ax4.set_ylabel('초과수익 (%)', fontsize=9)
ax4.yaxis.set_major_formatter(mtick.PercentFormatter())
ax4.legend(fontsize=8)
ax4.grid(True, alpha=0.2)
ax4.set_xlim(common[0], common[-1])

# ─── Plot 5: Drawdown ────────────────────────────────────────────────────
ax5 = fig.add_subplot(gs[2, :2])

for label_dd, ret_dd, color_dd, lw_dd, alpha_dd in [
    ('LUACTRUU (LQD)',  bench_ret,            COLORS['LUACTRUU (LQD)'], 2.0, 0.35),
    ('Top 3/Class',     sims['Top 3/Class'],  COLORS['Top 3/Class'],   2.0, 0.35),
    ('Full Universe',   sims['Full Universe (Eq-Wt)'], COLORS['Full Universe (Eq-Wt)'], 1.5, 0.25),
]:
    cw  = cum_wealth(ret_dd)
    dd  = (cw / cw.cummax() - 1) * 100
    ax5.fill_between(dd.index, dd.values, 0, alpha=alpha_dd, color=color_dd)
    ax5.plot(dd.index, dd.values, color=color_dd, lw=lw_dd, label=label_dd)

ax5.axhline(0, color='black', lw=0.5)
ax5.set_title('낙폭(Drawdown) 비교  (%)', fontsize=12, fontweight='bold', pad=6)
ax5.set_ylabel('Drawdown (%)', fontsize=10)
ax5.yaxis.set_major_formatter(mtick.PercentFormatter())
ax5.legend(fontsize=9)
ax5.grid(True, alpha=0.2)
ax5.set_xlim(common[0], common[-1])

# ─── Plot 6: Alpha 분해 ───────────────────────────────────────────────────
ax6 = fig.add_subplot(gs[2, 2])
ax6.axis('off')

top3_chars = chars['Top 3/Class']
ytw_diff   = top3_chars['ytw'] - LQD_YTW
oad_diff   = LQD_OAD - top3_chars['oad']

total_top3   = (1 + sims['Top 3/Class']).prod() - 1
total_bench  = (1 + bench_ret).prod() - 1
total_excess = total_top3 - total_bench
yrs = len(common) / 12

carry_alpha   = ytw_diff * yrs / 100
dur_alpha     = total_excess - carry_alpha * 100  # residual

decomp_data = [
    ['항목', '값', '설명'],
    ['── 포트폴리오 구성', '', ''],
    ['  종목 수', f"{top3_chars['n']}개", 'Top3 × 35클래스'],
    ['  Avg YTW', f"{top3_chars['ytw']:.2f}%", f"LQD {LQD_YTW:.1f}% 대비 {ytw_diff:+.2f}%"],
    ['  Avg OAD', f"{top3_chars['oad']:.2f}yr", f"LQD {LQD_OAD:.1f}yr 대비 {oad_diff:+.2f}yr"],
    ['', '', ''],
    ['── 시뮬레이션 성과', '', ''],
    ['  LUACTRUU 총수익', f"{total_bench:.1%}", f"연환산 {(1+total_bench)**(1/yrs)-1:.2%}"],
    ['  Top3 총수익', f"{total_top3:.1%}", f"연환산 {(1+total_top3)**(1/yrs)-1:.2%}"],
    ['  총 초과수익', f"{total_excess:+.1%}", '⚠ 편향 포함'],
    ['', '', ''],
    ['── 초과수익 원천', '', ''],
    ['  캐리 프리미엄', f"{ytw_diff:+.2f}%/yr", '높은 YTW 선택'],
    ['  듀레이션 포지션', f"{oad_diff:+.2f}yr short", '단기물 선호'],
]

tbl2 = ax6.table(cellText=[[r[0], r[1], r[2]] for r in decomp_data],
                  colLabels=None, cellLoc='left', loc='center',
                  bbox=[0, 0, 1, 1])
tbl2.auto_set_font_size(False)
tbl2.set_fontsize(8)
for (r, c), cell in tbl2.get_celld().items():
    cell.set_edgecolor('none')
    row_data = decomp_data[r] if r < len(decomp_data) else ['', '', '']
    if row_data[0].startswith('──'):
        cell.set_facecolor('#EBF3FB')
        cell.set_text_props(fontweight='bold', fontsize=8)
    elif r % 2 == 0:
        cell.set_facecolor('#FAFAFA')
    else:
        cell.set_facecolor('#FFFFFF')
    if c == 1 and r >= 7 and 'Top3' in str(row_data[0]):
        cell.set_text_props(fontweight='bold', color='#C00000')

ax6.set_title('Alpha 분해 분석', fontsize=11, fontweight='bold', pad=5)

# ─── 메인 타이틀 & 면책 ──────────────────────────────────────────────────
n_top3 = chars['Top 3/Class']['n']
ytw_t3 = chars['Top 3/Class']['ytw']
oad_t3 = chars['Top 3/Class']['oad']

fig.suptitle(
    f'Bloomberg US IG Corporate Bond  —  Scoring Model 10년 백테스트 시뮬레이션\n'
    f'Model Portfolio: Top3/Class ({n_top3}종목, Avg YTW {ytw_t3:.2f}%, OAD {oad_t3:.1f}yr)  │  '
    f'벤치마크: LUACTRUU (LQD ETF 실제 수익률)  │  기간: {common[0].strftime("%Y-%m")} ~ {common[-1].strftime("%Y-%m")}',
    fontsize=11, fontweight='bold', y=0.99
)

fig.text(
    0.5, 0.015,
    '⚠  주의: 현재(2026-03-31) 채권 특성을 과거에 소급 적용한 시뮬레이션입니다. '
    '생존편향(Survivorship Bias) 및 미래정보(Look-ahead Bias)로 인해 실제 성과보다 과대추산될 수 있습니다. '
    '초과수익 원천: 캐리 프리미엄(높은 YTW) + 듀레이션 포지셔닝 차이. 투자 의사결정에 직접 사용하지 마십시오.',
    ha='center', fontsize=7.5, color='#666666', style='italic'
)

plt.savefig(OUT, dpi=150, bbox_inches='tight', facecolor='white')
print(f"\nChart saved → {OUT}")
print(f"File size: {os.path.getsize(OUT)/1024:.0f} KB")
print("DONE")
