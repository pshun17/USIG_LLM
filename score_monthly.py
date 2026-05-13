"""
score_monthly.py
─────────────────────────────────────────────────────────────────
월별 LUACSTAT 원본 파일을 입력받아 전체 스코어를 산출하고
*_SCORED.xlsx 로 저장하는 범용 스코어링 스크립트.

사용법:
    python score_monthly.py LUACSTAT_2026_05_11.xlsx
    (인수 없으면 폴더 내 최신 LUACSTAT_*.xlsx 자동 선택)

처리 순서:
    1. 파일 로드 (헤더 자동 감지)
    2. Bond_TR_Score  (Carry + Compression + DP_Rating → class %ile)
    3. AI_Macro_Score (Sector × 0.40 + Maturity × 0.35 + RatingBuf × 0.25)
    4. Eq_Mom_Score   (yfinance 현재 데이터 기준)
    5. Eq_Fund_Score  = 0  (Bloomberg 티커 ↔ Yahoo 불일치로 N/A)
    6. Sentiment_Score= 0  (현재 시점 스코어에는 별도 update_sentiment 사용)
    7. Integrated_Score = 위 5개 각 0.20 가중
    8. Score 시트 6개 + Detail_Scored 시트 포함 SCORED.xlsx 출력
"""

import sys, os, re, math, warnings, glob
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ai_macro_score import compute_ai_macro_score

BASE_DIR = r'C:\Users\sh.park\Documents\USIG_LLM'

# ─── 0. 파일 선택 ──────────────────────────────────────────────────────────────
if len(sys.argv) > 1:
    IN_FILE = sys.argv[1]
    if not os.path.isabs(IN_FILE):
        IN_FILE = os.path.join(BASE_DIR, IN_FILE)
else:
    candidates = sorted(glob.glob(os.path.join(BASE_DIR, 'LUACSTAT_*.xlsx')))
    candidates = [f for f in candidates if 'SCORED' not in f]
    if not candidates:
        raise FileNotFoundError("LUACSTAT_*.xlsx 파일을 찾을 수 없습니다.")
    IN_FILE = candidates[-1]

date_tag = re.search(r'(\d{4}_\d{2}_\d{2})', os.path.basename(IN_FILE))
DATE_TAG  = date_tag.group(1) if date_tag else 'UNKNOWN'
AS_OF     = date(int(DATE_TAG[:4]), int(DATE_TAG[5:7]), int(DATE_TAG[8:10])) if date_tag else date.today()
OUT_FILE  = IN_FILE.replace('.xlsx', '_SCORED.xlsx')

print(f"{'='*60}")
print(f"  Input : {os.path.basename(IN_FILE)}")
print(f"  As-of : {AS_OF}")
print(f"  Output: {os.path.basename(OUT_FILE)}")
print(f"{'='*60}")

# ─── 1. 헤더 자동 감지 + 로드 ─────────────────────────────────────────────────
xl = pd.ExcelFile(IN_FILE)
SHEET = xl.sheet_names[0]
print(f"\n[1] Loading sheet '{SHEET}'...")

raw_top = pd.read_excel(IN_FILE, sheet_name=SHEET, header=None, nrows=15)
hdr_row = None
for i, row in raw_top.iterrows():
    vals = [str(v) for v in row.values if str(v) != 'nan']
    if 'Des' in vals and 'ISIN' in vals:
        hdr_row = i
        break
if hdr_row is None:
    raise ValueError("헤더 행을 찾을 수 없습니다 (Des, ISIN 컬럼 탐색 실패)")

df = pd.read_excel(IN_FILE, sheet_name=SHEET, header=hdr_row)
# 첫 번째 행이 합계/요약행이면 제거 (ISIN이 없는 행)
df = df[df['ISIN'].notna() & df['ISIN'].astype(str).str.match(r'^[A-Z]{2}\w+')]
df = df.reset_index(drop=True)
print(f"  Loaded {len(df)} bonds, {len(df.columns)} columns")
print(f"  Key cols: {[c for c in ['OAS','OASD','Yield to Worst','1Y Dflt','DPFundamentalRating','DPSpreadRating','class'] if c in df.columns]}")

for col in ['OAS','OASD','Yield to Worst','1Y Dflt','OAD','Cpn','LQA']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# 유효 채권 마스크
mask = (
    df['class'].notna() &
    df['class'].astype(str).str.lower().ne('off') &
    df['OAS'].notna() &
    df['OASD'].notna()
)
print(f"  Active bonds (mask): {mask.sum()}")

# ─── 2. Bond_TR_Score ──────────────────────────────────────────────────────────
print("\n[2] Computing Bond_TR_Score...")

DP_RATING_MAP = {
    'AAA':1,'AA1':2,'AA2':3,'AA3':4,
    'A1':5,'A2':6,'A3':7,
    'BAA1':8,'BAA2':9,'BAA3':10,
    'BA1':11,'BA2':12,'BA3':13,
    'B1':14,'B2':15,'B3':16,
    'CAA1':17,'CAA2':18,'CAA3':19,'CA':20,'C':21,
}

def percentile_norm(series, ascending=True):
    n = series.notna().sum()
    if n < 2:
        return pd.Series(np.nan, index=series.index)
    ranked = series.rank(method='average', na_option='keep') if ascending else (-series).rank(method='average', na_option='keep')
    return (ranked - 1) / (n - 1) * 2 - 1

# Carry
carry_mask = mask & df['Yield to Worst'].notna()
df.loc[carry_mask, 'Carry_2.5M_pct'] = df.loc[carry_mask, 'Yield to Worst'] / 12 * 2.5

# Compression
comp_mask = mask & df['1Y Dflt'].notna()
df.loc[comp_mask, 'Spread_floor_bp']    = df.loc[comp_mask, '1Y Dflt'] * 60
df.loc[comp_mask, 'Compression_gap_bp'] = df.loc[comp_mask, 'OAS'] - df.loc[comp_mask, 'Spread_floor_bp']
df.loc[comp_mask, 'Compression_Score_pct'] = df.loc[comp_mask, 'Compression_gap_bp'] * df.loc[comp_mask, 'OASD'] / 100

# DP Rating Score
df['_dp_fund_num'] = df['DPFundamentalRating'].map(DP_RATING_MAP)
df['_dp_spd_num']  = df['DPSpreadRating'].map(DP_RATING_MAP)
df['_dp_gap']      = df['_dp_spd_num'] - df['_dp_fund_num']
df['DP_Rating_Score'] = np.nan
df.loc[mask, 'DP_Rating_Score'] = percentile_norm(df.loc[mask, '_dp_gap'], ascending=True).values
df.drop(columns=['_dp_fund_num','_dp_spd_num','_dp_gap'], inplace=True)

# Bond_TR_Est_pct
df.loc[mask, 'Bond_TR_Est_pct'] = (
    df.loc[mask, 'Carry_2.5M_pct'].fillna(0) +
    df.loc[mask, 'Compression_Score_pct'].fillna(0) +
    df.loc[mask, 'DP_Rating_Score'].fillna(0) * 0.05
)

# Bond_TR_Score: class 내 percentile
df['Bond_TR_Score'] = np.nan
for cls in df.loc[mask, 'class'].dropna().unique():
    cls_mask = mask & (df['class'] == cls)
    df.loc[cls_mask, 'Bond_TR_Score'] = percentile_norm(df.loc[cls_mask, 'Bond_TR_Est_pct']).values

print(f"  Bond_TR_Score: non-null={df['Bond_TR_Score'].notna().sum()}, "
      f"range=[{df['Bond_TR_Score'].min():.3f}, {df['Bond_TR_Score'].max():.3f}]")

# ─── 3. AI_Macro_Score ─────────────────────────────────────────────────────────
print("\n[3] Computing AI_Macro_Score...")
df = compute_ai_macro_score(df)

# ─── 4. Eq_Mom_Score (yfinance) ────────────────────────────────────────────────
print("\n[4] Fetching equity momentum data via yfinance...")

VALID_TK = re.compile(r'^[A-Z][A-Z0-9./\-]{0,9}$')

def pick_ticker(row):
    for col in ['Ticker', 'Parent Ticker', 'Eqty Ticker']:
        v = str(row.get(col, '')).strip().split()[0] if pd.notna(row.get(col)) else ''
        if v and VALID_TK.match(v):
            return v
    return None

df['_eq_ticker'] = df.apply(pick_ticker, axis=1)
tickers = [t for t in df['_eq_ticker'].dropna().unique() if t]
print(f"  Valid tickers: {len(tickers)}")

# 가격 데이터 다운로드 (52주 + 여유)
price_start = (AS_OF - timedelta(days=400)).isoformat()
price_end   = AS_OF.isoformat()

print(f"  Downloading prices {price_start} → {price_end} ...")
CHUNK = 200
price_dict = {}
for i in range(0, len(tickers), CHUNK):
    chunk = tickers[i:i+CHUNK]
    try:
        raw = yf.download(chunk, start=price_start, end=price_end,
                          auto_adjust=True, progress=False, threads=True)
        close = raw['Close'] if 'Close' in raw else raw.xs('Close', axis=1, level=0)
        for tk in chunk:
            if tk in close.columns:
                s = close[tk].dropna()
                if len(s) > 20:
                    price_dict[tk] = s
    except Exception as e:
        print(f"  chunk {i//CHUNK+1} error: {e}")
print(f"  Price data: {len(price_dict)} tickers with data")

def eq_mom(ticker):
    if ticker not in price_dict:
        return np.nan, np.nan, np.nan, np.nan
    px = price_dict[ticker]
    # as_of 기준 마지막 가격
    idx = px.index[px.index <= pd.Timestamp(AS_OF)]
    if len(idx) == 0:
        return np.nan, np.nan, np.nan, np.nan
    last = px[idx[-1]]

    def ret(days):
        past_idx = px.index[px.index <= pd.Timestamp(AS_OF) - timedelta(days=days)]
        if len(past_idx) == 0: return np.nan
        return last / px[past_idx[-1]] - 1

    r1m  = ret(30)
    r3m  = ret(90)
    # 30D vol
    recent = px[px.index >= pd.Timestamp(AS_OF) - timedelta(days=45)]
    vol30 = recent.pct_change().std() * np.sqrt(252) if len(recent) > 5 else np.nan
    # 52W high
    yr_idx = px.index[px.index >= pd.Timestamp(AS_OF) - timedelta(days=365)]
    hi52 = px[yr_idx].max() if len(yr_idx) > 0 else np.nan
    vs52 = last / hi52 if hi52 and hi52 > 0 else np.nan

    return r1m, r3m, vol30, vs52

print("  Computing per-bond momentum metrics...")
rows_eq = df['_eq_ticker'].apply(eq_mom)
df['Eq_Ret_1M']    = [r[0] for r in rows_eq]
df['Eq_Ret_3M']    = [r[1] for r in rows_eq]
df['Eq_Vol_30D']   = [r[2] for r in rows_eq]
df['Eq_vs_52w_High'] = [r[3] for r in rows_eq]

n_eq = df['Eq_Ret_1M'].notna().sum()
print(f"  Equity data coverage: {n_eq}/{mask.sum()} active bonds ({n_eq/mask.sum()*100:.1f}%)")

# 크로스 섹션 rank-normalize → Eq_Mom_Score
def rank_score(series):
    n = series.notna().sum()
    if n < 2: return pd.Series(np.nan, index=series.index)
    return (series.rank(method='average', na_option='keep') - 1) / (n - 1) * 2 - 1

eq_mask = mask & df['Eq_Ret_1M'].notna()
if eq_mask.sum() > 10:
    r1  = rank_score(df.loc[eq_mask, 'Eq_Ret_1M'])
    r3  = rank_score(df.loc[eq_mask, 'Eq_Ret_3M'])
    vol = rank_score(-df.loc[eq_mask, 'Eq_Vol_30D'])   # 낮은 변동성 = 좋음
    h52 = rank_score(df.loc[eq_mask, 'Eq_vs_52w_High'])
    df.loc[eq_mask, 'Eq_Mom_Score'] = (r1 * 0.35 + r3 * 0.35 + vol * 0.15 + h52 * 0.15)
    print(f"  Eq_Mom_Score: non-null={df['Eq_Mom_Score'].notna().sum()}, "
          f"range=[{df['Eq_Mom_Score'].min():.3f}, {df['Eq_Mom_Score'].max():.3f}]")
else:
    df['Eq_Mom_Score'] = np.nan
    print("  Eq_Mom_Score: 데이터 부족으로 생략")

# ─── 5. Eq_Fund / Sentiment = 0 ───────────────────────────────────────────────
df['Eq_Fund_Score']   = np.nan   # Yahoo Finance ↔ Bloomberg 티커 불일치
df['Sentiment_Score'] = np.nan   # 별도 update_sentiment.py 실행 필요
df['News_Generic_Flag'] = ''

print("\n[5] Eq_Fund_Score = N/A (Bloomberg ticker mismatch with Yahoo Finance)")
print("[5] Sentiment_Score = N/A (run update_sentiment.py separately)")

# ─── 6. Integrated_Score ──────────────────────────────────────────────────────
print("\n[6] Computing Integrated_Score...")

df['Integrated_Score'] = (
    df['Bond_TR_Score'].fillna(0)  * 0.20 +
    df['Eq_Mom_Score'].fillna(0)   * 0.20 +
    df['Eq_Fund_Score'].fillna(0)  * 0.20 +
    df['Sentiment_Score'].fillna(0)* 0.20 +
    df['AI_Macro_Score'].fillna(0) * 0.20
)
df.loc[~mask, 'Integrated_Score'] = np.nan

# Class rank
df['Integrated_Rank_in_Class'] = np.nan
for cls in df.loc[mask, 'class'].dropna().unique():
    cm = mask & (df['class'] == cls)
    df.loc[cm, 'Integrated_Rank_in_Class'] = df.loc[cm, 'Integrated_Score'].rank(ascending=False, method='min')

def top_flag(r):
    if pd.isna(r): return ''
    r = int(r)
    if r <= 3:  return '★★★ TOP3'
    if r <= 10: return '★★ TOP10'
    if r <= 25: return '★ TOP25'
    return ''
df['Top_Pick_Flag'] = df['Integrated_Rank_in_Class'].apply(top_flag)
print(f"  Flags: {df['Top_Pick_Flag'].value_counts().to_dict()}")

# ─── 7. Sort ───────────────────────────────────────────────────────────────────
df_out = (df[mask]
          .sort_values(['class','Integrated_Rank_in_Class'], na_position='last')
          .reset_index(drop=True))
print(f"\n  Active bonds for output: {len(df_out)}")

# ─── 8. Write SCORED.xlsx ─────────────────────────────────────────────────────
print(f"\n[7] Writing {os.path.basename(OUT_FILE)} ...")

FILL_TITLE = PatternFill('solid', fgColor='1F3864')
FILL_ID    = PatternFill('solid', fgColor='BDD7EE')
FILL_COMP  = PatternFill('solid', fgColor='FFF2CC')
FILL_SCORE = PatternFill('solid', fgColor='E2EFDA')
FILL_POS   = PatternFill('solid', fgColor='C6EFCE')
FILL_NEG   = PatternFill('solid', fgColor='FFC7CE')
FILL_TOP3  = PatternFill('solid', fgColor='FFFF00')
FILL_TOP10 = PatternFill('solid', fgColor='FCE4D6')
FILL_TOP25 = PatternFill('solid', fgColor='DDEBF7')
FILL_AI_H  = PatternFill('solid', fgColor='4A148C')
FILL_AI    = PatternFill('solid', fgColor='EDE7F6')
FILL_MV_HDR  = PatternFill('solid', fgColor='4A148C')
FILL_MV_BG   = PatternFill('solid', fgColor='F3E5F5')
FILL_MV_THEME= PatternFill('solid', fgColor='311B92')
FILL_MV_POS  = PatternFill('solid', fgColor='C8E6C9')
FILL_MV_NEG  = PatternFill('solid', fgColor='FFCDD2')
FILL_MV_NEUT = PatternFill('solid', fgColor='F5F5F5')

FONT_TOP3  = Font(name='Arial', bold=True, color='FF0000', size=10)
FONT_TOP10 = Font(name='Arial', bold=True, color='C55A11', size=10)
FONT_TOP25 = Font(name='Arial', bold=True, color='1F3864', size=10)

def _to_py(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except: pass
    if isinstance(val, (np.integer,)): return int(val)
    if isinstance(val, (np.floating,)): return float(val)
    if isinstance(val, pd.Timestamp): return val.to_pydatetime()
    return val

wb = Workbook()
wb.remove(wb.active)   # 기본 Sheet 제거

def make_sheet(name):
    return wb.create_sheet(name)

def write_title(ws, text, n_cols):
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=n_cols)
    c = ws.cell(row=1, column=1, value=text)
    c.font = Font(name='Arial', bold=True, size=11, color='FFFFFF')
    c.fill = FILL_TITLE
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 22

def write_headers(ws, hdrs, id_set, comp_set, score_set):
    for ci, h in enumerate(hdrs, 1):
        c = ws.cell(row=2, column=ci, value=h)
        c.font = Font(name='Arial', bold=True, size=10)
        c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        if h in score_set:  c.fill = FILL_SCORE
        elif h in comp_set: c.fill = FILL_COMP
        else:               c.fill = FILL_ID

def write_rows(ws, hdrs, score_set, fmt_map, flag_col=None, color_cols=None):
    for ri, row in df_out.iterrows():
        er = ri + 3
        for ci, h in enumerate(hdrs, 1):
            raw = row.get(h, np.nan) if h in row.index else np.nan
            val = _to_py(raw)
            c = ws.cell(row=er, column=ci)
            c.value = val
            c.font = Font(name='Arial', size=10)
            if h in score_set:
                c.fill = PatternFill('solid', fgColor='F4FFF4')
            if h in fmt_map:
                c.number_format = fmt_map[h]
            if flag_col and h == flag_col:
                flag = str(val) if val else ''
                c.value = flag
                if flag == '★★★ TOP3':   c.font = FONT_TOP3;  c.fill = FILL_TOP3
                elif flag == '★★ TOP10': c.font = FONT_TOP10; c.fill = FILL_TOP10
                elif flag == '★ TOP25':  c.font = FONT_TOP25; c.fill = FILL_TOP25
            if color_cols and h in color_cols and val is not None:
                try:
                    fv = float(val)
                    if not math.isnan(fv):
                        if fv > 0.3:    c.fill = FILL_POS
                        elif fv < -0.3: c.fill = FILL_NEG
                except: pass

def finalize(ws, n_cols, col_widths):
    ws.freeze_panes = 'A3'
    ws.auto_filter.ref = f'A2:{get_column_letter(n_cols)}2'
    for ci, w in col_widths.items():
        ws.column_dimensions[get_column_letter(ci)].width = w

# ── Sheet 1: Detail_Scored ────────────────────────────────────────────────────
print("  Writing Detail_Scored...")
ws = make_sheet('Detail_Scored')
all_cols = list(df_out.columns)
write_title(ws, f'LUACSTAT Detail  |  As of {AS_OF}  |  {len(df_out)} Active Bonds', len(all_cols))
for ci, h in enumerate(all_cols, 1):
    c = ws.cell(row=2, column=ci, value=h)
    c.font = Font(name='Arial', bold=True, size=9)
    c.fill = FILL_ID
    c.alignment = Alignment(horizontal='center', wrap_text=True)
for ri, row in df_out.iterrows():
    for ci, h in enumerate(all_cols, 1):
        val = _to_py(row.get(h, np.nan))
        ws.cell(row=ri+3, column=ci, value=val).font = Font(name='Arial', size=9)
ws.freeze_panes = 'A3'
ws.auto_filter.ref = f'A2:{get_column_letter(len(all_cols))}2'

# ── Sheet 2: Score_BondTR ─────────────────────────────────────────────────────
print("  Writing Score_BondTR...")
ws = make_sheet('Score_BondTR')
id_c   = ['class','Des','ISIN','Ticker','Cpn','OAS','OASD']
comp_c = ['Carry_2.5M_pct','Compression_Score_pct','DPFundamentalRating','DPSpreadRating','DP_Rating_Score']
sc_c   = ['Bond_TR_Est_pct','Bond_TR_Score']
hdrs   = id_c + comp_c + sc_c
fmt    = {'Cpn':'0.000','OAS':'0.0','OASD':'0.00',
          'Carry_2.5M_pct':'0.0000','Compression_Score_pct':'0.0000',
          'DP_Rating_Score':'0.0000','Bond_TR_Est_pct':'0.0000','Bond_TR_Score':'0.0000'}
write_title(ws, 'Bond TR Score  │  Carry + Compression + DP Rating → Bond_TR_Score [-1,+1]', len(hdrs))
write_headers(ws, hdrs, set(id_c), set(comp_c), set(sc_c))
write_rows(ws, hdrs, set(sc_c), fmt)
finalize(ws, len(hdrs), {1:12,2:28,3:16,4:10,5:7,6:7,7:7,8:11,9:13,10:16,11:16,12:11,13:13,14:13})

# ── Sheet 3: Score_EqMom ──────────────────────────────────────────────────────
print("  Writing Score_EqMom...")
ws = make_sheet('Score_EqMom')
id_c   = ['class','Des','ISIN','Ticker','Eqty Ticker']
comp_c = ['Eq_Ret_1M','Eq_Ret_3M','Eq_Vol_30D','Eq_vs_52w_High']
sc_c   = ['Eq_Mom_Score']
hdrs   = id_c + comp_c + sc_c
fmt    = {'Eq_Ret_1M':'0.00%','Eq_Ret_3M':'0.00%',
          'Eq_Vol_30D':'0.00%','Eq_vs_52w_High':'0.00%','Eq_Mom_Score':'0.0000'}
write_title(ws, f'Equity Momentum Score  │  1M/3M Return + Vol + 52W High  |  As of {AS_OF}', len(hdrs))
write_headers(ws, hdrs, set(id_c), set(comp_c), set(sc_c))
write_rows(ws, hdrs, set(sc_c), fmt)
finalize(ws, len(hdrs), {1:12,2:28,3:16,4:10,5:12,6:11,7:11,8:11,9:13,10:13})

# ── Sheet 4: Score_EqFund ─────────────────────────────────────────────────────
print("  Writing Score_EqFund...")
ws = make_sheet('Score_EqFund')
id_c   = ['class','Des','ISIN','Ticker']
sc_c   = ['Eq_Fund_Score']
hdrs   = id_c + sc_c
write_title(ws, 'Equity Fundamental Score  │  N/A (Bloomberg ticker ↔ Yahoo Finance mismatch)', len(hdrs))
write_headers(ws, hdrs, set(id_c), set(), set(sc_c))
write_rows(ws, hdrs, set(sc_c), {})
finalize(ws, len(hdrs), {1:12,2:28,3:16,4:10,5:13})

# ── Sheet 5: Score_Sentiment ──────────────────────────────────────────────────
print("  Writing Score_Sentiment...")
ws = make_sheet('Score_Sentiment')
id_c   = ['class','Des','ISIN','Ticker']
sc_c   = ['Sentiment_Score']
hdrs   = id_c + sc_c
write_title(ws, 'Sentiment Score  │  N/A for backtest period (run update_sentiment.py for current scoring)', len(hdrs))
write_headers(ws, hdrs, set(id_c), set(), set(sc_c))
write_rows(ws, hdrs, set(sc_c), {})
finalize(ws, len(hdrs), {1:12,2:28,3:16,4:10,5:13})

# ── Sheet 6: Score_AI ─────────────────────────────────────────────────────────
print("  Writing Score_AI...")
ws = make_sheet('Score_AI')
id_c_ai   = ['class','Des','ISIN','Ticker','OAD','BCLASS3','Industry Subgroup','DPFundamentalRating','Issuer Rtg']
comp_c_ai = ['AI_Sector_Score','AI_Maturity_Score','AI_RatingBuf_Score']
sc_c_ai   = ['AI_Macro_Score']
hdrs_ai   = id_c_ai + comp_c_ai + sc_c_ai
fmt_ai    = {'OAD':'0.00','AI_Sector_Score':'0.00','AI_Maturity_Score':'0.00',
             'AI_RatingBuf_Score':'0.00','AI_Macro_Score':'0.0000'}

ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(hdrs_ai))
tc = ws.cell(row=1, column=1,
    value=f'AI Macro Score  |  Sector×0.40 + Maturity×0.35 + RatingBuf×0.25  |  As of {AS_OF}')
tc.font = Font(name='Arial', bold=True, size=11, color='FFFFFF')
tc.fill = FILL_AI_H
tc.alignment = Alignment(horizontal='center', vertical='center')
ws.row_dimensions[1].height = 22

for ci, h in enumerate(hdrs_ai, 1):
    c = ws.cell(row=2, column=ci, value=h)
    c.font = Font(name='Arial', bold=True, size=10)
    c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    if h in sc_c_ai:    c.fill = FILL_AI
    elif h in comp_c_ai:c.fill = FILL_COMP
    else:               c.fill = FILL_ID

for ri, row in df_out.iterrows():
    er = ri + 3
    for ci, h in enumerate(hdrs_ai, 1):
        raw = row.get(h, np.nan) if h in row.index else np.nan
        val = _to_py(raw)
        c = ws.cell(row=er, column=ci)
        c.value = val
        c.font  = Font(name='Arial', size=10)
        if h in fmt_ai: c.number_format = fmt_ai[h]
        if h == 'AI_Macro_Score' and val is not None:
            try:
                fv = float(val)
                c.fill = FILL_POS if fv > 0.3 else FILL_NEG if fv < -0.3 else FILL_AI
            except: pass
        if h in comp_c_ai and val is not None:
            try:
                fv = float(val)
                c.fill = PatternFill('solid', fgColor='E8F5E9') if fv > 0 else PatternFill('solid', fgColor='FFEBEE') if fv < 0 else PatternFill()
            except: pass

ws.freeze_panes = 'A3'
ws.auto_filter.ref = f'A2:{get_column_letter(len(hdrs_ai))}2'
for ci, w in {1:12,2:28,3:16,4:10,5:7,6:18,7:26,8:16,9:12,10:14,11:14,12:14,13:14}.items():
    ws.column_dimensions[get_column_letter(ci)].width = w

# ── Macro View 섹션 (Score_AI 오른쪽) ─────────────────────────────────────────
import re as _re
from ai_macro_score import SUBGROUP_SCORE_MAP as _SUBMAP

_rationale = {}
_ai_src = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ai_macro_score.py')
with open(_ai_src, 'r', encoding='utf-8') as _f:
    for _line in _f:
        _m = _re.match(r"\s+'([^']+)':\s*([-\d.]+),\s*#\s*(.+)", _line)
        if _m:
            _rationale[_m.group(1)] = _m.group(3).strip()

MV_COL = len(hdrs_ai) + 2
MACRO_THEMES = [
    ("① Fed Policy & Tariff Inflation Risk",
     "[Current] Fed is in an easing cycle (cumulative -100bp since Sep 2024), but Trump's sweeping tariffs "
     "(10% universal + 25% autos/steel/aluminum) are reigniting inflation fears. Core PCE remains sticky above 2.5%.\n"
     "[Risk] If tariffs fully pass through to CPI, the Fed may be forced to pause — stagflation-lite scenario "
     "where growth slows but rates stay high. Key tail risk for credit.\n"
     "[Positioning] Prefer shorter duration, high-carry defensive names. Avoid cyclicals with thin margins "
     "and high import cost exposure. Favor regulated utilities, domestic healthcare, infrastructure."),
    ("② IG Spread Level — Historically Tight, Carry Dominates",
     "[Current] US IG OAS near post-GFC tights (~85-95bp range as of Q1 2026). Limited upside from further "
     "compression. Risk/reward asymmetric — downside (widening) larger than upside.\n"
     "[Implication] Total return increasingly driven by carry (coupon income) rather than capital gains. "
     "Selection alpha comes from superior carry-to-risk profiles within each rating/maturity bucket.\n"
     "[Positioning] Bond_TR Score emphasizes carry (YTW-based) and DP rating buffer — rewarding high-coupon, "
     "fundamentally cheap bonds. AI_Macro avoids sectors with elevated spread widening risk."),
    ("③ Yield Curve — Steepening Pressure, Front-End Anchored",
     "[Current] UST curve bear-steepening, with 10Y-2Y spread widening as fiscal deficit concerns push up "
     "term premium. 2Y anchored by near-term Fed expectations; 30Y faces upward pressure from Treasury supply.\n"
     "[Maturity Sweet Spot] OAD 4-7 years: captures carry and spread duration without excessive long-end "
     "term premium risk. OAD 7-10Y acceptable. Bonds with OAD 13Y+ penalized.\n"
     "[Positioning] Maturity_Score: peak at OAD 4-7 (+1.0), declining sharply for 13Y+ (-0.50 to -1.00). "
     "Avoid long-dated BBB bonds where both spread widening and rate risk are elevated simultaneously."),
    ("④ Tariff Exposure by Sector — Winners & Losers",
     "[Losers] Autos & Parts (-0.70 to -0.90): 25% import tariff on finished vehicles + parts supply chain. "
     "Retail Apparel/Dept Stores (-0.40 to -0.60): high China-sourced inventory → margin compression. "
     "Consumer Mfg Toys/Footwear (-0.40 to -0.55): Asia-manufactured → direct COGS impact.\n"
     "[Winners] Regulated Utilities (+0.75 to +0.85): pass-through pricing, recession-resistant, immune to trade policy. "
     "Domestic Healthcare (+0.60 to +0.75): domestic service delivery, no import exposure. "
     "Defense/Aerospace (+0.35 to +0.40): defense spending tailwinds, no tariff exposure. "
     "Domestic Steel (+0.20): protected by 25% tariff on imports.\n"
     "[Monitor] Semiconductors: US-China export controls tightening; supply chain restructuring ongoing."),
]

ws.merge_cells(start_row=1, start_column=MV_COL, end_row=1, end_column=MV_COL+3)
tc = ws.cell(row=1, column=MV_COL,
    value=f'MACRO VIEW  |  AI Sector Score Rationale  |  April 2026 Macro Environment')
tc.font = Font(name='Arial', bold=True, size=10, color='FFFFFF')
tc.fill = FILL_MV_HDR
tc.alignment = Alignment(horizontal='center', vertical='center')

for ti, (label, body) in enumerate(MACRO_THEMES):
    tr = ti + 2
    lc = ws.cell(row=tr, column=MV_COL, value=label)
    lc.font = Font(name='Arial', bold=True, size=9, color='FFFFFF')
    lc.fill = FILL_MV_THEME
    lc.alignment = Alignment(horizontal='center', vertical='top', wrap_text=True)
    ws.merge_cells(start_row=tr, start_column=MV_COL+1, end_row=tr, end_column=MV_COL+3)
    bc = ws.cell(row=tr, column=MV_COL+1, value=body)
    bc.font = Font(name='Arial', size=8.5)
    bc.fill = FILL_MV_BG
    bc.alignment = Alignment(vertical='top', wrap_text=True)

mv_subhdr_row = 6
for ci_off, (h, f) in enumerate([('Industry Subgroup',FILL_ID),('Score',FILL_AI),('Rationale',FILL_COMP),('Direction',FILL_AI)]):
    c = ws.cell(row=mv_subhdr_row, column=MV_COL+ci_off, value=h)
    c.font = Font(name='Arial', bold=True, size=9)
    c.fill = f
    c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

for si, (subgroup, score) in enumerate(sorted(_SUBMAP.items(), key=lambda x: x[1], reverse=True)):
    dr = mv_subhdr_row + 1 + si
    direction = '▲ Overweight' if score > 0.3 else ('▼ Underweight' if score < -0.3 else '— Neutral')
    row_fill = FILL_MV_POS if score > 0.3 else (FILL_MV_NEG if score < -0.3 else FILL_MV_NEUT)
    for col_off, val in enumerate([subgroup, round(score,2), _rationale.get(subgroup,''), direction]):
        c = ws.cell(row=dr, column=MV_COL+col_off, value=val)
        c.fill = row_fill
        c.font = Font(name='Arial', size=8.5,
                      bold=(col_off==1),
                      color=('375623' if score > 0.3 else '9C0006' if score < -0.3 else '555555') if col_off==3 else '000000')
        c.alignment = Alignment(horizontal='center' if col_off in (1,3) else 'left', vertical='center', wrap_text=(col_off==2))
        if col_off == 1: c.number_format = '0.00'

ws.column_dimensions[get_column_letter(MV_COL)].width   = 30
ws.column_dimensions[get_column_letter(MV_COL+1)].width = 7
ws.column_dimensions[get_column_letter(MV_COL+2)].width = 80
ws.column_dimensions[get_column_letter(MV_COL+3)].width = 15
for ti, rh in enumerate([52, 60, 60, 100]):
    ws.row_dimensions[ti+2].height = rh
ws.row_dimensions[mv_subhdr_row].height = 18

# ── Sheet 7: Score_Integrated ─────────────────────────────────────────────────
print("  Writing Score_Integrated...")
ws = make_sheet('Score_Integrated')
id_c   = ['class','Des','ISIN','Ticker','Cpn','OAS','LQA','Issuer Rtg','BCLASS3','Industry Subgroup']
comp_c = ['Bond_TR_Score','Eq_Mom_Score','Eq_Fund_Score','Sentiment_Score','AI_Macro_Score']
sc_c   = ['Integrated_Score','Integrated_Rank_in_Class','Top_Pick_Flag']
hdrs   = id_c + comp_c + sc_c
fmt    = {'Cpn':'0.000','OAS':'0.0','LQA':'0.0',
          'Bond_TR_Score':'0.0000','Eq_Mom_Score':'0.0000',
          'Eq_Fund_Score':'0.0000','Sentiment_Score':'0.0000',
          'AI_Macro_Score':'0.0000','Integrated_Score':'0.0000','Integrated_Rank_in_Class':'0'}
write_title(ws, f'Integrated Score  |  Bond_TR+EqMom+EqFund+Sentiment+AI_Macro (×0.20 each)  |  As of {AS_OF}', len(hdrs))
write_headers(ws, hdrs, set(id_c), set(comp_c), set(sc_c))
write_rows(ws, hdrs, set(sc_c), fmt,
           flag_col='Top_Pick_Flag',
           color_cols={'Bond_TR_Score','Eq_Mom_Score','AI_Macro_Score'})
finalize(ws, len(hdrs), {1:12,2:28,3:16,4:10,5:7,6:7,7:8,8:10,9:14,10:26,
                          11:13,12:13,13:13,14:13,15:13,16:13,17:10,18:14})

# ─── 저장 ─────────────────────────────────────────────────────────────────────
wb.save(OUT_FILE)
print(f"\n{'='*60}")
print(f"  Saved: {OUT_FILE}")
print(f"  Sheets: {wb.sheetnames}")
print(f"  Active bonds: {len(df_out)}")
top3_cnt = (df['Top_Pick_Flag'] == '★★★ TOP3').sum()
print(f"  TOP3 picks: {top3_cnt} bonds across {df.loc[mask,'class'].nunique()} classes")
print(f"{'='*60}")
print("DONE")
