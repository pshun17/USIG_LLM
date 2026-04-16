"""
update_ai_scores.py
───────────────────
실행할 때마다 Claude API를 호출해 현재 채권시장 상황을 분석하고
ai_macro_score.py의 SUBGROUP_SCORE_MAP / RATING_BUFFER_MAP / maturity_score 를
최신 판단으로 자동 갱신합니다.

실행:
    python update_ai_scores.py

필요:
    pip install anthropic
    ANTHROPIC_API_KEY 환경변수 또는 스크립트 내 직접 입력
"""

import os
import sys
import io
import json
import datetime
import re
import warnings

warnings.filterwarnings('ignore')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import anthropic
import pandas as pd

# ─── 설정 ────────────────────────────────────────────────────────────────────
FILE     = 'C:/Users/sh.park/Documents/USIG_LLM/LUACSTAT_2026_03_31_SCORED.xlsx'
AI_SCORE = 'C:/Users/sh.park/Documents/USIG_LLM/ai_macro_score.py'
API_KEY  = os.environ.get('ANTHROPIC_API_KEY', '')   # 환경변수 없으면 아래에 직접 입력
# API_KEY = 'sk-ant-...'                              # 직접 입력 시 여기에

TODAY = datetime.date.today().isoformat()

# ─── 1. 현재 유니버스 업종 목록 수집 ─────────────────────────────────────────
print("=== update_ai_scores.py ===")
print(f"  Date: {TODAY}")
print("  Loading universe for subgroup list...")

df = pd.read_excel(FILE, sheet_name='Detail_Scored', header=1)
mask = df['class'].notna() & (df['class'].astype(str).str.lower() != 'off')
dfa = df[mask]

subgroups = sorted(dfa['Industry Subgroup'].dropna().unique().tolist())
bclass3   = sorted(dfa['BCLASS3'].dropna().unique().tolist())
ratings   = sorted(dfa['Issuer Rtg'].dropna().unique().tolist())

# 섹터별 OAS 현황 (컨텍스트 제공용)
sector_oas = (dfa.groupby('BCLASS3')['OAS']
              .agg(['mean','median','count'])
              .round(1)
              .to_dict(orient='index'))

sector_oas_str = '\n'.join(
    f"  {k}: mean={v['mean']}bp, median={v['median']}bp, n={int(v['count'])}"
    for k, v in sorted(sector_oas.items())
)

subgroup_list = '\n'.join(f'  - "{s}"' for s in subgroups)

print(f"  Subgroups: {len(subgroups)}, BCLASS3: {len(bclass3)}")

# ─── 2. Claude API 호출 ───────────────────────────────────────────────────────
print("\n  Calling Claude API for market analysis...")

client = anthropic.Anthropic(api_key=API_KEY)

PROMPT = f"""
Today is {TODAY}. You are a senior fixed income portfolio manager analyzing the US IG corporate bond market.

## Current Universe Context
The portfolio universe is Bloomberg US IG Corporate Bond Index (LUAC), ~8,600 active bonds.

## Current Sector OAS Levels (basis points)
{sector_oas_str}

## Your Task
Based on your knowledge of current macro conditions as of {TODAY}, analyze:
1. Fed policy trajectory and rate curve outlook
2. Credit spread environment (tight/wide vs historical)
3. Key macro risks (tariffs, inflation, geopolitics, recession probability)
4. Sector-specific tailwinds and headwinds

Then assign a score from -1.0 to +1.0 for EACH of the following Industry Subgroups, reflecting how attractive bonds in that sector are RIGHT NOW given current macro conditions.

Score meaning:
- +1.0 = very attractive (defensive, tailwinds, low credit risk)
-  0.0 = neutral
- -1.0 = very unattractive (macro headwinds, credit risk, tariff exposure)

Also provide:
- maturity_curve: a JSON object describing the preferred OAD duration positioning with keys "ultra_short" (OAD<2), "short" (2-4), "medium" (4-7), "medium_long" (7-10), "long" (10-13), "very_long" (13-16), "ultra_long" (16+) — each value between -1.0 and +1.0
- rating_buffer: scores for each IG rating (AAA, AA1, AA2, AA3, A1, A2, A3, BAA1, BAA2, BAA3, BA1, BA2) between -1.0 and +1.0

## Industry Subgroups to Score
{subgroup_list}

## Output Format
Return ONLY a valid JSON object with this exact structure:
{{
  "analysis_summary": "2-3 sentence summary of current macro view and key reasoning",
  "macro_date": "{TODAY}",
  "key_themes": ["theme1", "theme2", "theme3"],
  "subgroup_scores": {{
    "Electric-Integrated": 0.85,
    "Auto-Cars/Light Trucks": -0.90,
    ... (all subgroups listed above)
  }},
  "maturity_curve": {{
    "ultra_short": 0.0,
    "short": 0.3,
    "medium": 1.0,
    "medium_long": 0.7,
    "long": 0.0,
    "very_long": -0.5,
    "ultra_long": -1.0
  }},
  "rating_buffer": {{
    "AAA": 0.5,
    "AA1": 0.55,
    "AA2": 0.55,
    "AA3": 0.5,
    "A1": 0.7,
    "A2": 0.7,
    "A3": 0.65,
    "BAA1": 0.25,
    "BAA2": 0.05,
    "BAA3": -0.65,
    "BA1": -0.85,
    "BA2": -1.0
  }}
}}
"""

try:
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=8000,
        messages=[{"role": "user", "content": PROMPT}]
    )
    raw_text = response.content[0].text
    print(f"  API response received ({len(raw_text)} chars)")
except Exception as e:
    print(f"  ERROR calling Claude API: {e}")
    sys.exit(1)

# ─── 3. JSON 파싱 ─────────────────────────────────────────────────────────────
print("  Parsing response...")
try:
    # JSON 블록 추출 (마크다운 코드블록 제거)
    json_match = re.search(r'\{[\s\S]*\}', raw_text)
    if not json_match:
        raise ValueError("No JSON found in response")
    result = json.loads(json_match.group())
except Exception as e:
    print(f"  ERROR parsing JSON: {e}")
    print(f"  Raw response:\n{raw_text[:500]}")
    sys.exit(1)

analysis   = result.get('analysis_summary', '')
themes     = result.get('key_themes', [])
scores     = result.get('subgroup_scores', {})
mat_curve  = result.get('maturity_curve', {})
rat_buf    = result.get('rating_buffer', {})

print(f"\n  === Claude's Market Analysis ({TODAY}) ===")
print(f"  {analysis}")
print(f"  Key themes: {', '.join(themes)}")
print(f"  Scored subgroups: {len(scores)}")

# ─── 4. ai_macro_score.py 업데이트 ────────────────────────────────────────────
print("\n  Updating ai_macro_score.py...")

# Subgroup scores dict → Python code string
def dict_to_code(d, indent=4):
    lines = []
    for k, v in sorted(d.items()):
        lines.append(f'{" " * indent}"{k}": {float(v):.2f},')
    return '\n'.join(lines)

# Maturity score function → Python code
def maturity_to_code(mc):
    us  = mc.get('ultra_short', 0.0)
    sh  = mc.get('short', 0.3)
    me  = mc.get('medium', 1.0)
    ml  = mc.get('medium_long', 0.7)
    lo  = mc.get('long', 0.0)
    vl  = mc.get('very_long', -0.5)
    ul  = mc.get('ultra_long', -1.0)
    return f"""def maturity_score(oad):
    \"\"\"OAD 기반 만기 포지셔닝 점수 — {TODAY} 기준 AI 분석\"\"\"
    if pd.isna(oad):
        return 0.0
    oad = float(oad)
    if oad < 2:       return {us:.2f}   # ultra short
    elif oad < 4:     return {sh:.2f}   # short
    elif oad < 7:     return {me:.2f}   # medium (sweet spot)
    elif oad < 10:    return {ml:.2f}   # medium-long
    elif oad < 13:    return {lo:.2f}   # long
    elif oad < 16:    return {vl:.2f}   # very long
    else:             return {ul:.2f}   # ultra long"""

# Rating buffer dict → Python code
def rating_to_code(rb):
    lines = []
    for k in ['AAA','AA1','AA2','AA3','A1','A2','A3','BAA1','BAA2','BAA3','BA1','BA2','BA3','B1','B2','B3']:
        v = rb.get(k, rat_buf.get(k, 0.0))
        lines.append(f'    \'{k}\': {float(v):.2f},')
    return '\n'.join(lines)

new_content = f'''"""
ai_macro_score.py
─────────────────
현재 매크로 환경 기반 AI 점수 산출
※ 이 파일은 update_ai_scores.py 실행 시 자동 갱신됩니다.

[마지막 업데이트] {TODAY}
[분석 요약] {analysis}
[핵심 테마] {", ".join(themes)}

[3개 세부 점수]
1. Subgroup_Score  : Industry Subgroup 기반 세부 업종 점수
2. Maturity_Score  : OAD 기반 만기 포지셔닝
3. RatingBuf_Score : Fallen Angel 위험도

[최종]
AI_Macro_Score = Subgroup×0.40 + Maturity×0.35 + RatingBuf×0.25 → [-1, +1]
"""

import numpy as np
import pandas as pd

# ─── 1. Industry Subgroup 점수 ({TODAY} 기준) ──────────────────────────────────
SUBGROUP_SCORE_MAP = {{
{dict_to_code(scores)}
}}

# ─── 2. 만기 포지셔닝 점수 ────────────────────────────────────────────────────
{maturity_to_code(mat_curve)}

# ─── 3. 등급 안전마진 점수 ────────────────────────────────────────────────────
RATING_BUFFER_MAP = {{
{rating_to_code(rat_buf)}
}}

# ─── 4. 점수 산출 ─────────────────────────────────────────────────────────────
def compute_ai_macro_score(df):
    df = df.copy()

    subgroup_col = 'Industry Subgroup' if 'Industry Subgroup' in df.columns else 'BCLASS3'
    df['AI_Sector_Score'] = df[subgroup_col].map(SUBGROUP_SCORE_MAP)
    unmapped = df['AI_Sector_Score'].isna().sum()
    if unmapped > 0:
        print(f"    Unmapped subgroups ({{unmapped}} bonds): "
              f"{{df.loc[df['AI_Sector_Score'].isna(), subgroup_col].value_counts().head(5).to_dict()}}")
    df['AI_Sector_Score'] = df['AI_Sector_Score'].fillna(0.0)

    df['AI_Maturity_Score'] = df['OAD'].apply(maturity_score)

    df['AI_RatingBuf_Score'] = df['DPFundamentalRating'].map(RATING_BUFFER_MAP)
    fallback_mask = df['AI_RatingBuf_Score'].isna()
    df.loc[fallback_mask, 'AI_RatingBuf_Score'] = (
        df.loc[fallback_mask, 'Issuer Rtg'].map(RATING_BUFFER_MAP)
    )
    df['AI_RatingBuf_Score'] = df['AI_RatingBuf_Score'].fillna(0.0)

    df['AI_Macro_Score'] = (
        df['AI_Sector_Score']    * 0.40 +
        df['AI_Maturity_Score']  * 0.35 +
        df['AI_RatingBuf_Score'] * 0.25
    ).clip(-1.0, 1.0)

    n = df['AI_Macro_Score'].notna().sum()
    print(f"  AI_Macro_Score: non-null={{n}}, "
          f"range=[{{df['AI_Macro_Score'].min():.3f}}, {{df['AI_Macro_Score'].max():.3f}}]")
    print(f"    [{TODAY}] {{result.get('analysis_summary','')[:80]}}...")

    return df
'''

# 저장
with open(AI_SCORE, 'w', encoding='utf-8') as f:
    f.write(new_content)

print(f"  ai_macro_score.py updated successfully")

# ─── 5. build_score_sheets.py 자동 실행 ──────────────────────────────────────
print("\n  Running build_score_sheets.py with updated scores...")
import subprocess
result_run = subprocess.run(
    [sys.executable,
     'C:/Users/sh.park/Documents/USIG_LLM/build_score_sheets.py'],
    capture_output=True, text=True, encoding='utf-8'
)
print(result_run.stdout)
if result_run.returncode != 0:
    print("ERROR:", result_run.stderr[:300])

print(f"\n=== DONE ===")
print(f"  Updated: {TODAY}")
print(f"  Analysis: {analysis}")
print(f"  Themes: {', '.join(themes)}")
print(f"  Subgroups scored: {len(scores)}")
