#!/usr/bin/env python3
"""테스트용 CSV 파일 통합 스크립트"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# CSV 파일 찾기
base_dir = Path('bugfix/daily-report')
csv_files = list(base_dir.rglob('*.csv'))

print(f"발견된 CSV 파일: {len(csv_files)}개")

all_dfs = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    # 파일명에서 날짜 추출 (202511240000 -> 2025-11-24)
    filename = csv_file.name
    date_str = filename.split('_')[-2][:8]  # 202511240000 -> 20251124
    report_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    df['ReportDate'] = report_date
    df['SourceFile'] = filename
    all_dfs.append(df)
    print(f"  - {filename}: {len(df)}행, ReportDate={report_date}")

# 통합
merged_df = pd.concat(all_dfs, ignore_index=True)

# 중복 제거 (UserId + Date 기준) - consolidate_kiro_reports_fixed.py와 동일
before_count = len(merged_df)
if 'Date' in merged_df.columns:
    merged_df = merged_df.drop_duplicates(subset=['UserId', 'Date'], keep='last')
after_count = len(merged_df)
if before_count != after_count:
    print(f"⚠️  중복 제거: {before_count - after_count}개 행 제거됨")

merged_df = merged_df.sort_values(['ReportDate', 'UserId'])

# 저장
output_path = Path('data/bugfix_gsn_consolidated.csv')
output_path.parent.mkdir(exist_ok=True)
merged_df.to_csv(output_path, index=False)

print(f"\n✅ 통합 완료: {output_path}")
print(f"   - 총 {len(merged_df)}행")
print(f"   - 날짜 범위: {merged_df['ReportDate'].min()} ~ {merged_df['ReportDate'].max()}")
