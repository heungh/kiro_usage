#!/usr/bin/env python3
"""
Amazon Kiro User Activity Reports 통합 스크립트 (수정됨)
"""

# 공통 설정 import
from config import BUCKET_NAME, DEFAULT_REGION

import boto3
import pandas as pd
from datetime import datetime, timedelta
import argparse
from pathlib import Path
import io
import re


class KiroReportConsolidator:
    def __init__(self, bucket_name: str, base_prefix: str):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.base_prefix = base_prefix
    
    def extract_date_from_filename(self, filename: str) -> datetime:
        """파일명에서 날짜 추출"""
        # 패턴: {account_id}_by_user_analytic_202510200000_report.csv
        # 날짜는 마지막에서 두 번째 부분의 첫 8자리
        match = re.search(r'_(\d{12})_', filename)
        if match:
            date_str = match.group(1)[:8]  # 202510200000 -> 20251020
            return datetime.strptime(date_str, '%Y%m%d')
        
        raise ValueError(f"날짜를 추출할 수 없습니다: {filename}")
    
    def list_csv_files(self, start_date: datetime = None, end_date: datetime = None):
        """S3에서 CSV 파일 목록 조회"""
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.base_prefix
            )
            
            csv_files = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv'):
                    try:
                        filename = key.split('/')[-1]
                        file_date = self.extract_date_from_filename(filename)
                        
                        # 날짜 필터링
                        if start_date and file_date < start_date:
                            continue
                        if end_date and file_date > end_date:
                            continue
                        
                        csv_files.append({
                            'key': key,
                            'date': file_date,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
                    except ValueError:
                        continue
            
            return sorted(csv_files, key=lambda x: x['date'])
            
        except Exception as e:
            print(f"❌ S3 파일 목록 조회 실패: {e}")
            return []
    
    def download_and_parse_csv(self, key: str) -> pd.DataFrame:
        """S3에서 CSV 다운로드 및 파싱"""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read()
            
            # CSV 파싱
            df = pd.read_csv(io.BytesIO(content))
            
            # 날짜 컬럼 추가
            filename = key.split('/')[-1]
            file_date = self.extract_date_from_filename(filename)
            df['ReportDate'] = file_date.strftime('%Y-%m-%d')
            
            return df
            
        except Exception as e:
            print(f"❌ CSV 다운로드/파싱 실패 ({key}): {e}")
            return pd.DataFrame()
    
    def consolidate_reports(self, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """모든 리포트를 하나로 통합"""
        print("🔍 S3에서 CSV 파일 목록 조회 중...")
        csv_files = self.list_csv_files(start_date, end_date)
        
        if not csv_files:
            print("❌ 조건에 맞는 CSV 파일을 찾을 수 없습니다.")
            return pd.DataFrame()
        
        print(f"✅ {len(csv_files)}개의 CSV 파일 발견")
        
        all_dataframes = []
        
        for i, file_info in enumerate(csv_files, 1):
            key = file_info['key']
            date = file_info['date'].strftime('%Y-%m-%d')
            
            print(f"📥 [{i}/{len(csv_files)}] {date} 데이터 다운로드 중...")
            
            df = self.download_and_parse_csv(key)
            if not df.empty:
                all_dataframes.append(df)
        
        if not all_dataframes:
            print("❌ 유효한 데이터가 없습니다.")
            return pd.DataFrame()
        
        # 모든 DataFrame 합치기
        print("🔄 데이터 통합 중...")
        consolidated_df = pd.concat(all_dataframes, ignore_index=True)
        
        # 중복 제거 (UserId + Date 기준)
        before_count = len(consolidated_df)
        consolidated_df = consolidated_df.drop_duplicates(subset=['UserId', 'Date'], keep='last')
        after_count = len(consolidated_df)
        
        if before_count != after_count:
            print(f"⚠️  중복 제거: {before_count - after_count}개 행 제거됨")
        
        # 날짜순 정렬
        consolidated_df = consolidated_df.sort_values(['ReportDate', 'UserId'])
        
        print(f"✅ 통합 완료: {len(consolidated_df)}행, {len(consolidated_df.columns)}개 컬럼")
        
        return consolidated_df


def main():
    parser = argparse.ArgumentParser(
        description='Amazon Kiro User Activity Reports 통합'
    )
    
    parser.add_argument(
        '--bucket',
        default=BUCKET_NAME,
        help='S3 버킷명'
    )
    
    parser.add_argument(
        '--prefix',
        default=f'daily-report/AWSLogs/{{account_id}}/KiroLogs/by_user_analytic/{DEFAULT_REGION}/',
        help='S3 프리픽스 (account_id는 자동으로 대체됨)'
    )
    
    parser.add_argument(
        '--start-date',
        help='시작 날짜 (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--end-date',
        help='종료 날짜 (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--output',
        default='consolidated_kiro_reports.csv',
        help='출력 파일명'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        help='최근 N일 데이터만 처리'
    )
    
    args = parser.parse_args()
    
    # Account ID 자동 조회 및 prefix 대체
    if '{account_id}' in args.prefix:
        try:
            import boto3
            sts = boto3.client('sts')
            account_id = sts.get_caller_identity()['Account']
            args.prefix = args.prefix.replace('{account_id}', account_id)
            print(f"🔍 Account ID 자동 조회: {account_id}")
        except Exception as e:
            print(f"⚠️ Account ID 조회 실패: {e}")
            print("기본값을 사용하거나 --prefix에서 {account_id}를 실제 값으로 대체하세요")
            return
    
    # data 디렉토리 생성
    Path('data').mkdir(exist_ok=True)
    
    # 날짜 범위 설정
    start_date = None
    end_date = None
    
    if args.days:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        print(f"📅 최근 {args.days}일 데이터 처리: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    if start_date and end_date:
        print(f"📅 날짜 범위: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    # 통합 실행
    consolidator = KiroReportConsolidator(args.bucket, args.prefix)
    
    print("="*80)
    print("🎯 Amazon Kiro Reports 통합 시작")
    print("="*80)
    
    consolidated_df = consolidator.consolidate_reports(start_date, end_date)
    
    if consolidated_df.empty:
        print("❌ 통합할 데이터가 없습니다.")
        return
    
    # 결과 저장
    output_path = Path(args.output)
    consolidated_df.to_csv(output_path, index=False)
    
    print("="*80)
    print("✅ 통합 완료!")
    print("="*80)
    print(f"📊 결과:")
    print(f"  - 총 행 수: {len(consolidated_df):,}")
    print(f"  - 총 사용자 수: {consolidated_df['UserId'].nunique():,}")
    print(f"  - 날짜 범위: {consolidated_df['ReportDate'].min()} ~ {consolidated_df['ReportDate'].max()}")
    print(f"  - 출력 파일: {output_path.absolute()}")
    
    # 기본 통계
    print(f"\n📈 기본 통계:")
    if 'Chat_MessagesSent' in consolidated_df.columns:
        total_chat = consolidated_df['Chat_MessagesSent'].sum()
        print(f"  - 총 Chat 메시지: {total_chat:,}")
    
    if 'Inline_SuggestionsCount' in consolidated_df.columns:
        total_inline = consolidated_df['Inline_SuggestionsCount'].sum()
        print(f"  - 총 Inline 제안: {total_inline:,}")
    
    if 'Dev_GenerationEventCount' in consolidated_df.columns:
        total_dev = consolidated_df['Dev_GenerationEventCount'].sum()
        print(f"  - 총 Dev 이벤트: {total_dev:,}")
    
    print(f"\n💡 다음 단계:")
    print(f"  - CSV 확인: head {output_path}")
    print(f"  - Excel 열기: open {output_path}")


if __name__ == "__main__":
    main()
