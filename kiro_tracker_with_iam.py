#!/usr/bin/env python3
"""
Kiro Tracker with IAM Identity Center Integration

IAM Identity Center API를 통해 실제 사용자명과 이메일을 조회하는 Kiro 트래커
"""

# 공통 설정 import
from config import BUCKET_NAME, S3_USER_ACTIVITY_REPORT_PREFIX, SUBSCRIPTION_SERVICE_NAME

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import boto3
from botocore.exceptions import ClientError
from iam_identity_center_mapper import IAMIdentityCenterMapper, create_user_mapping_interface


# 지원 리전 정의
SUPPORTED_REGIONS = {
    'us-east-1': 'US East (N. Virginia)',
    'us-west-2': 'US West (Oregon)',
    'eu-central-1': 'Europe (Frankfurt)',
    'ap-northeast-1': 'Asia Pacific (Tokyo)',
    'ap-northeast-2': 'Asia Pacific (Seoul)',
    'ap-southeast-1': 'Asia Pacific (Singapore)',
    'ap-southeast-2': 'Asia Pacific (Sydney)'
}


class KiroTrackerWithIAM:
    def __init__(self, bucket_name: str = None, account_id: str = None):
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        # 동적 설정
        self.bucket_name = bucket_name or self._get_default_bucket()
        self.account_id = account_id or self._get_account_id()
        
        # IAM Identity Center 매퍼 초기화
        if 'user_mapper' not in st.session_state:
            st.session_state.user_mapper = IAMIdentityCenterMapper()
        self.user_mapper = st.session_state.user_mapper
    
    def _get_account_id(self) -> str:
        """현재 AWS 계정 ID 조회"""
        try:
            sts = boto3.client('sts')
            return sts.get_caller_identity()['Account']
        except Exception as e:
            st.error(f"AWS 계정 ID 조회 실패: {e}")
            return "123456789012"  # 기본값
    
    def _get_default_bucket(self) -> str:
        """기본 버킷명 반환"""
        return BUCKET_NAME
    
    def get_region_prefix(self, region: str) -> str:
        """리전별 S3 프리픽스 생성"""
        return f'{S3_USER_ACTIVITY_REPORT_PREFIX}/{self.account_id}/{SUBSCRIPTION_SERVICE_NAME}Logs/by_user_analytic/{region}/'
    
    def list_s3_buckets(self) -> list:
        """S3 버킷 목록 조회"""
        try:
            s3 = boto3.client('s3')
            response = s3.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            return sorted(buckets)
        except Exception as e:
            st.error(f"S3 버킷 목록 조회 실패: {e}")
            return []
    
    def validate_bucket_structure(self, bucket_name: str) -> dict:
        """버킷 구조 검증"""
        try:
            s3 = boto3.client('s3')
            
            # 기본 경로 확인
            base_prefix = f'{S3_USER_ACTIVITY_REPORT_PREFIX}/{self.account_id}/{SUBSCRIPTION_SERVICE_NAME}Logs/by_user_analytic/'
            
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=base_prefix,
                MaxKeys=10
            )
            
            # 디버깅: 응답 내용 확인
            contents = response.get('Contents', [])
            
            # 추가 디버깅: 정확한 prefix로 다시 검색
            exact_response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f'{S3_USER_ACTIVITY_REPORT_PREFIX}/{self.account_id}/{SUBSCRIPTION_SERVICE_NAME}Logs/by_user_analytic/',
                MaxKeys=10
            )
            exact_contents = exact_response.get('Contents', [])
            
            if contents or exact_contents:
                files = [obj['Key'] for obj in (contents or exact_contents)]
                regions = set()
                for file_key in files:
                    # 리전 추출: .../by_user_analytic/us-east-1/...
                    parts = file_key.replace(base_prefix, '').split('/')
                    if len(parts) > 0 and parts[0]:
                        regions.add(parts[0])
                
                return {
                    'valid': True,
                    'regions': list(regions),
                    'file_count': len(files),
                    'sample_files': files[:3],
                    'search_path': base_prefix
                }
            else:
                # 버킷 전체 구조 확인
                all_response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=20)
                bucket_files = [obj['Key'] for obj in all_response.get('Contents', [])]
                
                return {
                    'valid': False,
                    'error': '해당 경로에 파일이 없습니다.',
                    'search_path': base_prefix,
                    'bucket_files': bucket_files[:10],
                    'response_keys': len(contents),
                    'exact_response_keys': len(exact_contents),
                    'account_id_used': self.account_id
                }
                
        except ClientError as e:
            return {
                'valid': False,
                'error': f'버킷 접근 오류: {e}',
                'search_path': base_prefix
            }
    
    def consolidate_region_data(self, regions: list, days: int = None) -> bool:
        """선택된 리전들의 데이터를 통합"""
        try:
            all_dataframes = []
            
            for region in regions:
                st.write(f"🔄 {SUPPORTED_REGIONS[region]} 데이터 처리 중...")
                
                cmd = [
                    'python3', 'consolidate_kiro_reports_fixed.py',
                    '--bucket', self.bucket_name,
                    '--prefix', self.get_region_prefix(region),
                    '--output', f'data/temp_{region}.csv'
                ]
                
                if days:
                    cmd.extend(['--days', str(days)])
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    region_file = Path(f'data/temp_{region}.csv')
                    if region_file.exists():
                        df = pd.read_csv(region_file)
                        if not df.empty:
                            df['Region'] = region
                            df['RegionName'] = SUPPORTED_REGIONS[region]
                            all_dataframes.append(df)
                        region_file.unlink()
                    st.success(f"✅ {SUPPORTED_REGIONS[region]} 완료")
                else:
                    st.warning(f"⚠️ {SUPPORTED_REGIONS[region]} 데이터 없음")
            
            if not all_dataframes:
                st.error("❌ 모든 리전에서 데이터를 찾을 수 없습니다.")
                return False
            
            consolidated_df = pd.concat(all_dataframes, ignore_index=True)
            
            # 타임스탬프 추가하여 고유한 파일명 생성
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = self.data_dir / f'consolidated_multiregion_{timestamp}.csv'
            consolidated_df.to_csv(output_file, index=False)
            
            st.success(f"✅ 멀티 리전 데이터 통합 완료: {len(consolidated_df)}행, 사용자 {consolidated_df['UserId'].nunique()}명")
            return True
            
        except Exception as e:
            st.error(f"❌ 데이터 통합 오류: {e}")
            return False
    
    def load_data_with_user_info(self, file_path: str) -> pd.DataFrame:
        """데이터 로드 및 IAM Identity Center 사용자 정보 추가"""
        try:
            df = pd.read_csv(file_path)
            if 'ReportDate' in df.columns:
                df['ReportDate'] = pd.to_datetime(df['ReportDate'])
            
            # 고유 사용자 ID 목록
            unique_user_ids = df['UserId'].unique()
            st.info(f"📊 CSV에서 발견된 고유 사용자: {len(unique_user_ids)}명")
            
            # IAM Identity Center에서 사용자 정보 일괄 조회
            with st.spinner(f"IAM Identity Center에서 {len(unique_user_ids)}명 사용자 정보 조회 중..."):
                user_mappings = self.user_mapper.bulk_get_users(unique_user_ids)
            
            # 사용자 정보를 DataFrame에 추가
            df['DisplayName'] = df['UserId'].apply(lambda uid: user_mappings[uid]['display_name'])
            df['Email'] = df['UserId'].apply(lambda uid: user_mappings[uid]['email'])
            df['Username'] = df['UserId'].apply(lambda uid: user_mappings[uid]['username'])
            df['UserSource'] = df['UserId'].apply(lambda uid: user_mappings[uid]['source'])
            
            st.success(f"✅ 사용자 정보 매핑 완료: {len(df)}행, {df['UserId'].nunique()}명")
            return df
            
        except Exception as e:
            st.error(f"데이터 로드 실패: {e}")
            return pd.DataFrame()
    
    def analyze_user_patterns_with_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """실제 사용자명과 함께 패턴 분석"""
        user_patterns = []
        unique_users = df['UserId'].unique()
        
        st.info(f"🔍 패턴 분석 대상 사용자: {len(unique_users)}명")
        
        for user_id in unique_users:
            user_data = df[df['UserId'] == user_id]
            
            # 사용자 정보 (첫 번째 행에서 추출)
            first_row = user_data.iloc[0]
            display_name = first_row.get('DisplayName', f'User-{user_id[:8]}')
            email = first_row.get('Email', '')
            username = first_row.get('Username', '')
            user_source = first_row.get('UserSource', 'unknown')
            
            # 활동 통계
            total_days = len(user_data)
            first_activity = user_data['ReportDate'].min()
            last_activity = user_data['ReportDate'].max()
            
            # 사용 패턴
            total_chat = user_data['Chat_MessagesSent'].sum()
            total_code = user_data['Chat_AICodeLines'].sum()
            total_inline_suggestions = user_data['Inline_SuggestionsCount'].sum()
            total_inline_accepted = user_data['Inline_AcceptanceCount'].sum()
            total_dev = user_data['Dev_GenerationEventCount'].sum()
            
            # 평균 계산
            avg_chat_per_day = user_data['Chat_MessagesSent'].mean()
            avg_inline_per_day = user_data['Inline_SuggestionsCount'].mean()
            
            # 수락률
            acceptance_rate = (total_inline_accepted / total_inline_suggestions * 100) if total_inline_suggestions > 0 else 0
            
            # 사용 스타일 분류
            if avg_chat_per_day > 50:
                usage_style = "🔥 Heavy Chat User"
            elif avg_inline_per_day > 50:
                usage_style = "⚡ Heavy Code Assistant"
            elif avg_chat_per_day > 10:
                usage_style = "📝 Regular User"
            else:
                usage_style = "🌱 Light User"
            
            user_patterns.append({
                'UserId': user_id,
                'DisplayName': display_name,
                'Email': email,
                'Username': username,
                'UserSource': user_source,
                'TotalDays': total_days,
                'FirstActivity': first_activity.strftime('%Y-%m-%d') if pd.notna(first_activity) else 'N/A',
                'LastActivity': last_activity.strftime('%Y-%m-%d') if pd.notna(last_activity) else 'N/A',
                'TotalChatMessages': int(total_chat),
                'TotalCodeLines': int(total_code),
                'TotalInlineSuggestions': int(total_inline_suggestions),
                'TotalInlineAccepted': int(total_inline_accepted),
                'AcceptanceRate': round(acceptance_rate, 1),
                'TotalDevEvents': int(total_dev),
                'AvgChatPerDay': round(avg_chat_per_day, 1),
                'AvgInlinePerDay': round(avg_inline_per_day, 1),
                'UsageStyle': usage_style
            })
        
        return pd.DataFrame(user_patterns)


def main():
    st.set_page_config(
        page_title="Kiro Tracker with IAM",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Kiro Tracker with IAM Identity Center")
    st.markdown("IAM Identity Center 연동으로 실제 사용자명과 이메일 표시")
    
    # 사이드바 설정
    st.sidebar.header("⚙️ 설정")
    # 트래커 초기화 (고정된 버킷명과 STS로 자동 Account ID 조회)
    tracker = KiroTrackerWithIAM()
    
    # 버킷 구조 검증
    if st.sidebar.button("🔍 버킷 구조 검증"):
        with st.spinner("버킷 구조 검증 중..."):
            validation = tracker.validate_bucket_structure(tracker.bucket_name)
            
            if validation['valid']:
                st.sidebar.success(f"✅ 버킷 구조 유효")
                st.sidebar.info(f"발견된 리전: {', '.join(validation['regions'])}")
                st.sidebar.info(f"파일 수: {validation['file_count']}개")
            else:
                st.sidebar.error(f"❌ {validation['error']}")
                st.sidebar.code(f"검색 경로: {validation.get('search_path', 'N/A')}")
                
                if 'bucket_files' in validation:
                    st.sidebar.write("**버킷 내 실제 파일들:**")
                    for file in validation['bucket_files']:
                        st.sidebar.text(file)
    
    # 현재 설정 표시
    st.sidebar.markdown("---")
    st.sidebar.markdown("**현재 설정:**")
    st.sidebar.code(f"버킷: {tracker.bucket_name}")
    st.sidebar.code(f"계정: {tracker.account_id}")
    
    # ========== S3 데이터 수집 섹션 ==========
    st.sidebar.subheader("📥 S3 데이터 수집")
    
    # 리전 선택
    selected_regions = st.sidebar.multiselect(
        "수집할 리전 선택",
        options=list(SUPPORTED_REGIONS.keys()),
        default=['us-east-1'],
        format_func=lambda x: f"{x} ({SUPPORTED_REGIONS[x]})"
    )
    
    # S3 수집 기간 설정
    collect_date_option = st.sidebar.radio("수집 기간", ["전체 기간", "최근 N일"], key="collect_date")
    collect_days = None
    if collect_date_option == "최근 N일":
        collect_days = st.sidebar.slider("수집할 일수", 1, 90, 30, key="collect_days")
    
    # 데이터 통합 버튼
    if st.sidebar.button("🔄 리전 데이터 통합"):
        if selected_regions:
            with st.spinner("멀티 리전 데이터 통합 중..."):
                success = tracker.consolidate_region_data(selected_regions, collect_days)
                if success:
                    st.rerun()
    
    st.sidebar.markdown("---")
    
    # ========== 데이터 분석 섹션 ==========
    st.sidebar.subheader("📂 데이터 파일")
    
    # 파일 목록을 매번 새로 조회하고 최신순으로 정렬
    csv_files = sorted(list(tracker.data_dir.glob("*.csv")), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not csv_files:
        st.info("""
        💡 **시작하기:**
        1. 리전 선택 후 "리전 데이터 통합" 클릭
        2. 또는 기존 CSV 파일을 data/ 폴더에 배치
        """)
        return
    
    csv_options = [f"{f.name} ({datetime.fromtimestamp(f.stat().st_mtime).strftime('%m-%d %H:%M')})" for f in csv_files]
    csv_names = [f.name for f in csv_files]
    
    selected_index = st.sidebar.selectbox("CSV 파일 선택 (최신순)", range(len(csv_options)), format_func=lambda x: csv_options[x])
    selected_csv = csv_names[selected_index]
    selected_file_path = tracker.data_dir / selected_csv
    
    # 조회 기간 (로드된 데이터 내에서 필터링)
    st.sidebar.subheader("📅 조회 기간")
    display_date_option = st.sidebar.radio("조회 기간", ["전체 기간", "최근 N일"], key="display_date")
    display_days = None
    if display_date_option == "최근 N일":
        display_days = st.sidebar.slider("조회할 일수", 1, 90, 30, key="display_days")
    
    # 분석 모드 선택
    st.sidebar.subheader("📊 분석 모드")
    analysis_mode = st.sidebar.radio(
        "분석 유형",
        ["사용자 분석", "IAM 매핑 관리", "개별 사용자 상세"]
    )
    
    # IAM Identity Center 연결 상태
    mapper_stats = tracker.user_mapper.get_cache_stats()
    connection_status = "🟢 연결됨" if mapper_stats['identity_store_connected'] else "🔴 미연결"
    st.sidebar.write(f"**IAM Identity Center**: {connection_status}")
    st.sidebar.write(f"**매핑된 사용자**: {mapper_stats['total_users']}명")
    
    # 분석 모드별 처리
    if analysis_mode == "IAM 매핑 관리":
        create_user_mapping_interface()
        return
    
    # 데이터 로드
    df = tracker.load_data_with_user_info(selected_file_path)
    
    if df.empty:
        st.error("❌ 데이터를 로드할 수 없습니다.")
        return
    
    # 선택된 리전만 필터링
    if 'Region' in df.columns and selected_regions:
        df = df[df['Region'].isin(selected_regions)]
    
    # 날짜 필터링 적용 (로드된 데이터에서 조회 기간 적용)
    if display_days and 'ReportDate' in df.columns:
        cutoff_date = datetime.now() - timedelta(days=display_days)
        df = df[df['ReportDate'] >= cutoff_date]
        st.sidebar.info(f"📅 {cutoff_date.strftime('%Y-%m-%d')} 이후 데이터만 조회")
    
    if df.empty:
        st.warning("⚠️ 선택된 조건에 맞는 데이터가 없습니다.")
        return
    
    # 기본 정보
    st.header("📋 데이터 개요")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("총 행 수", len(df))
    with col2:
        st.metric("사용자 수", df['UserId'].nunique())
    with col3:
        region_count = df['Region'].nunique() if 'Region' in df.columns else 1
        st.metric("리전 수", region_count)
    with col4:
        iam_users = len(df[df['UserSource'] == 'iam_identity_center']['UserId'].unique())
        st.metric("IAM 연동 사용자", iam_users)
    
    if analysis_mode == "사용자 분석":
        st.header("👥 사용자 분석 (실제 이름 포함)")
        
        # 사용자 패턴 분석
        user_patterns = tracker.analyze_user_patterns_with_names(df)
        
        # 전체 통계
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_chat = user_patterns['TotalChatMessages'].sum()
            st.metric("총 Chat 메시지", f"{total_chat:,}")
        
        with col2:
            total_code = user_patterns['TotalCodeLines'].sum()
            st.metric("총 코드 라인", f"{total_code:,}")
        
        with col3:
            avg_acceptance = user_patterns['AcceptanceRate'].mean()
            st.metric("평균 수락률", f"{avg_acceptance:.1f}%")
        
        with col4:
            iam_connected = len(user_patterns[user_patterns['UserSource'] == 'iam_identity_center'])
            st.metric("IAM 연동 사용자", iam_connected)
        
        # 사용자별 상세 테이블
        st.subheader("👤 사용자별 상세 정보")
        
        # 표시할 컬럼 선택
        display_columns = [
            'DisplayName', 'Email', 'Username', 'UserSource',
            'TotalChatMessages', 'TotalCodeLines', 'AcceptanceRate',
            'TotalDays', 'UsageStyle', 'FirstActivity', 'LastActivity'
        ]
        
        # 정렬 옵션
        sort_options = ['TotalChatMessages', 'TotalCodeLines', 'AcceptanceRate', 'TotalDays']
        sort_column = st.selectbox("정렬 기준", sort_options)
        sort_ascending = st.checkbox("오름차순", value=False)
        
        sorted_patterns = user_patterns.sort_values(sort_column, ascending=sort_ascending)
        
        # 인덱스를 1부터 시작하도록 재설정
        display_df = sorted_patterns[display_columns].copy()
        display_df.index = range(1, len(display_df) + 1)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "DisplayName": "표시명",
                "Email": "이메일",
                "Username": "사용자명",
                "UserSource": "데이터 소스",
                "TotalChatMessages": "Chat 메시지",
                "TotalCodeLines": "코드 라인",
                "AcceptanceRate": "수락률 (%)",
                "TotalDays": "활동 일수",
                "UsageStyle": "사용 스타일",
                "FirstActivity": "첫 활동",
                "LastActivity": "마지막 활동"
            }
        )
        
        # 시각화
        st.subheader("📈 시각화")
        
        # ========== 1행: 2열 레이아웃 ==========
        col_left, col_right = st.columns(2)
        
        with col_left:
            # 상위 10명 Chat 메시지
            fig1 = px.bar(
                sorted_patterns.head(10),
                x='DisplayName',
                y='TotalChatMessages',
                title='상위 10명 사용자별 Chat 메시지 수',
                labels={'TotalChatMessages': 'Chat 메시지 수', 'DisplayName': '사용자명'}
            )
            fig1.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col_right:
            # 기능별 사용 비율 도넛 차트
            feature_usage = []
            
            chat_count = df['Chat_MessagesSent'].sum()
            if chat_count > 0:
                feature_usage.append({'Feature': 'Chat', 'Count': int(chat_count)})
            
            inline_count = df['Inline_SuggestionsCount'].sum()
            if inline_count > 0:
                feature_usage.append({'Feature': 'Inline 코드 제안', 'Count': int(inline_count)})
            
            if 'CodeReview_SucceededEventCount' in df.columns:
                codereview_count = df['CodeReview_SucceededEventCount'].sum() + df['CodeReview_FailedEventCount'].sum()
                if codereview_count > 0:
                    feature_usage.append({'Feature': 'Code Review', 'Count': int(codereview_count)})
            
            if 'TestGeneration_EventCount' in df.columns:
                testgen_count = df['TestGeneration_EventCount'].sum()
                if testgen_count > 0:
                    feature_usage.append({'Feature': '테스트 생성', 'Count': int(testgen_count)})
            
            if 'DocGeneration_EventCount' in df.columns:
                docgen_count = df['DocGeneration_EventCount'].sum()
                if docgen_count > 0:
                    feature_usage.append({'Feature': '문서 생성', 'Count': int(docgen_count)})
            
            if 'Dev_GenerationEventCount' in df.columns:
                dev_count = df['Dev_GenerationEventCount'].sum()
                if dev_count > 0:
                    feature_usage.append({'Feature': 'Dev Agent', 'Count': int(dev_count)})
            
            if feature_usage:
                usage_df = pd.DataFrame(feature_usage)
                fig_usage = px.pie(
                    usage_df,
                    values='Count',
                    names='Feature',
                    title='기능별 사용 비율',
                    hole=0.4
                )
                fig_usage.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_usage, use_container_width=True)
        
        # ========== 2행: 일별 트렌드 (전체 너비) ==========
        if 'ReportDate' in df.columns:
            daily_df = df.groupby('ReportDate').agg({
                'Chat_MessagesSent': 'sum',
                'Inline_SuggestionsCount': 'sum',
                'Inline_AcceptanceCount': 'sum',
                'Chat_AICodeLines': 'sum'
            }).reset_index()
            daily_df = daily_df.sort_values('ReportDate')
            
            fig_trend = go.Figure()
            
            fig_trend.add_trace(go.Scatter(
                x=daily_df['ReportDate'],
                y=daily_df['Chat_MessagesSent'],
                mode='lines+markers',
                name='Chat 메시지',
                line=dict(color='#1f77b4')
            ))
            
            fig_trend.add_trace(go.Scatter(
                x=daily_df['ReportDate'],
                y=daily_df['Inline_SuggestionsCount'],
                mode='lines+markers',
                name='Inline 제안',
                line=dict(color='#ff7f0e')
            ))
            
            fig_trend.add_trace(go.Scatter(
                x=daily_df['ReportDate'],
                y=daily_df['Inline_AcceptanceCount'],
                mode='lines+markers',
                name='Inline 수락',
                line=dict(color='#2ca02c')
            ))
            
            fig_trend.update_layout(
                title='일별 주요 지표 트렌드',
                xaxis_title='날짜',
                yaxis_title='횟수',
                hovermode='x unified',
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        
        # ========== 3행: Inline 제안 vs 수락률 (전체 너비) ==========
        fig2 = px.scatter(
            user_patterns,
            x='TotalInlineSuggestions',
            y='AcceptanceRate',
            size='TotalCodeLines',
            hover_data=['DisplayName', 'Email'],
            title='Inline 제안 수 vs 수락률 (크기: 코드 라인 수)',
            labels={
                'TotalInlineSuggestions': 'Inline 제안 수',
                'AcceptanceRate': '수락률 (%)'
            }
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    else:  # 개별 사용자 상세
        st.header("👤 개별 사용자 상세 분석")
        
        users = df['UserId'].unique()
        
        # 사용자 선택 (실제 이름으로 표시)
        user_options = []
        for uid in users:
            display_name = df[df['UserId'] == uid]['DisplayName'].iloc[0]
            email = df[df['UserId'] == uid]['Email'].iloc[0]
            user_label = f"{display_name}"
            if email:
                user_label += f" ({email})"
            user_options.append((uid, user_label))
        
        selected_user = st.selectbox(
            "분석할 사용자 선택",
            users,
            format_func=lambda uid: next(label for u, label in user_options if u == uid)
        )
        
        if selected_user:
            user_data = df[df['UserId'] == selected_user].copy()
            user_data = user_data.sort_values('ReportDate')
            
            # 사용자 정보
            first_row = user_data.iloc[0]
            display_name = first_row['DisplayName']
            email = first_row['Email']
            username = first_row['Username']
            user_source = first_row['UserSource']
            
            # 사용자 기본 정보
            st.subheader(f"👤 {display_name}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**이메일**: {email or '없음'}")
                st.write(f"**사용자명**: {username or '없음'}")
            with col2:
                st.write(f"**데이터 소스**: {user_source}")
                st.write(f"**UserId**: {selected_user[:12]}...")
            
            # 활동 통계
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("활동 일수", len(user_data))
            
            with col2:
                total_chat = user_data['Chat_MessagesSent'].sum()
                st.metric("총 Chat 메시지", f"{total_chat:,}")
            
            with col3:
                total_code = user_data['Chat_AICodeLines'].sum()
                st.metric("총 코드 라인", f"{total_code:,}")
            
            with col4:
                suggestions = user_data['Inline_SuggestionsCount'].sum()
                accepted = user_data['Inline_AcceptanceCount'].sum()
                rate = (accepted / suggestions * 100) if suggestions > 0 else 0
                st.metric("Inline 수락률", f"{rate:.1f}%")
            
            # 일별 활동 차트
            st.subheader("📈 일별 활동 패턴")
            
            fig = px.line(
                user_data,
                x='ReportDate',
                y='Chat_MessagesSent',
                title=f'{display_name} - 일별 Chat 메시지 추이',
                labels={'Chat_MessagesSent': 'Chat 메시지 수', 'ReportDate': '날짜'}
            )
            fig.update_traces(mode='lines+markers')
            st.plotly_chart(fig, use_container_width=True)
            
            # 일별 상세 데이터
            st.subheader("📅 일별 상세 데이터")
            
            display_columns = ['ReportDate', 'Chat_MessagesSent', 'Chat_AICodeLines',
                             'Inline_SuggestionsCount', 'Inline_AcceptanceCount']
            
            if 'RegionName' in user_data.columns:
                display_columns.insert(1, 'RegionName')
            
            # 테이블 인덱스를 1부터 시작하도록 설정
            display_df = user_data[display_columns].copy()
            display_df.index = range(1, len(display_df) + 1)
            
            st.dataframe(display_df, use_container_width=True)
            
            # CSV 다운로드
            user_csv = user_data.to_csv(index=False)
            st.download_button(
                label=f"📥 {display_name} 데이터 다운로드",
                data=user_csv,
                file_name=f"kiro_{display_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )


if __name__ == "__main__":
    main()
