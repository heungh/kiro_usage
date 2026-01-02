#!/usr/bin/env python3
"""
Offline Kiro Tracker - 파일 업로드 기반 버전

AWS 크레덴셜 없이 사용자가 직접 CSV 파일을 업로드하여 분석하는 버전
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
import io
from typing import List, Dict
from iam_identity_center_mapper import IAMIdentityCenterMapper


class OfflineKiroTracker:
    def __init__(self):
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        # IAM Identity Center 매퍼 (선택적)
        if 'user_mapper' not in st.session_state:
            st.session_state.user_mapper = IAMIdentityCenterMapper()
        self.user_mapper = st.session_state.user_mapper
    
    def validate_csv_format(self, df: pd.DataFrame) -> bool:
        """CSV 파일 형식 검증"""
        required_columns = [
            'UserId', 'Date', 'Chat_MessagesSent', 'Chat_AICodeLines',
            'Inline_SuggestionsCount', 'Inline_AcceptanceCount'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"❌ 필수 컬럼이 누락되었습니다: {', '.join(missing_columns)}")
            return False
        
        return True
    
    def process_uploaded_files(self, uploaded_files: List) -> pd.DataFrame:
        """업로드된 파일들을 처리하여 통합 DataFrame 생성"""
        all_dataframes = []
        
        for uploaded_file in uploaded_files:
            try:
                # CSV 파일 읽기
                df = pd.read_csv(uploaded_file)
                
                # 형식 검증
                if not self.validate_csv_format(df):
                    continue
                
                # 파일명에서 날짜 추출 시도
                filename = uploaded_file.name
                if 'ReportDate' not in df.columns:
                    # 파일명에서 날짜 추출 (예: 737168310512_by_user_analytic_202511240000_report.csv)
                    try:
                        date_str = filename.split('_')[-2][:8]  # 202511240000 -> 20251124
                        report_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        df['ReportDate'] = report_date
                    except (IndexError, ValueError):
                        # 파일명에서 추출 실패 시 Date 컬럼 사용 또는 현재 날짜
                        if 'Date' in df.columns:
                            df['ReportDate'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                        else:
                            df['ReportDate'] = datetime.now().strftime('%Y-%m-%d')
                
                # 파일 소스 정보 추가
                df['SourceFile'] = filename
                
                all_dataframes.append(df)
                st.success(f"✅ {filename} 처리 완료 ({len(df)}행)")
                
            except Exception as e:
                st.error(f"❌ {uploaded_file.name} 처리 실패: {e}")
                continue
        
        if not all_dataframes:
            return pd.DataFrame()
        
        # 모든 DataFrame 통합
        consolidated_df = pd.concat(all_dataframes, ignore_index=True)
        
        # 중복 제거
        before_count = len(consolidated_df)
        consolidated_df = consolidated_df.drop_duplicates(subset=['UserId', 'Date'], keep='last')
        after_count = len(consolidated_df)
        
        if before_count != after_count:
            st.info(f"ℹ️ 중복 제거: {before_count - after_count}개 행 제거됨")
        
        return consolidated_df
    
    def load_data_with_user_info(self, df: pd.DataFrame, use_iam: bool = True) -> pd.DataFrame:
        """데이터에 사용자 정보 추가 (IAM 연동 선택적)"""
        if df.empty:
            return df
        
        # 날짜 컬럼 처리
        if 'ReportDate' in df.columns:
            df['ReportDate'] = pd.to_datetime(df['ReportDate'])
        
        if use_iam and self.user_mapper.identity_store_client:
            # IAM Identity Center 연동
            unique_user_ids = df['UserId'].unique()
            st.info(f"📊 IAM Identity Center에서 {len(unique_user_ids)}명 사용자 정보 조회 중...")
            
            with st.spinner("사용자 정보 매핑 중..."):
                user_mappings = self.user_mapper.bulk_get_users(unique_user_ids)
            
            # 사용자 정보 추가
            df['DisplayName'] = df['UserId'].apply(lambda uid: user_mappings[uid]['display_name'])
            df['Email'] = df['UserId'].apply(lambda uid: user_mappings[uid]['email'])
            df['Username'] = df['UserId'].apply(lambda uid: user_mappings[uid]['username'])
            df['UserSource'] = df['UserId'].apply(lambda uid: user_mappings[uid]['source'])
            
            st.success(f"✅ IAM 사용자 정보 매핑 완료")
        else:
            # 기본 사용자 정보 (IAM 없이)
            df['DisplayName'] = df['UserId'].apply(lambda uid: f'User-{uid[:8]}')
            df['Email'] = ''
            df['Username'] = df['UserId'].apply(lambda uid: f'user-{uid[:8]}')
            df['UserSource'] = 'uploaded_file'
            
            st.info("ℹ️ 기본 사용자 정보로 표시 (IAM 연동 미사용)")
        
        return df
    
    def analyze_user_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """사용자 패턴 분석"""
        user_patterns = []
        unique_users = df['UserId'].unique()
        
        for user_id in unique_users:
            user_data = df[df['UserId'] == user_id]
            first_row = user_data.iloc[0]
            
            # 기본 정보
            display_name = first_row.get('DisplayName', f'User-{user_id[:8]}')
            email = first_row.get('Email', '')
            username = first_row.get('Username', '')
            user_source = first_row.get('UserSource', 'unknown')
            
            # 활동 통계
            total_days = len(user_data)
            total_chat = user_data['Chat_MessagesSent'].sum()
            total_code = user_data['Chat_AICodeLines'].sum()
            total_inline_suggestions = user_data['Inline_SuggestionsCount'].sum()
            total_inline_accepted = user_data['Inline_AcceptanceCount'].sum()
            
            # 평균 및 수락률
            avg_chat_per_day = user_data['Chat_MessagesSent'].mean()
            acceptance_rate = (total_inline_accepted / total_inline_suggestions * 100) if total_inline_suggestions > 0 else 0
            
            # 사용 스타일 분류
            if avg_chat_per_day > 50:
                usage_style = "🔥 Heavy Chat User"
            elif total_inline_suggestions > 100:
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
                'TotalChatMessages': int(total_chat),
                'TotalCodeLines': int(total_code),
                'TotalInlineSuggestions': int(total_inline_suggestions),
                'TotalInlineAccepted': int(total_inline_accepted),
                'AcceptanceRate': round(acceptance_rate, 1),
                'AvgChatPerDay': round(avg_chat_per_day, 1),
                'UsageStyle': usage_style
            })
        
        return pd.DataFrame(user_patterns)


def main():
    st.set_page_config(
        page_title="Offline Kiro Tracker",
        page_icon="📁",
        layout="wide"
    )
    
    st.title("📁 Offline Kiro Tracker")
    st.markdown("CSV 파일 업로드 기반 Kiro 사용 현황 분석 (AWS 크레덴셜 불필요)")
    
    # 트래커 초기화
    tracker = OfflineKiroTracker()
    
    # 사이드바 설정
    st.sidebar.header("⚙️ 설정")
    
    # IAM Identity Center 연동 옵션
    use_iam = st.sidebar.checkbox(
        "IAM Identity Center 연동 사용",
        value=False,  # 기본값을 False로 변경 (크레덴셜 없이 사용)
        help="체크 해제 시 기본 사용자 정보로 표시"
    )
    
    # 분석 모드 선택
    st.sidebar.subheader("📊 분석 모드")
    analysis_mode = st.sidebar.radio(
        "분석 유형",
        ["사용자 분석", "개별 사용자 상세"]
    )
    
    # 조회 기간 필터
    st.sidebar.subheader("📅 조회 기간")
    display_date_option = st.sidebar.radio("조회 기간", ["전체 기간", "최근 N일"], key="display_date")
    display_days = None
    if display_date_option == "최근 N일":
        display_days = st.sidebar.slider("조회할 일수", 1, 90, 30, key="display_days")
    
    if use_iam:
        try:
            mapper_stats = tracker.user_mapper.get_cache_stats()
            connection_status = "🟢 연결됨" if mapper_stats['identity_store_connected'] else "🔴 미연결"
            st.sidebar.write(f"**IAM Identity Center**: {connection_status}")
            st.sidebar.write(f"**매핑된 사용자**: {mapper_stats['total_users']}명")
        except Exception as e:
            st.sidebar.error(f"IAM 연동 오류: {e}")
            use_iam = False
    
    # 파일 업로드 섹션
    st.header("📤 CSV 파일 업로드")
    
    uploaded_files = st.file_uploader(
        "Kiro 사용 현황 CSV 파일들을 업로드하세요",
        type=['csv'],
        accept_multiple_files=True,
        help="여러 파일을 동시에 선택할 수 있습니다. 파일들은 자동으로 통합됩니다."
    )
    
    if not uploaded_files:
        st.info("""
        💡 **사용 방법:**
        1. S3에서 다운로드한 Kiro 사용 현황 CSV 파일들을 업로드하세요
        2. 여러 파일을 동시에 선택 가능합니다
        3. 파일들은 자동으로 통합되어 분석됩니다
        
        **필수 컬럼:** UserId, Date, Chat_MessagesSent, Chat_AICodeLines, Inline_SuggestionsCount, Inline_AcceptanceCount
        """)
        return
    
    # 파일 처리
    with st.spinner("업로드된 파일들을 처리하는 중..."):
        df = tracker.process_uploaded_files(uploaded_files)
    
    if df.empty:
        st.error("❌ 처리 가능한 데이터가 없습니다.")
        return
    
    # 데이터에 사용자 정보 추가
    df = tracker.load_data_with_user_info(df, use_iam)
    
    # 조회 기간 필터링 적용
    date_column = 'ReportDate' if 'ReportDate' in df.columns else 'Date'
    if display_days and date_column in df.columns:
        from datetime import timedelta
        df[date_column] = pd.to_datetime(df[date_column])
        cutoff_date = datetime.now() - timedelta(days=display_days)
        df = df[df[date_column] >= cutoff_date]
        st.sidebar.info(f"📅 {cutoff_date.strftime('%Y-%m-%d')} 이후 데이터만 조회")
    
    if df.empty:
        st.warning("⚠️ 선택된 조건에 맞는 데이터가 없습니다.")
        return
    
    # 기본 정보 표시
    st.header("📋 데이터 개요")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("총 행 수", len(df))
    with col2:
        st.metric("사용자 수", df['UserId'].nunique())
    with col3:
        file_count = df['SourceFile'].nunique() if 'SourceFile' in df.columns else len(uploaded_files)
        st.metric("업로드 파일 수", file_count)
    with col4:
        if use_iam:
            iam_users = len(df[df['UserSource'] == 'iam_identity_center']['UserId'].unique())
            st.metric("IAM 연동 사용자", iam_users)
        else:
            st.metric("분석 모드", "오프라인")
    
    # 분석 모드별 처리
    if analysis_mode == "사용자 분석":
        # 사용자 분석
        st.header("👥 사용자 분석")
        
        user_patterns = tracker.analyze_user_patterns(df)
        
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
            active_users = len(user_patterns[user_patterns['TotalChatMessages'] > 0])
            st.metric("활성 사용자", active_users)
        
        # 사용자별 상세 테이블
        st.subheader("👤 사용자별 상세 정보")
        
        display_columns = [
            'DisplayName', 'Email', 'Username', 'UserSource',
            'TotalChatMessages', 'TotalCodeLines', 'AcceptanceRate',
            'TotalDays', 'UsageStyle'
        ]
        
        # 정렬 옵션
        sort_column = st.selectbox("정렬 기준", ['TotalChatMessages', 'TotalCodeLines', 'AcceptanceRate', 'TotalDays'])
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
                "UsageStyle": "사용 스타일"
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
        date_column = 'Date' if 'Date' in df.columns else 'ReportDate'
        if date_column in df.columns:
            daily_df = df.groupby(date_column).agg({
                'Chat_MessagesSent': 'sum',
                'Inline_SuggestionsCount': 'sum',
                'Inline_AcceptanceCount': 'sum',
                'Chat_AICodeLines': 'sum'
            }).reset_index()
            daily_df = daily_df.sort_values(date_column)
            
            fig_trend = go.Figure()
            
            fig_trend.add_trace(go.Scatter(
                x=daily_df[date_column],
                y=daily_df['Chat_MessagesSent'],
                mode='lines+markers',
                name='Chat 메시지',
                line=dict(color='#1f77b4')
            ))
            
            fig_trend.add_trace(go.Scatter(
                x=daily_df[date_column],
                y=daily_df['Inline_SuggestionsCount'],
                mode='lines+markers',
                name='Inline 제안',
                line=dict(color='#ff7f0e')
            ))
            
            fig_trend.add_trace(go.Scatter(
                x=daily_df[date_column],
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
        
        # 데이터 다운로드
        st.subheader("💾 결과 다운로드")
        
        # 통합된 원본 데이터
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="📥 통합 데이터 다운로드 (CSV)",
            data=csv_data,
            file_name=f"consolidated_kiro_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
        # 사용자 분석 결과
        analysis_csv = user_patterns.to_csv(index=False)
        st.download_button(
            label="📊 사용자 분석 결과 다운로드 (CSV)",
            data=analysis_csv,
            file_name=f"kiro_user_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
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
            
            # 날짜 컬럼 처리 및 정렬
            if 'Date' in user_data.columns:
                user_data['Date'] = pd.to_datetime(user_data['Date'])
                user_data = user_data.sort_values('Date')
            elif 'ReportDate' in user_data.columns:
                user_data['ReportDate'] = pd.to_datetime(user_data['ReportDate'])
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
            
            # 날짜 컬럼 확인
            date_column = 'Date' if 'Date' in user_data.columns else 'ReportDate'
            
            if date_column in user_data.columns:
                fig = px.line(
                    user_data,
                    x=date_column,
                    y='Chat_MessagesSent',
                    title=f'{display_name} - 일별 Chat 메시지 추이',
                    labels={'Chat_MessagesSent': 'Chat 메시지 수', date_column: '날짜'}
                )
                fig.update_traces(mode='lines+markers')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("날짜 정보가 없어 시계열 차트를 표시할 수 없습니다.")
            
            # 일별 상세 데이터
            st.subheader("📅 일별 상세 데이터")
            
            display_columns = ['Chat_MessagesSent', 'Chat_AICodeLines',
                             'Inline_SuggestionsCount', 'Inline_AcceptanceCount']
            
            if date_column in user_data.columns:
                display_columns.insert(0, date_column)
            
            if 'SourceFile' in user_data.columns:
                display_columns.append('SourceFile')
            
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