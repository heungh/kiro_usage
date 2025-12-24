#!/usr/bin/env python3
"""
IAM Identity Center 사용자 매핑 모듈 (실제 연동 버전)

실제 IAM Identity Center에서 사용자 정보를 조회합니다.
- bedrock_group 그룹
- bedrock_user1 사용자
"""

from config import DEFAULT_REGION

import boto3
import json
import streamlit as st
from pathlib import Path
from typing import Dict, Optional, List
import time


class IAMIdentityCenterMapper:
    def __init__(self):
        self.identity_store_client = None
        self.sso_admin_client = None
        self.identity_store_id = None
        self.cache_file = Path("data/user_cache.json")
        self.user_cache = {}
        self.load_cache()
        self.initialize_clients()

    def initialize_clients(self):
        """IAM Identity Center 클라이언트 초기화"""
        try:
            # DEFAULT_REGION 리전에서 Identity Center 사용
            self.sso_admin_client = boto3.client(
                "sso-admin", region_name=DEFAULT_REGION
            )
            self.identity_store_client = boto3.client(
                "identitystore", region_name=DEFAULT_REGION
            )

            # Identity Store ID 자동 감지
            self.identity_store_id = self.get_identity_store_id()

            if self.identity_store_id:
                st.success(f"✅ IAM Identity Center 연결됨: {self.identity_store_id}")
            else:
                st.warning("⚠️ IAM Identity Center가 설정되지 않음")

        except Exception as e:
            st.error(f"❌ IAM Identity Center 연결 실패: {e}")
            self.identity_store_client = None

    def get_identity_store_id(self) -> Optional[str]:
        """Identity Store ID 자동 감지"""
        try:
            if not self.sso_admin_client:
                return None

            response = self.sso_admin_client.list_instances()
            instances = response.get("Instances", [])

            if instances:
                return instances[0]["IdentityStoreId"]
            else:
                return None

        except Exception as e:
            print(f"Identity Store ID 조회 실패: {e}")
            return None

    def load_cache(self):
        """사용자 캐시 로드"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.user_cache = json.load(f)
            except Exception as e:
                print(f"캐시 로드 실패: {e}")
                self.user_cache = {}
        else:
            self.user_cache = {}

    def save_cache(self):
        """사용자 캐시 저장"""
        try:
            self.cache_file.parent.mkdir(exist_ok=True)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.user_cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"캐시 저장 실패: {e}")

    def get_user_info(self, user_id: str) -> Dict[str, str]:
        """UserId로 사용자 정보 조회"""
        # 캐시 확인 (24시간 유효)
        if user_id in self.user_cache:
            cached_info = self.user_cache[user_id]
            if time.time() - cached_info.get("cached_at", 0) < 86400:
                return cached_info

        # IAM Identity Center에서 조회
        if self.identity_store_client and self.identity_store_id:
            try:
                response = self.identity_store_client.describe_user(
                    IdentityStoreId=self.identity_store_id, UserId=user_id
                )

                user_info = {
                    "user_id": user_id,
                    "username": response.get("UserName", ""),
                    "display_name": response.get("DisplayName", ""),
                    "email": "",
                    "first_name": response.get("Name", {}).get("GivenName", ""),
                    "last_name": response.get("Name", {}).get("FamilyName", ""),
                    "cached_at": time.time(),
                    "source": "iam_identity_center",
                }

                # 이메일 추출
                emails = response.get("Emails", [])
                if emails:
                    user_info["email"] = emails[0].get("Value", "")

                # 캐시에 저장
                self.user_cache[user_id] = user_info
                self.save_cache()

                return user_info

            except Exception as e:
                print(f"사용자 {user_id} 조회 실패: {e}")

        # 기본값 반환 (연결 실패 시)
        return {
            "user_id": user_id,
            "username": f"User-{user_id[:8]}",
            "display_name": f"User-{user_id[:8]}",
            "email": "",
            "first_name": "",
            "last_name": "",
            "cached_at": time.time(),
            "source": "fallback",
        }

    def list_all_users(self) -> List[Dict]:
        """Identity Center의 모든 사용자 조회"""
        if not self.identity_store_client or not self.identity_store_id:
            return []

        try:
            users = []
            next_token = None

            while True:
                if next_token:
                    response = self.identity_store_client.list_users(
                        IdentityStoreId=self.identity_store_id, NextToken=next_token
                    )
                else:
                    response = self.identity_store_client.list_users(
                        IdentityStoreId=self.identity_store_id
                    )

                for user in response.get("Users", []):
                    user_info = {
                        "user_id": user["UserId"],
                        "username": user.get("UserName", ""),
                        "display_name": user.get("DisplayName", ""),
                        "email": (
                            user.get("Emails", [{}])[0].get("Value", "")
                            if user.get("Emails")
                            else ""
                        ),
                        "first_name": user.get("Name", {}).get("GivenName", ""),
                        "last_name": user.get("Name", {}).get("FamilyName", ""),
                        "cached_at": time.time(),
                        "source": "iam_identity_center",
                    }
                    users.append(user_info)

                    # 캐시에도 저장
                    self.user_cache[user["UserId"]] = user_info

                next_token = response.get("NextToken")
                if not next_token:
                    break

            # 캐시 저장
            self.save_cache()
            return users

        except Exception as e:
            st.error(f"사용자 목록 조회 실패: {e}")
            return []

    def search_user_by_username(self, username: str) -> Optional[Dict]:
        """사용자명으로 사용자 검색"""
        if not self.identity_store_client or not self.identity_store_id:
            return None

        try:
            response = self.identity_store_client.list_users(
                IdentityStoreId=self.identity_store_id,
                Filters=[{"AttributePath": "UserName", "AttributeValue": username}],
            )

            users = response.get("Users", [])
            if users:
                user = users[0]
                return self.get_user_info(user["UserId"])

            return None

        except Exception as e:
            print(f"사용자 검색 실패: {e}")
            return None

    def get_display_name(self, user_id: str) -> str:
        """사용자 표시명 조회"""
        user_info = self.get_user_info(user_id)

        if user_info["display_name"]:
            return user_info["display_name"]
        elif user_info["username"]:
            return user_info["username"]
        else:
            return f"User-{user_id[:8]}"

    def get_email(self, user_id: str) -> str:
        """사용자 이메일 조회"""
        user_info = self.get_user_info(user_id)
        return user_info.get("email", "")

    def bulk_get_users(self, user_ids: list) -> Dict[str, Dict]:
        """여러 사용자 정보 일괄 조회"""
        results = {}

        for user_id in user_ids:
            results[user_id] = self.get_user_info(user_id)

        return results

    def get_cache_stats(self) -> Dict:
        """캐시 통계 조회"""
        total_users = len(self.user_cache)
        iam_users = len(
            [
                u
                for u in self.user_cache.values()
                if u.get("source") == "iam_identity_center"
            ]
        )
        fallback_users = len(
            [u for u in self.user_cache.values() if u.get("source") == "fallback"]
        )

        return {
            "total_users": total_users,
            "iam_users": iam_users,
            "fallback_users": fallback_users,
            "identity_store_connected": self.identity_store_client is not None
            and self.identity_store_id is not None,
        }


def create_user_mapping_interface():
    """IAM Identity Center 사용자 조회 인터페이스"""
    st.subheader("👤 IAM Identity Center 사용자 조회")

    # 매퍼 초기화
    if "user_mapper" not in st.session_state:
        st.session_state.user_mapper = IAMIdentityCenterMapper()

    mapper = st.session_state.user_mapper

    # 연결 상태 표시
    stats = mapper.get_cache_stats()

    col1, col2, col3 = st.columns(3)

    with col1:
        connection_status = (
            "🟢 연결됨" if stats["identity_store_connected"] else "🔴 미연결"
        )
        st.metric("Identity Center", connection_status)

    with col2:
        st.metric("IAM 사용자", stats["iam_users"])

    with col3:
        st.metric("캐시된 사용자", stats["total_users"])

    # Identity Store ID 표시
    if mapper.identity_store_id:
        st.info(f"**Identity Store ID**: {mapper.identity_store_id}")

    # 모든 사용자 조회
    if st.button("🔍 Identity Center 사용자 전체 조회"):
        with st.spinner("IAM Identity Center에서 사용자 목록 조회 중..."):
            all_users = mapper.list_all_users()

            if all_users:
                st.success(f"✅ {len(all_users)}명의 사용자를 찾았습니다.")

                # 사용자 목록 표시
                import pandas as pd

                users_df = pd.DataFrame(all_users)

                display_columns = [
                    "username",
                    "display_name",
                    "email",
                    "first_name",
                    "last_name",
                ]
                st.dataframe(
                    users_df[display_columns],
                    use_container_width=True,
                    column_config={
                        "username": "사용자명",
                        "display_name": "표시명",
                        "email": "이메일",
                        "first_name": "이름",
                        "last_name": "성",
                    },
                )
            else:
                st.warning("⚠️ 사용자를 찾을 수 없습니다.")

    # 특정 사용자 검색
    with st.expander("🔍 특정 사용자 검색"):
        search_username = st.text_input(
            "사용자명으로 검색", placeholder="bedrock_user1"
        )

        if st.button("사용자 검색"):
            if search_username:
                with st.spinner(f"'{search_username}' 사용자 검색 중..."):
                    user_info = mapper.search_user_by_username(search_username)

                    if user_info:
                        st.success("✅ 사용자를 찾았습니다!")
                        st.json(user_info)
                    else:
                        st.warning("⚠️ 해당 사용자를 찾을 수 없습니다.")

    # 캐시된 사용자 목록
    if mapper.user_cache:
        st.subheader("📋 캐시된 사용자 목록")

        cache_data = []
        for user_id, info in mapper.user_cache.items():
            cache_data.append(
                {
                    "UserId": user_id[:12] + "...",
                    "Username": info.get("username", ""),
                    "DisplayName": info.get("display_name", ""),
                    "Email": info.get("email", ""),
                    "Source": info.get("source", ""),
                    "LastUpdated": time.strftime(
                        "%Y-%m-%d %H:%M", time.localtime(info.get("cached_at", 0))
                    ),
                }
            )

        import pandas as pd

        cache_df = pd.DataFrame(cache_data)
        st.dataframe(cache_df, use_container_width=True)

    return mapper


if __name__ == "__main__":
    # 테스트 실행
    mapper = IAMIdentityCenterMapper()

    print("=== IAM Identity Center 사용자 매핑 테스트 ===")
    print(f"Identity Store ID: {mapper.identity_store_id}")
    print(f"연결 상태: {'연결됨' if mapper.identity_store_client else '미연결'}")

    # bedrock_user1 사용자 검색
    print("\n=== bedrock_user1 사용자 검색 ===")
    user_info = mapper.search_user_by_username("bedrock_user1")
    if user_info:
        print(f"사용자 발견: {user_info}")
    else:
        print("사용자를 찾을 수 없습니다.")

    # 캐시 통계
    stats = mapper.get_cache_stats()
    print(f"\n캐시 통계: {stats}")
