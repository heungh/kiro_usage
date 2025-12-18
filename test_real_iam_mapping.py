#!/usr/bin/env python3
"""
실제 IAM Identity Center 매핑 테스트
"""

import boto3
from iam_identity_center_mapper import IAMIdentityCenterMapper

def test_real_mapping():
    """실제 IAM Identity Center 연동 테스트"""
    
    # 실제 Identity Store ID 설정
    identity_store_id = "d-9067f33d3d"
    
    # 직접 boto3 클라이언트로 테스트
    identity_store = boto3.client('identitystore', region_name='us-east-1')
    
    print("=== 실제 IAM Identity Center 연동 테스트 ===")
    print(f"Identity Store ID: {identity_store_id}")
    
    # 실제 UserId로 사용자 정보 조회
    test_user_id = "1458e458-7041-709b-8e32-f0a74754c127"
    
    try:
        response = identity_store.describe_user(
            IdentityStoreId=identity_store_id,
            UserId=test_user_id
        )
        
        print(f"\n✅ 사용자 정보 조회 성공!")
        print(f"UserName: {response.get('UserName')}")
        print(f"DisplayName: {response.get('DisplayName')}")
        print(f"Email: {response.get('Emails', [{}])[0].get('Value', '')}")
        print(f"FirstName: {response.get('Name', {}).get('GivenName', '')}")
        print(f"LastName: {response.get('Name', {}).get('FamilyName', '')}")
        
        return True
        
    except Exception as e:
        print(f"❌ 사용자 조회 실패: {e}")
        return False

def test_mapper_class():
    """매퍼 클래스 테스트"""
    print("\n=== 매퍼 클래스 테스트 ===")
    
    # 매퍼 초기화 (실제 리전 지정)
    import os
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    
    mapper = IAMIdentityCenterMapper()
    
    # 강제로 올바른 Identity Store ID 설정
    mapper.identity_store_id = "d-9067f33d3d"
    mapper.identity_store_client = boto3.client('identitystore', region_name='us-east-1')
    
    print(f"Identity Store ID: {mapper.identity_store_id}")
    print(f"클라이언트 연결: {'성공' if mapper.identity_store_client else '실패'}")
    
    # 실제 사용자 정보 조회
    test_user_id = "1458e458-7041-709b-8e32-f0a74754c127"
    
    user_info = mapper.get_user_info(test_user_id)
    print(f"\n사용자 정보: {user_info}")
    
    print(f"표시명: {mapper.get_display_name(test_user_id)}")
    print(f"이메일: {mapper.get_email(test_user_id)}")
    
    # 모든 사용자 조회
    print(f"\n=== 전체 사용자 목록 ===")
    all_users = mapper.list_all_users()
    for user in all_users:
        print(f"- {user['display_name']} ({user['username']}) - {user['email']}")

if __name__ == "__main__":
    # 직접 API 테스트
    success = test_real_mapping()
    
    if success:
        # 매퍼 클래스 테스트
        test_mapper_class()
    else:
        print("직접 API 테스트 실패")
