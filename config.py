#!/usr/bin/env python3
"""
Kiro 프로젝트 공통 설정
"""

# S3 설정
DEFAULT_REGION = "us-east-1"
BUCKET_NAME = "<YourBucketName>"

# 폴더 경로 설정
S3_USER_ACTIVITY_REPORT_PREFIX = "daily-report/AWSLogs"

# 서비스 설정 ("Kiro" 또는 "QDeveloper")
SUBSCRIPTION_SERVICE_NAME = "QDeveloper"