# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.6
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# +
import sys
import boto3
import os

ENDPOINT="lifeplan.cossllctaabu.ap-northeast-2.rds.amazonaws.com"
PORT="3306"
USER="ubuntu"
REGION="ap-northeast-2"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-2'

#gets the credentials from .aws/credentials
session = boto3.Session()
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)    
print(token)
# -




