# -*- coding: utf-8 -*-
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

# 외부 모듈 import
import sys
import os
import requests
import mysql.connector
import base64
import json
import logging
import time
import pymysql
import boto3
import json
import csv
import numpy as np
import pandas as pd
import import_ipynb
from IPython.display import display

# +
# 다른 디렉토리에 있는 모듈 경로 가져오기
module_path = list()

module_path.append(os.path.abspath(os.getcwd() + '/services'))
module_path.append(os.path.abspath(os.getcwd() + '/utils'))

for path in module_path:
    if path not in sys.path:
        sys.path.append(path)
# -

# 서비스 계층 class import
from services.user import User
from services.lifeScore import LifeScore
from services.financeScore import FinanceScore
from services.visionGoal import VisionGoal
from services.scheduling import Scheduling
from services.visionKeyword import VisionKeyword
from services.goalFinance import GoalFinance
from services.financialExpense import FinancialExpense
from services.financialIncome import FinancialIncome
from services.financialAssetGoal import FinancialAssetGoal
from services.financialAsset import FinancialAsset

# 유틸리티 계층 class import
from utils.connectDB import connectMySQL
from utils.writeCSV import writeToCSV
from utils.syncWithS3Bucket import SyncWithS3Bucket

try:
    cursor = connectMySQL()
except Exception as e:
    display(e)
    sys.exit()

pd.set_option('display.max_rows', 50)

# lifeScore 데이터 전처리 후 csv 파일 작성 
lifeScoreInstance = LifeScore(cursor)
lifeScoreData = lifeScoreInstance.writeLifeScore()
display(lifeScoreData)
writeToCSV(lifeScoreData, "life_score", "life_score")

# + active=""
# # tenYearsGoal 데이터 전처리 후 csv 파일 작성
# tenYearsGoalInstance = TenYearsGoal(cursor)
# tenYearsGoalData = tenYearsGoalInstance.writeTenYearsGoal()
# display(tenYearsGoalData)
# writeToCSV(tenYearsGoalData, "ten_years_goal", "ten_years_goal")
# -

# financeScore 데이터 전처리 후 csv 파일 작성
financeScoreInstance = FinanceScore(cursor)
financeScoreData = financeScoreInstance.writeFinanceScore()
display(financeScoreData)
writeToCSV(financeScoreData, "financial_score", "financial_score")

# visionGoal 데이터 전처리 후 csv 파일 작성
visionGoalInstance = VisionGoal(cursor)
visionGoalData = visionGoalInstance.writeVisionGoal()
display(visionGoalData)
writeToCSV(visionGoalData, "vision_goal", "vision_goal")

# scheduling 데이터 전처리 후 csv 파일 작성
schedulingInstance = Scheduling(cursor)
schedulingData = schedulingInstance.writeScheduling()
display(schedulingData)
writeToCSV(schedulingData, "scheduling", "scheduling")

# user 데이터 전처리 후 csv 파일 생성
userInstance = User(cursor)
userData = userInstance.writeUser()
display(userData)
writeToCSV(userData, "user", "user")

# visionKeyword 데이터 전처리 후 csv 파일 생성
visionKeywordInstance = VisionKeyword(cursor)
visionKeywordData = visionKeywordInstance.writeVisionKeyword()
display(visionKeywordData)
writeToCSV(visionKeywordData, "vision_keyword", "vision_keyword")

# goalFinance 데이터 전처리 후 csv 파일 생성
goalFinanceInstance = GoalFinance(cursor)
goalFinanceData, goalFinancialIdData = goalFinanceInstance.writeGoalFinance()
display(goalFinanceData, goalFinancialIdData)
writeToCSV(goalFinanceData, "goal_finance", "goal_finance")
writeToCSV(goalFinancialIdData, "goal_financial_id", "goal_financial_id")

# financialExpense 데이터 전처리 후 csv 파일 생성
financialExpenseInstance = FinancialExpense(cursor)
financialExpenseData = financialExpenseInstance.writeFinancialExpense()
display(financialExpenseData)
writeToCSV(financialExpenseData, "financial_exp", "financial_exp")

# financialIncome 데이터 전처리 후 csv 파일 생성
financialIncomeInstance = FinancialIncome(cursor)
financialIncomeData, incomeIdData = financialIncomeInstance.writeFinancialIncome()
display(financialIncomeData, incomeIdData)
writeToCSV(financialIncomeData, "financial_inc", "financial_inc")
writeToCSV(incomeIdData, "financial_inc_id", "financial_inc_id")

# financialAssetGoal 데이터 전처리 후 csv 파일 생성
financialAssetGoalInstance = FinancialAssetGoal(cursor)
financialAssetGoalData = financialAssetGoalInstance.writeFinancialAssetGoal()
display(financialAssetGoalData)
writeToCSV(financialAssetGoalData, "financial_asset_goal", "financial_asset_goal")

# financialAsset 데이터 전처리 후 csv 파일 생성
financialAssetInstance = FinancialAsset(cursor)
financialAssetData, assetIdData = financialAssetInstance.writeFinancialAsset()
display(financialAssetData, assetIdData)
writeToCSV(financialAssetData, "financial_asset", "financial_asset")
writeToCSV(assetIdData, "financial_asset_id", "financial_asset_id")

syncWithS3BucketInstance = SyncWithS3Bucket()
syncWithS3BucketInstance.uploadFile()
