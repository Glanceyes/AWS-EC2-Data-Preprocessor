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

# 서비스 계층 class import
from services.user import User
from services.lifeScore import LifeScore
from services.tenYearsGoal import TenYearsGoal
from services.financeScore import FinanceScore
from services.visionGoal import VisionGoal
from services.scheduling import Scheduling
from services.visionKeyword import VisionKeyword
from services.financialExpense import FinancialExpense
from services.financialIncome import FinancialIncome

# 유틸리티 계층 class import
from utils.connectDB import connectMySQL
from utils.writeCSV import writeToCSV

cursor = connectMySQL()

pd.set_option('display.max_rows', 50)

# lifeScore 데이터 전처리 후 csv 파일 작성 
lifeScoreInstance = LifeScore(cursor)
lifeScoreData = lifeScoreInstance.writeLifeScore()
display(lifeScoreData)
writeToCSV(lifeScoreData, "life_score", "life_score")

# tenYearsGoal 데이터 전처리 후 csv 파일 작성
tenYearsGoalInstance = TenYearsGoal(cursor)
tenYearsGoalData = tenYearsGoalInstance.writeTenYearsGoal()
display(tenYearsGoalData)
writeToCSV(tenYearsGoalData, "ten_years_goal", "ten_years_goal")

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

# financialExpense 데이터 전처리 후 csv 파일 생성
financialExpenseInstance = FinancialExpense(cursor)
financialExpenseData = financialExpenseInstance.writeFinancialExpense()
display(financialExpenseData)
writeToCSV(financialExpenseData, "financial_exp", "financial_exp")

# financialIncome 데이터 전처리 후 csv 파일 생성
financialIncomeInstance = FinancialIncome(cursor)
financialIncomeData = financialIncomeInstance.writeFinancialIncome()
display(financialIncomeData)
writeToCSV(financialIncomeData, "financial_inc", "financial_inc")
