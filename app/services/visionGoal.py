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
import mysql.connector
import base64
import json
import pymysql
import json
import csv
import import_ipynb
import pandas as pd
import datetime as dt
from dateutil.parser import parse

# +
# 다른 디렉토리에 있는 모듈 import
module_path = os.path.abspath(os.path.join('..'))

if module_path not in sys.path:
    sys.path.append(module_path)
    
from utils.connectDB import connectMySQL
from utils.keywordExtractor import *


# -

class VisionGoal:
    
    # Class Variable
    domain = { "user_id": "id", 
                "level": "목표단위", 
                "division": "목표영역",
                "content": "목표내용",
                "finishYear": "목표나이",
                "isNeedMoney": "목표금액설정",
                "cost": "목표금액",
                "useOrSave": "목표금액성격"
            }
    
    goalSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가']
    
    columnKeyList = list(domain.keys())
    columnValueList = list(domain.values())
    
    keywordExtractorInstance = KeywordExtractor()
    
    changeGoalCostAttribute = {-1 : "", 1 : 0, 2 : 1}

    def __init__(self, cursor):
        self.cursor = cursor
        
    def getVisionGoal(self):
        # 사용자별 재무 외의 목표와 생일을 가져온다.
        sqlQuery = """
                    SELECT * FROM pro.GoalLivingHierarchy AS T1 
                    LEFT JOIN (
                        SELECT id, birthday FROM pro.users
                    ) AS T2
                    ON T1.user_id = T2.id
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = VisionGoal.columnKeyList + ['birthday'])
        return resultDataFrame
    
    def getVisionFinanceGoal(self):
        # 사용자별 재무 목표와 생일을 가져온다.
        sqlQuery = """
                    SELECT * FROM pro.GoalFinance AS T1 
                    LEFT JOIN (
                        SELECT id, birthday FROM pro.users
                    ) AS T2
                    ON T1.user_id = T2.id
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = [
            'user_id', 'level', 'endYear', 'spend', 'earn', 'assets', 'spendSavable', 'birthday'
        ])
        return resultDataFrame
    
    def updateVisionGoal(self, resultData):
        visionGoalData = pd.DataFrame(columns = VisionGoal.columnValueList)
            
        # 재무 외의 목표를 읽어서 데이터 프레임으로 만든다.
        for value in resultData.values:
            row = dict()
            
            for index in range(len(VisionGoal.columnValueList)):
                columnKey = VisionGoal.columnKeyList[index]
                columnValue = VisionGoal.columnValueList[index]
                
                # 목표내용 컬럼은 자연어 처리를 통해 하나의 키워드를 추출해야 한다.
                if (columnKey == "content"):
                    # content 컬럼의 데이터를 NLP(자연어 처리)하여 10년 목표키워드를 구한다.
                    try:
                        row[columnValue] = VisionGoal.keywordExtractorInstance.extractKeyword(value[index])
                    except:
                        continue
                
                # 목표영역 컬럼은 값에 따라 영역 이름에 관한 문자열을 mapping 해야 한다.
                elif (columnKey == "division"):
                    row[columnValue] = VisionGoal.goalSection[value[index]]
                
                # 목표나이 컬럼은 값에서 사용자의 생일 연도를 뺀다. (연 나이 기준)
                elif (columnKey == "finishYear"):
                    # 사용자의 생일은 마지막 컬럼에 존재한다.
                    row[columnValue] = int(value[index]) - int(parse(value[len(VisionGoal.columnValueList)]).year) + 1
                
                # 목표단위 컬럼은 값에서 1을 뺀다.
                elif (columnKey == "level"):
                    row[columnValue] = int(value[index]) - 1
                
                else:
                    row[columnValue] = value[index]
                
            visionGoalData = visionGoalData.append(row, ignore_index = True)
        
        visionGoalData = visionGoalData.fillna({"목표내용": ""})
        visionGoalData = visionGoalData.replace({"목표금액": -1}, {"목표금액": ""})
        visionGoalData = visionGoalData.replace({"목표금액성격": VisionGoal.changeGoalCostAttribute})
        return visionGoalData
    
    def doVisionGoal(self):
        resultData = self.getVisionGoal()
        VisionGoalData = self.updateVisionGoal(resultData)
        print(VisionGoalData)
        return VisionGoalData

# + active=""
# # Only for test
# cursor = connectMySQL()
# visionGoalInstance = VisionGoal(cursor)
# resultDataFrame = visionGoalInstance.getVisionGoal()
# visionGoalData = visionGoalInstance.updateVisionGoal(resultDataFrame)
# print(visionGoalData)
# -


