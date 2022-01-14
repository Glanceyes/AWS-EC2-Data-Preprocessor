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
import numpy as np
import pandas as pd
import datetime as dt
from dateutil.parser import parse
from IPython.display import display

# +
# 다른 디렉토리에 있는 모듈 import
module_path = os.path.abspath(os.path.join('..'))

if module_path not in sys.path:
    sys.path.append(module_path)
    
from utils.connectDB import connectMySQL
from utils.keywordExtractor import *


# -

class Goal:
    
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
    
    nonMappingColumnKey = ["birthday"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values())
    
    keywordExtractorInstance = KeywordExtractor()
    
    changeGoalCostAttribute = {-1 : np.nan, 1 : 0, 2 : 1}

    
    
    def __init__(self, cursor):
        self.cursor = cursor
  


    def getGoal(self):
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
        resultDataFrame = pd.DataFrame(result, columns = Goal.columnKeyList + ['birthday'])
        return resultDataFrame
    
        
    
    def procGoal(self, resultData):
        goalData = pd.DataFrame(columns = Goal.columnValueList)
            
        # 재무 외의 목표를 읽어서 데이터 프레임으로 만든다.
        for value in resultData.values:
            row = dict()
            
            for index in range(len(Goal.domain)):
                columnKey = Goal.columnKeyList[index]
                columnValue = Goal.columnValueList[index]
                
                # 목표내용 컬럼은 자연어 처리를 통해 하나의 키워드를 추출해야 한다.
                if (columnKey == "content"):
                    # content 컬럼의 데이터를 NLP(자연어 처리)하여 10년 목표키워드를 구한다.
                    try:
                        extractKeyword = Goal.keywordExtractorInstance.extractKeyword
                        row[columnValue] = extractKeyword(value[index])
                    except:
                        continue
                
                # 목표영역 컬럼은 값에 따라 영역 이름에 관한 문자열을 mapping 해야 한다.
                elif (columnKey == "division"):
                    row[columnValue] = Goal.goalSection[value[index]]
                
                # 목표나이 컬럼은 값에서 사용자의 생일 연도를 뺀다. (연 나이 기준)
                elif (columnKey == "finishYear"):
                    # 사용자의 생일은 마지막 컬럼에 존재한다.
                    row[columnValue] = int(value[index]) - int(parse(value[Goal.columnKeyList.index("birthday")]).year)
                
                # 목표단위 컬럼은 값에서 1을 뺀다.
                elif (columnKey == "level"):
                    row[columnValue] = int(value[index]) - 1
                
                else:
                    row[columnValue] = value[index]
                
            goalData = goalData.append(row, ignore_index = True)
        
        
        return goalData
   
    
    def writeGoal(self):
        resultData = self.getGoal()
        goalData = self.procGoal(resultData)
        goalData = goalData.fillna({"목표내용": ""})
        goalData = goalData.replace({"목표금액": -1}, {"목표금액": np.nan})
        goalData = goalData.replace({"목표금액성격": Goal.changeGoalCostAttribute})
        # print(goalData)
        return goalData


class GoalFinance():

    # Class Variable
    domain = { "user_id": "id", 
                "level": "목표단위",
                "endYear": "목표나이",
            }
    
    nonMappingColumnKey = ["spend", "earn", "assets", "spendSavable", "birthday"]
    nonMappingColumnValue = ["목표영역", "목표내용", "목표금액설정", "목표금액", "목표금액성격"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    
    def __init__(self, cursor):
        self.cursor = cursor
        
        
    def getGoalFinance(self):
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
        resultDataFrame = pd.DataFrame(result, columns = GoalFinance.columnKeyList)
        return resultDataFrame
    

    def procGoalFinance(self, resultData):
        goalFinanceData = pd.DataFrame(columns = GoalFinance.columnValueList)
        
        # 재무 목표를 읽어서 데이터 프레임으로 만든다.
        for value in resultData.values:
            defaultRow = dict()
            
            defaultRow["목표영역"] = "재무"
            defaultRow["목표금액설정"] = 1
            
            for index in range(len(GoalFinance.domain)):
                columnKey = GoalFinance.columnKeyList[index]
                columnValue = GoalFinance.columnValueList[index]
                
                # 목표나이 컬럼은 값에서 사용자의 생일 연도를 뺀다. (연 나이 기준)
                if (columnKey == "endYear"):
                    defaultRow[columnValue] = int(value[index]) - int(parse(value[GoalFinance.columnKeyList.index("birthday")]).year)
                else:
                    defaultRow[columnValue] = value[index]
        
            
            for index in range(len(GoalFinance.domain), len(GoalFinance.columnKeyList)):
                columnKey = GoalFinance.columnKeyList[index]
                
                if (columnKey == "birthday"):
                    continue
                
                row = defaultRow.copy()
                row["목표금액설정"] = 1
                row["목표금액"] = value[index]
                
                # spend 컬럼은 소멸성 지출이므로 목표금액성격 컬럼의 값을 0으로 설정한다.
                if (columnKey == "spend"):
                    row["목표금액성격"] = 0
                elif (columnKey == "spendSavable"):
                    row["목표금액성격"] = 1
                    
                goalFinanceData = goalFinanceData.append(row, ignore_index = True)
        
        return goalFinanceData
    
    
    def writeGoalFinance(self):
        resultData = self.getGoalFinance()
        goalFinanceData = self.procGoalFinance(resultData)
        goalFinanceData = goalFinanceData.fillna({"목표내용": ""})
        return goalFinanceData


class VisionGoal:
    columnType = {
        "id": 'Int64', 
        "목표단위": 'Int64', 
        "목표영역": str, 
        "목표내용": str, 
        "목표나이": 'Int64', 
        "목표금액설정": 'Int64', 
        "목표금액": 'Int64',
        "목표금액성격": 'Int64'
    }
    
    def __init__(self, cursor):
        self.goalInstance = Goal(cursor)
        self.goalFinanceInstance = GoalFinance(cursor)
        
    def writeVisionGoal(self):
        goalInstance = self.goalInstance
        goalFinanceInstance = self.goalFinanceInstance
        goalDataFrame = goalInstance.writeGoal()
        goalFinanceDataFrame = goalFinanceInstance.writeGoalFinance()
        visionGoalData = pd.concat([goalDataFrame, goalFinanceDataFrame], ignore_index = True)
        visionGoalData = visionGoalData.astype(VisionGoal.columnType)
        return visionGoalData

# + active=""
# # Only for test
#
# cursor = connectMySQL()
# visionGoalInstance = VisionGoal(cursor)
# visionGoalInstance.writeVisionGoal()
# -


