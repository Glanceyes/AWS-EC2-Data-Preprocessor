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
    domain = { 
                "user_id": "id", 
                "level": "목표단위", 
                "division": "목표영역",
                "content": "목표내용",
                "finishYear": "목표연도",
                "isNeedMoney": "목표금액설정",
                "cost": "목표금액",
                "useOrSave": "목표금액성격"
            }
    
    goalSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가']
    goalLevel = ['최종목표', '대목표', '중목표', '소목표']
    
    nonMappingColumnKey = []
    nonMappingColumnValue = []
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    keywordExtractorInstance = KeywordExtractor()
    
    changeGoalCostAttribute = {-1 : np.nan, 1 : 0, 2 : 1}
    
    def __init__(self, cursor):
        self.cursor = cursor
  


    def getGoal(self):
        # 사용자별 재무 외의 목표와 생일을 가져온다.
        sqlQuery = """
                    SELECT * FROM pro.GoalLivingHierarchy
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = Goal.columnKeyList)
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
                
                elif (columnKey == "useOrSave"):
                    if (value[index] == np.nan):
                        continue
                        
                    if (int(value[index]) % 2 == 0):
                        row[columnValue] = "소멸형"
                    else:
                        row[columnValue] = "저축형"
                
                # 목표단위 컬럼은 값에 따라 목표 단계명에 매칭되는 문자열을 찾는다.
                elif (columnKey == "level"):
                    try: 
                        row[columnValue] = Goal.goalLevel[int(value[index]) - 1]
                    except:
                        continue
                else:
                    row[columnValue] = value[index]
                
            goalData = goalData.append(row, ignore_index = True)
        
        
        return goalData
   
    
    def writeGoal(self):
        resultData = self.getGoal()
        goalData = self.procGoal(resultData)
        goalData = goalData.replace({"목표금액": -1}, {"목표금액": np.nan})
        goalData = goalData.replace({"목표금액성격": Goal.changeGoalCostAttribute})
        
        return goalData


class TenYearsGoal:
    
    # Class Variable
    domain = { 
                "user_id": "id", 
                "division": "목표영역",
                "content": "목표내용",
                "year": "목표연도",
                "isNeedMoney": "목표금액설정",
                "cost": "목표금액",
                "useOrSave": "목표금액성격"
            }
    
    nonMappingColumnKey = []
    nonMappingColumnValue = []
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue

    goalSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가']
    
    keywordExtractorInstance = KeywordExtractor()
    
    def __init__(self, cursor):
        self.cursor = cursor
        
    def getTenYearsGoal(self):
        # 사용자별 개수 제한 없이 가져온다.
        sqlQuery = """
                    SELECT *
                    FROM pro.Goal10Year
                    WHERE user_id IS NOT NULL;
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = TenYearsGoal.columnKeyList)
        return resultDataFrame
    
    def procTenYearsGoal(self, resultData):
        tenYearsGoalData = pd.DataFrame(columns = TenYearsGoal.columnValueList)
        
        for value in resultData.values:
            row = dict()
            
            for index in range(len(TenYearsGoal.columnKeyList)):
                columnKey = TenYearsGoal.columnKeyList[index]
                columnValue = TenYearsGoal.columnValueList[index]
                
                # content 컬럼의 데이터를 NLP(자연어 처리)하여 10년 목표키워드를 구한다.
                if (columnKey == "content"):
                    try:
                        extractKeyword = TenYearsGoal.keywordExtractorInstance.extractKeyword
                        row[columnValue] = extractKeyword(value[index])
                    except:
                        continue
                elif (columnKey == "division"):
                    row[columnValue] = TenYearsGoal.goalSection[int(value[index])]
                    
                elif (columnKey == "useOrSave"):
                    if (np.isnan(value[index])):
                        continue
                        
                    if (int(value[index]) % 2 == 0):
                        row[columnValue] = "소멸형"
                    else:
                        row[columnValue] = "저축형"
                else:
                    row[columnValue] = value[index]
                    
            tenYearsGoalData = tenYearsGoalData.append(row, ignore_index = True)
        return tenYearsGoalData
    
    def writeTenYearsGoal(self):
        resultData = self.getTenYearsGoal()
        tenYearsGoalData = self.procTenYearsGoal(resultData)
        return tenYearsGoalData


class VisionGoal:
    columnType = {
        "id": 'Int64', 
        "목표단위": str, 
        "목표영역": str, 
        "목표내용": str, 
        "목표연도": 'Int64', 
        "목표금액설정": 'Int64', 
        "목표금액": 'Int64',
        "목표금액성격": str
    }
    
    stringColumnList = ["목표단위", "목표내용"]
    
    def __init__(self, cursor):
        self.goalInstance = Goal(cursor)
        self.tenYearsGoalInstance = TenYearsGoal(cursor)
        
    def writeVisionGoal(self):
        goalInstance = self.goalInstance
        tenYearsGoalInstance = self.tenYearsGoalInstance
        
        goalDataFrame = goalInstance.writeGoal()
        tenYearsGoalDataFrame = tenYearsGoalInstance.writeTenYearsGoal()
        
        visionGoalData = pd.concat([goalDataFrame, tenYearsGoalDataFrame], ignore_index = True)
        visionGoalData[VisionGoal.stringColumnList] = visionGoalData[VisionGoal.stringColumnList].replace(np.nan, '', regex=True)
        visionGoalData = visionGoalData.fillna({"목표내용": "", "목표금액성격": "", "목표단위": ""})
        visionGoalData = visionGoalData.astype(VisionGoal.columnType)
        visionGoalData.loc[visionGoalData["목표금액설정"] == 0, "목표금액"] = np.nan
        
        return visionGoalData

# + active=""
# # Only for test
#
# cursor = connectMySQL()
# visionGoalInstance = VisionGoal(cursor)
# visionGoalData = visionGoalInstance.writeVisionGoal()
# display(visionGoalData)
# -


