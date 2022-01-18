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

# +
# 다른 디렉토리에 있는 모듈 import
module_path = os.path.abspath(os.path.join('..'))

if module_path not in sys.path:
    sys.path.append(module_path)

from utils.connectDB import connectMySQL
from utils.keywordExtractor import *


# -

class TenYearsGoal:
    
    # Class Variable
    domain = { "user_id": "id", 
                "year": "십년_목표연도", 
                "division": "십년_목표영역",
                "content": "십년_목표키워드",
            }
    
    columnKeyList = list(domain.keys())
    columnValueList = list(domain.values())
    
    columnType = {
        "id": 'Int64', 
        "십년_목표연도": 'Int64'
    }
    
    goalSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가', '재무']
    
    keywordExtractorInstance = KeywordExtractor()
    
    def __init__(self, cursor):
        self.cursor = cursor
        
    def getTenYearsGoal(self):
        # 사용자별 개수 제한 없이 가져온다.
        sqlQuery = """
                    SELECT user_id, year, division, content
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
                        row[columnValue] = TenYearsGoal.keywordExtractorInstance.extractKeyword(value[index])
                    except:
                        continue
                elif (columnKey == "division"):
                    row[columnValue] = TenYearsGoal.goalSection[int(value[index])]
                else:
                    row[columnValue] = value[index]
            
            tenYearsGoalData = tenYearsGoalData.append(row, ignore_index = True)
        
        tenYearsGoalData = tenYearsGoalData.astype(TenYearsGoal.columnType)
        return tenYearsGoalData
    
    def writeTenYearsGoal(self):
        resultData = self.getTenYearsGoal()
        tenYearsGoalData = self.procTenYearsGoal(resultData)
        return tenYearsGoalData

# + active=""
# # Only for test
# cursor = connectMySQL()
# tenYearsGoalInstance = TenYearsGoal(cursor)
# tenYearsGoalData = tenYearsGoalInstance.writeTenYearsGoal()
# display(tenYearsGoalData)
