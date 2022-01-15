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
import ast
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


# +
# pd.set_option('display.max_rows', None)
# -

class FinancialAssetGoal:
    
    # Class Variable
    
    domain = { 
        "user_id": "id",
        "endYear": "자산_목표_연도"
    }
    
    assetsGoalSection = ['부동산 자산', '사용 자산', '현금성 자산', '금융 자산', '보장 자산']
    
    nonMappingColumnKey = ["assetsDivisionArray"]
    nonMappingColumnValue = ["자산_목표_종류", "자산_목표"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    columnType = {
        "id": 'Int64', 
        "자산_목표_종류": 'Int64', 
        "자산_목표": 'Int64', 
        "자산_목표_현황": 'Int64'
    }
    
    

    def __init__(self, cursor):
        self.cursor = cursor


        
    def getFinancialAssetGoal(self):
        sqlQuery = """
                    SELECT * FROM pro.GoalFinance
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialAssetGoal.columnKeyList)
        return resultDataFrame
    
    
    
    def procFinancialAssetGoal(self, financialAssetGoalRawData):
        financialAssetGoalData = pd.DataFrame(columns = FinancialAssetGoal.columnValueList)
            
        # 재무 목표에서 자산에 해당되는 데이터를 가져와서 데이터프레임을 만든다.
        for value in financialAssetGoalRawData.values:
            defaultRow = dict()
            
            for index in range(len(FinancialAssetGoal.domain)):
                columnKey = FinancialAssetGoal.columnKeyList[index]
                columnValue = FinancialAssetGoal.columnValueList[index]
                defaultRow[columnValue] = value[index]

                
            assetsDivisionIndex = FinancialAssetGoal.columnKeyList.index("assetsDivisionArray")
            
            try:
                assetsDivisionList = json.loads(value[assetsDivisionIndex])
                if (not isinstance(assetsDivisionList, list)):
                    raise
            except:
                continue
            
            for index in range(len(assetsDivisionList)):
                row = defaultRow.copy()
                row["자산_목표_종류"] = index
                row["자산_목표"] = assetsDivisionList[index]
                
                financialAssetGoalData = financialAssetGoalData.append(row, ignore_index = True)
        
        return financialAssetGoalData
   
    
    def writeFinancialAssetGoal(self):
        financialAssetGoalRawData = self.getFinancialAssetGoal()
    
        financialAssetGoalData = self.procFinancialAssetGoal(financialAssetGoalRawData)
        return financialAssetGoalData

# +
# Only for test

cursor = connectMySQL()
financialAssetGoalInstance = FinancialAssetGoal(cursor)
financialAssetGoalData = financialAssetGoalInstance.writeFinancialAssetGoal()
display(financialAssetGoalData)
# -


