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


# -

class FinancialAsset:
    
    # Class Variable
    domain = { "user_id": "id", 
                "kinds_index": "자산_목표_종류", 
                "targetCost": "자산_목표",
                "actualCost": "지출_현황",
                "useOrSave": "지출_종류"
            }
    
    expenseSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가', '재무']
    
    columnKeyList = list(domain.keys())
    columnValueList = list(domain.values())
    
    expenseType = {0: "소멸성", 1: "지출성"}
    
    columnType = {
        "id": 'Int64', 
        "지출_목표_영역": 'Int64', 
        "지출_목표": 'Int64', 
        "지출_현황": 'Int64'
    }
    
    def __init__(self, cursor):
        self.cursor = cursor
  


    def getFinancialExpense(self):
        sqlQuery = """
                    SELECT * FROM pro.SpendLedger
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialExpense.columnKeyList)
        return resultDataFrame
    
        
    
    def procFinancialExpense(self, resultData):
        financialExpenseData = pd.DataFrame(columns = FinancialExpense.columnValueList)
            
        # 지출 내역을 가져와서 데이터프레임을 만든다.
        for value in resultData.values:
            row = dict()
            
            for index in range(len(FinancialExpense.domain)):
                columnKey = FinancialExpense.columnKeyList[index]
                columnValue = FinancialExpense.columnValueList[index]
                
                try:
                    # cat_eight 컬럼 값에 따라서 지출_목표_영역 컬럼 값이 정해진다. 
                    if (columnKey == "cat_eight"):
                        row[columnValue] = FinancialExpense.expenseSection[int(value[index])]

                    # 지출이 소멸성인지 저축성인지에 따라서 지출_종류 컬럼 값이 정해진다.
                    elif (columnKey == "useOrSave"):
                        row[columnValue] = FinancialExpense.expenseType.get(int(value[index]), None)

                    else:
                        row[columnValue] = value[index]
                except:
                    continue
                
            financialExpenseData = financialExpenseData.append(row, ignore_index = True)
        
        
        return financialExpenseData
   
    
    def writeFinancialExpense(self):
        resultData = self.getFinancialExpense()
        financialExpenseData = self.procFinancialExpense(resultData)
        financialExpenseData = financialExpenseData.fillna({"지출_종류": ""})
        financialExpenseData = financialExpenseData.astype(FinancialExpense.columnType, errors='ignore')
        return financialExpenseData

# + active=""
# # Only for test
#
# cursor = connectMySQL()
# financialExpenseInstance = FinancialExpense(cursor)
# financialExpenseInstance.writeFinancialExpense()
# -


