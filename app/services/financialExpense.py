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

class FinancialExpense:
    
    # Class Variable
    
    domain = { 
        "user_id": "id",
        "year": "지출_연도"
    }
    
    expenseSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가', '재무']
    
    nonMappingColumnKey = ["usable", "savable", "percentageKey"]
    nonMappingColumnValue = ["지출_월", "지출_목표_영역", "지출_목표", "지출_현황", "지출_종류"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    columnType = {
        "id": 'Int64', 
        "지출_목표_영역": 'Int64', 
        "지출_목표": 'Int64', 
        "지출_현황": 'Int64'
    }
    
    expenseType = ["usable", "savable"]
    
    percentageKeyList = ["key", "usable", "savable"]
    
    
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.FinancialExpenseLive = FinancialExpenseLive(cursor)


    def getFinancialExpense(self):
        sqlQuery = """
                    SELECT * FROM pro.LedgerBudgetSpend
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialExpense.columnKeyList)
        return resultDataFrame
    
    
    
    def getExpensePercentages(self):
        sqlQuery = """
                    SELECT * FROM pro.LedgerPercentages
                    """
        
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialExpense.percentageKeyList)
        return resultDataFrame
        

    
    def procFinancialExpense(self, financialExpenseRawData, expensePercentagesRawData, financialExpenseLiveRawData):
        financialExpenseData = pd.DataFrame(columns = FinancialExpense.columnValueList)
            
        # 지출 내역을 가져와서 데이터프레임을 만든다.
        for value in financialExpenseRawData.values:
            defaultRow = dict()
            
            for index in range(len(FinancialExpense.domain)):
                columnKey = FinancialExpense.columnKeyList[index]
                columnValue = FinancialExpense.columnValueList[index]
                defaultRow[columnValue] = value[index]

            percentageKeyIndex = FinancialExpense.columnKeyList.index("percentageKey")        
            percentageKeyList = ast.literal_eval(value[percentageKeyIndex])

            expenseList = list()
            for index in range(len(FinancialExpense.expenseType)):
                typeIndex = FinancialExpense.columnKeyList.index(FinancialExpense.expenseType[index])
                expenseList.append(ast.literal_eval(value[typeIndex]))
            
            # 12개월마다 usable과 savable의 8대 영역별 지출 금액을 구한다.
            for index in range(12):
                defaultRow["지출_월"] = "{}".format(index + 1)
                
                for index2 in range(len(FinancialExpense.expenseType)):
                    # 소멸성 자산을 percentageKey를 통해 처리한다.
                    # 지출 계산 단위을 1만원 단위에서 1원 단위로 바꾼다.
                    expenseValue = expenseList[index2][index] * 10000 
                    expenseName = FinancialExpense.expenseType[index2]
                    defaultRow["지출_종류"] = FinancialExpense.expenseType[index2]

                    if (percentageKeyList[index] > 0):
                        percentageDataFrame = expensePercentagesRawData[["key", expenseName]]
                        percentageSection = percentageDataFrame.loc[percentageDataFrame["key"] == percentageKeyList[index]]
                        
                        if (percentageSection.empty):
                            continue
                            
                        try:
                            percentageSectionList = ast.literal_eval(percentageSection[expenseName].values[0])
                        
                        except:
                            continue

                        for index3 in range(len(FinancialExpense.expenseSection)):
                            row = dict()
                            row = defaultRow.copy()
                            
                            expenseLiveDataFrame = financialExpenseLiveRawData
                            expenseLiveCost = expenseLiveDataFrame.loc[
                                (expenseLiveDataFrame["year"] == int(row["지출_연도"])) &
                                (expenseLiveDataFrame["month"] == int(row["지출_월"])) &
                                (expenseLiveDataFrame["user_id"] == int(row["id"])) &
                                (expenseLiveDataFrame["cat_eight"] == index3) &
                                (expenseLiveDataFrame["useOrSave"] == index2)
                            ]
    
                            if (not expenseLiveCost.empty):
                                row["지출_현황"] = expenseLiveCost["actualCost"].values[0]
                            
                            row["지출_목표_영역"] = FinancialExpense.expenseSection[index3]
                            row["지출_목표"] = expenseValue * percentageSectionList[index3] / 100
                            financialExpenseData = financialExpenseData.append(row, ignore_index = True)

        
        return financialExpenseData
   
    
    def writeFinancialExpense(self):
        financialExpenseRawData = self.getFinancialExpense()
        expensePercentagesRawData = self.getExpensePercentages()
        financialExpenseLiveRawData = self.FinancialExpenseLive.getFinancialExpenseLive()
    
        financialExpenseData = self.procFinancialExpense(financialExpenseRawData, expensePercentagesRawData, financialExpenseLiveRawData)
        financialExpenseData = financialExpenseData.fillna({"지출_종류": ""})
        financialExpenseData = financialExpenseData.astype(FinancialExpense.columnType, errors='ignore')
        return financialExpenseData


class FinancialExpenseLive:
    # Class Variable
    domain = { 
        "user_id": "id", 
        "year": "지출_연도",
        "month": "지출_월",
        "cat_eight": "지출_목표_영역",
        "actualCost": "지출_현황",
        "useOrSave": "지출_종류"
    }
    
    nonMappingColumnKey = list()
    nonMappingColumnValue = list()
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    
    def getFinancialExpenseLive(self):
        sqlQuery = """            
                    SELECT user_id, year, month, cat_eight, useOrSave, SUM(actualCost) as actualCost
                    FROM pro.SpendLedger
                    GROUP BY user_id, year, month, cat_eight, useOrSave;
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialExpenseLive.columnKeyList)
        return resultDataFrame


# + active=""
# # Only for test
#
# cursor = connectMySQL()
# financialExpenseInstance = FinancialExpense(cursor)
# financialExpenseData = financialExpenseInstance.writeFinancialExpense()
# display(financialExpenseData)
# -


