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

class FinancialIncome:
    
    # Class Variable
    
    domain = { 
        "user_id": "id",
        "year": "소득_연도"
    }
    
    incomeSection = ['연금소득', '근로/사업소득', '자산소득', '이전소득', '기타소득', '증여상속']
    
    nonMappingColumnKey = ["isDivisionSet", "earnDivisionByMonth"]
    nonMappingColumnValue = ["소득_월", "소득_목표_종류", "소득_목표", "소득_목표_현황"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    columnType = {
        "id": 'Int64', 
        "소득_목표_종류": 'Int64', 
        "소득_목표": 'Int64', 
        "소득_목표_현황": 'Int64'
    }
    
    

    def __init__(self, cursor):
        self.cursor = cursor
        self.financialIncomeLive = FinancialIncomeLive(cursor)


        
    def getFinancialIncome(self):
        sqlQuery = """
                    SELECT * FROM pro.LedgerBudgetEarn
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialIncome.columnKeyList)
        return resultDataFrame
    
    
    
    def procFinancialIncome(self, financialIncomeRawData, financialIncomeLiveRawData):
        financialIncomeData = pd.DataFrame(columns = FinancialIncome.columnValueList)
            
        # 소득 내역을 가져와서 데이터프레임을 만든다.
        for value in financialIncomeRawData.values:
            defaultRow = dict()
            
            for index in range(len(FinancialIncome.domain)):
                columnKey = FinancialIncome.columnKeyList[index]
                columnValue = FinancialIncome.columnValueList[index]
                defaultRow[columnValue] = value[index]

                
            divisionSetIndex = FinancialIncome.columnKeyList.index("isDivisionSet")
            earnMonthIndex = FinancialIncome.columnKeyList.index("earnDivisionByMonth")
            
            
            try:
                divisionSetList = json.loads(value[divisionSetIndex])
                earnMonthList = json.loads(value[earnMonthIndex])
            except ValueError as e:
                continue
            
            for index in range(len(divisionSetList)):
                if (divisionSetList[index] == True):
                    defaultRow["소득_월"] = "{}".format(index + 1)
                    for index2 in range(len(FinancialIncome.incomeSection)):
                        row = defaultRow.copy()
                        row["소득_목표_종류"] = index2
                        row["소득_목표"] = earnMonthList[index][index2] * 10000
                        
                        incomeLiveDataFrame = financialIncomeLiveRawData
                        incomeLiveCost = incomeLiveDataFrame.loc[
                            (incomeLiveDataFrame["year"] == int(row["소득_연도"])) &
                            (incomeLiveDataFrame["month"] == int(row["소득_월"])) &
                            (incomeLiveDataFrame["user_id"] == int(row["id"])) &
                            (incomeLiveDataFrame["earnDivisionInt"] == index2)
                        ]
                        
                        if (not incomeLiveCost.empty):
                                row["소득_목표_현황"] = incomeLiveCost["cost"].values[0]
                        
                        financialIncomeData = financialIncomeData.append(row, ignore_index = True)
        
        return financialIncomeData
   
    
    def writeFinancialIncome(self):
        financialIncomeRawData = self.getFinancialIncome()
        financialIncomeLiveRawData = self.financialIncomeLive.getFinancialIncomeLive()
    
        financialIncomeData = self.procFinancialIncome(financialIncomeRawData, financialIncomeLiveRawData)
        return financialIncomeData


class FinancialIncomeLive:
    # Class Variable
    domain = { 
        "user_id": "id", 
        "year": "소득_연도",
        "month": "소득_월",
        "earnDivisionInt": "소득_목표_영역",
        "cost": "소득_목표_현황"
    }
    
    nonMappingColumnKey = list()
    nonMappingColumnValue = list()
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    
    def getFinancialIncomeLive(self):
        sqlQuery = """            
                    SELECT user_id, year, month, earnDivisionInt, SUM(cost) as cost
                    FROM pro.EarnLedger
                    GROUP BY user_id, year, month, earnDivisionInt;
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialIncomeLive.columnKeyList)
        return resultDataFrame


# + active=""
# # Only for test
#
# cursor = connectMySQL()
# financialIncomeInstance = FinancialIncome(cursor)
# financialIncomeData = financialIncomeInstance.writeFinancialIncome()
# display(financialIncomeData)
# -


