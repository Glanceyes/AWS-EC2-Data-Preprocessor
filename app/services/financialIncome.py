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
    nonMappingColumnValue = ["소득_월", "소득_목표_종류", "소득_목표", "소득_목표_현황", "incomeId"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    incomeIdColumnList = ["incomeId", "소득현황항목", "소득현황금액"]
    
    incomeItem = {
        "연금소득": ["공적연금소득", "퇴직연금소득", "기타연금소득"],
        "근로/사업소득": ["근로소득", "사업소득", "기타소득"],
        "자산소득": ["금융투자소득", "자산임대소득", "자산매각소득"],
        "이전소득": ["공적이전금", "사적이전금"],
        "기타소득": ["일시소득", "부채"],
        "증여상속": ["증여소득", "상속소득"]
    }
    
    columnType = {
        "id": 'Int64', 
        "소득_목표_종류": 'Int64', 
        "소득_목표": 'Int64', 
        "소득_목표_현황": 'Int64'
    }
    
    

    def __init__(self, cursor):
        self.cursor = cursor
        self.FinancialIncomeLive = FinancialIncomeLive(cursor)


        
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
        incomeIdData = pd.DataFrame(columns = FinancialIncome.incomeIdColumnList)
        
        financialIncomeLiveGroupData = financialIncomeLiveRawData.groupby(["user_id", "year", "month", "earnDivisionInt"], as_index = False).sum()
        
        
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
                        incomeSectionName = FinancialIncome.incomeSection[index2]
                        row["소득_목표_종류"] = incomeSectionName
                        row["소득_목표"] = earnMonthList[index][index2] * 10000
                        
                        
                        
                        incomeLiveGroupDataFrame = financialIncomeLiveGroupData
                        incomeLiveGroupCost = incomeLiveGroupDataFrame.loc[
                            (incomeLiveGroupDataFrame["user_id"] == int(row["id"])) &
                            (incomeLiveGroupDataFrame["year"] == int(row["소득_연도"])) &
                            (incomeLiveGroupDataFrame["month"] == int(row["소득_월"])) &
                            (incomeLiveGroupDataFrame["earnDivisionInt"] == index2)
                        ]
                        
                        if (not incomeLiveGroupCost.empty):
                            row["소득_목표_현황"] = incomeLiveGroupCost["cost"].values[0]
                        
                        incomeLiveDataFrame = financialIncomeLiveRawData
                        incomeLiveCostList = incomeLiveDataFrame.loc[
                            (incomeLiveDataFrame["user_id"] == int(row["id"])) &
                            (incomeLiveDataFrame["year"] == int(row["소득_연도"])) &
                            (incomeLiveDataFrame["month"] == int(row["소득_월"])) &
                            (incomeLiveDataFrame["earnDivisionInt"] == index2)
                        ]
                        
                        
                        if (not incomeLiveCostList.empty):
                            incomeIdRow = dict()
                            
                            incomeIdValue = incomeIdData["incomeId"].max()
                            incomeIdValue = incomeIdValue + 1 if incomeIdValue is not np.nan else 1
                            
                            row["incomeId"] = incomeIdValue
                            incomeIdRow["incomeId"] = incomeIdValue
                            
                            # 되도록이면 나머지도 데이터프레임의 row를 to_dict 메소드의 'records' parameter를 통해 dictionary type으로 바꿀 필요가 있다.
                            for incomeCost in incomeLiveCostList.to_dict('records'):
                                incomeItemList = FinancialIncome.incomeItem[incomeSectionName]
                                if (incomeCost["earnDivisionItem"] in incomeItemList):
                                    incomeIdRow["소득현황항목"] = incomeCost["earnDivisionItem"]
                                    incomeIdRow["소득현황금액"] = incomeCost["cost"]
                                    incomeIdData = incomeIdData.append(incomeIdRow, ignore_index = True)
                                    
                        
                        financialIncomeData = financialIncomeData.append(row, ignore_index = True)
        
        return financialIncomeData, incomeIdData
   
    
    def writeFinancialIncome(self):
        financialIncomeRawData = self.getFinancialIncome()
        financialIncomeLiveRawData = self.FinancialIncomeLive.getFinancialIncomeLive()

        financialIncomeData, incomeIdData = self.procFinancialIncome(financialIncomeRawData, financialIncomeLiveRawData)
        return financialIncomeData, incomeIdData


class FinancialIncomeLive:
    # Class Variable
    domain = { 
        "user_id": "id", 
        "year": "소득_연도",
        "month": "소득_월",
        "earnDivisionInt": "소득_현황_영역",
        "earnDivisionItem": "소득_현황_항목",
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
                    SELECT user_id, year, month, earnDivisionInt, earnDivisionItem, SUM(cost) as cost
                    FROM pro.EarnLedger
                    GROUP BY user_id, year, month, earnDivisionInt, earnDivisionItem;
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
# financialIncomeData, incomeIdData = financialIncomeInstance.writeFinancialIncome()
# display(financialIncomeData, incomeIdData)
# -

