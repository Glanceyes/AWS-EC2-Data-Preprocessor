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
from utils.keywordExtractor import *


# +
# pd.set_option('display.max_rows', None)
# -

class GoalFinance:

    # Class Variable
    
    domain = { 
                "user_id": "id", 
                "level": "재무목표단위",
                "endYear": "재무목표연도",
            }
    
    nonMappingColumnKey = ["spend", "earn", "assets", "spendSavable", "spendPercentageKey", "earnDivisionArray", "assetsDivisionArray"]
    nonMappingColumnValue = ["재무목표", "재무목표금액", "financialId"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    goalSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가', '재무']
    incomeSection = ['연금소득', '근로/사업소득', '자산소득', '이전소득', '기타소득', '증여상속']
    assetsSection = ['부동산', '사용', '현금성', '금융', '보장']
    
    percentageKeyList = ["key", "usable", "savable"]
    financialIdColumnList = ["financialId", "지출목표영역", "지출목표영역비율", "소득분류", "소득분류목표금액", "자산분류", "자산분류목표금액"]
    
    
    columnType = {
        "id": 'Int64', 
        "재무목표단위": 'Int64',
        "financialId": 'Int64',
    }
    
    
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.Goal10YearFinanceInstance = Goal10YearFinance(cursor)
        
        
    def getGoalFinance(self):
        sqlQuery = """
                    SELECT * 
                    FROM pro.GoalFinance
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = GoalFinance.columnKeyList)
        return resultDataFrame
    
    
    
    def getExpensePercentages(self):
        sqlQuery = """
                    SELECT * FROM pro.LedgerPercentages
                    """
        
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = GoalFinance.percentageKeyList)
        return resultDataFrame
    

    def procGoalFinance(self, goalFinanceRawData, expensePercentagesRawData):
        goalFinanceData = pd.DataFrame(columns = GoalFinance.columnValueList)
        goalFinancialIdData = pd.DataFrame(columns = GoalFinance.financialIdColumnList)
        
        
        # 재무 목표를 읽어서 데이터 프레임으로 만든다.
        for value in goalFinanceRawData.values:
            defaultRow = dict()
            
            for index in range(len(GoalFinance.domain)):
                columnKey = GoalFinance.columnKeyList[index]
                columnValue = GoalFinance.columnValueList[index]
                
                defaultRow[columnValue] = value[index]
            
            
            for index in range(len(GoalFinance.domain), len(GoalFinance.columnKeyList)):
                columnKey = GoalFinance.columnKeyList[index]
                
                row = defaultRow.copy()
                
                if (columnKey in ["spend", "spendSavable", "earn", "assets"]):

                    row["재무목표금액"] = value[index]
                    financialIdValue = goalFinancialIdData["financialId"].max()
                    financialIdValue = financialIdValue + 1 if financialIdValue is not np.nan else 1
                    
                    if (columnKey == "spend"):
                        row["재무목표"] = "소멸성_지출목표"
                        percentageKeyIndex = GoalFinance.columnKeyList.index("spendPercentageKey")
                        
                        if (value[percentageKeyIndex] > 0):
                            financialIdRow = dict()

                            row["financialId"] = financialIdValue
                            financialIdRow["financialId"] = financialIdValue

                            df = expensePercentagesRawData
                            percentageList = df.loc[df["key"] == int(value[percentageKeyIndex])]["usable"].values[0]

                            try:
                                percentageList = ast.literal_eval(percentageList)
                            except:
                                continue

                            for index2 in range(len(percentageList)):
                                financialIdRow["지출목표영역"] = GoalFinance.goalSection[index2]
                                financialIdRow["지출목표영역비율"] = percentageList[index2]
                                goalFinancialIdData = goalFinancialIdData.append(financialIdRow, ignore_index = True)


                    elif (columnKey == "spendSavable"):
                        row["재무목표"] = "저축성_지출목표"
                        percentageKeyIndex = GoalFinance.columnKeyList.index("spendPercentageKey")

                        if (value[percentageKeyIndex] > 0):
                            financialIdRow = dict()

                            row["financialId"] = financialIdValue
                            financialIdRow["financialId"] = financialIdValue

                            df = expensePercentagesRawData
                            percentageList = df.loc[df["key"] == int(value[percentageKeyIndex])]["savable"].values[0]

                            try:
                                percentageList = ast.literal_eval(percentageList)
                            except:
                                continue

                            for index2 in range(len(percentageList)):
                                financialIdRow["지출목표영역"] = GoalFinance.goalSection[index2]
                                financialIdRow["지출목표영역비율"] = percentageList[index2]
                                goalFinancialIdData = goalFinancialIdData.append(financialIdRow, ignore_index = True)


                    elif (columnKey == "earn"):
                        row["재무목표"] = "소득목표"
                        percentageKeyIndex = GoalFinance.columnKeyList.index("earnDivisionArray")
                        percentageList = value[percentageKeyIndex]

                        try:
                            percentageList = ast.literal_eval(percentageList)
                        except:
                            continue

                        for index2 in range(len(percentageList)):
                            financialIdRow = dict()

                            row["financialId"] = financialIdValue
                            financialIdRow["financialId"] = financialIdValue

                            financialIdRow["소득분류"] = GoalFinance.incomeSection[index2]
                            financialIdRow["소득분류목표금액"] = percentageList[index2]
                            goalFinancialIdData = goalFinancialIdData.append(financialIdRow, ignore_index = True)


                    elif (columnKey == "assets"):
                        row["재무목표"] = "자산목표"
                        percentageKeyIndex = GoalFinance.columnKeyList.index("assetsDivisionArray")
                        percentageList = value[percentageKeyIndex]

                        try:
                            percentageList = ast.literal_eval(percentageList)
                        except:
                            continue

                        for index2 in range(len(percentageList)):
                            financialIdRow = dict()

                            row["financialId"] = financialIdValue
                            financialIdRow["financialId"] = financialIdValue

                            financialIdRow["자산분류"] = GoalFinance.assetsSection[index2]
                            financialIdRow["자산분류목표금액"] = percentageList[index2]
                            goalFinancialIdData = goalFinancialIdData.append(financialIdRow, ignore_index = True)
                    
                    goalFinanceData = goalFinanceData.append(row, ignore_index = True)
        
        return goalFinanceData, goalFinancialIdData
    
    
    def writeGoalFinance(self):
        goalFinanceRawData = self.getGoalFinance()
        goal10YearFinanceRawData = self.Goal10YearFinanceInstance.getGoal10YearFinance()
        goalFinanceRawData = pd.concat([goalFinanceRawData, goal10YearFinanceRawData])
        expensePercentagesRawData = self.getExpensePercentages()
        
        goalFinanceData, goalFinancialIdData = self.procGoalFinance(goalFinanceRawData, expensePercentagesRawData)
        goalFinanceData = goalFinanceData.astype(GoalFinance.columnType, errors = 'ignore')
        return goalFinanceData, goalFinancialIdData


class Goal10YearFinance:
    
    # Class Variable
    
    domain = { 
                "user_id": "id",
                "year": "재무목표연도",
            }
    
    nonMappingColumnKey = ["usableSpend", "savableSpend", "earn", "assets", "spendPercentageKey", "earnDivisionArray", "assetsDivisionArray"]
    nonMappingColumnValue = ["재무목표", "재무목표금액", "financialId"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    
    # Goal10YearFinance 테이블을 GoalFinance 테이블과 연관시키기 위한 변수
    
    matchColumn = {
                "year": "endYear",
                "usableSpend": "spend",
                "savableSpend": "spendSavable",
            }
    
    
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    
    def getGoal10YearFinance(self):
        sqlQuery = """
                    SELECT *
                    FROM pro.Goal10YearFinance
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = Goal10YearFinance.columnKeyList)
        resultDataFrame.rename(columns = Goal10YearFinance.matchColumn, inplace = True)
        return resultDataFrame

# + active=""
# # Only for test
#
# cursor = connectMySQL()
# goalFinanceInstance = GoalFinance(cursor)
# goalFinanceData, goalFinancialIdData = goalFinanceInstance.writeGoalFinance()
# display(goalFinanceData, goalFinancialIdData)
