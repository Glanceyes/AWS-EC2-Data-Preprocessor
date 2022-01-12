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


# -

class FinanceScore:
    def __init__(self, cursor):
        self.cursor = cursor
        
    def getFinanceScore(self):
        # 사용자별 참여 날짜가 최신인 설문조사 결과만 가져온다.
        sqlQuery = """
                    SELECT user_id, type, totalPoint, T2.ranking
                    FROM (
                        SELECT *, ROW_NUMBER() OVER(PARTITION BY T1.user_id, T1.type ORDER BY date DESC) 'ranking'
                        FROM pro.SurveyResult as T1
                        WHERE (
                            type = 'FinanceIndex' OR
                            type = 'InvestmentIndex' OR
                            type = 'ConsumptionIndex' OR
                            type = 'DebtIndex'
                        )
                    ) as T2
                    WHERE T2.ranking = 1
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = ['user_id', 'type', 'totalPoint'])
        return resultDataFrame
    
    
    def updateFinanceScore(self, resultData):
        scoreDomain = {"FinanceIndex": "금융성향", 
                       "InvestmentIndex": "투자성향", 
                       "ConsumptionIndex": "소비성향", 
                       "DebtIndex": "부채관리성향"}
        
        scoreData = pd.DataFrame(columns = ["id"] + list(scoreDomain.values()))
        
        # totalPoint 컬럼은 불러온 데이터 프레임의 3번째 column(index상 2번째)에 해당된다.
        columnIndex = 2 # index 기준
        
        for value in resultData.values:
            typeName = scoreDomain.get(value[1], None)
            if (typeName is None):
                continue
            
            findData = scoreData.loc[scoreData['id'] == value[0]]
            if (findData.empty):
                scoreData = scoreData.append({'id': int(value[0])}, ignore_index = True)
                
            scoreData.loc[scoreData['id'] == value[0], ("{}".format(typeName))] = int(value[columnIndex])
        
        scoreData = scoreData.fillna(0)
        scoreData = scoreData.astype('int')
        return scoreData
    
    def doFinanceScore(self):
        resultData = self.getFinanceScore()
        financeScoreData = self.updateFinanceScore(resultData)
        print(financeScoreData)
        return financeScoreData

# + active=""
# # Only for test
# cursor = connectMySQL()
# financeScoreInstance = FinanceScore(cursor)
# resultDataFrame = financeScoreInstance.getFinanceScore()
# scoreData = financeScoreInstance.updateFinanceScore(resultDataFrame)
# print(scoreData)
# -


