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


# +
# pd.set_option('display.max_rows', None)
# -

class LifeScore:
    
    domain = { 
            "user_id": "id"
    }
    
    scoreSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가', '재무']
    
    nonMappingColumnKey = ["pageResult"]
    nonMappingColumnValue = ["영역", "영역점수"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    columnType = {
        "id": 'Int64', 
        "영역": 'str', 
        "영역점수": 'Int64'
    }
    
    
    def __init__(self, cursor):
        self.cursor = cursor
        
    def getLifeScore(self):
        # 사용자별 참여 날짜가 최신인 설문조사 결과만 가져온다.
        sqlQuery = """
                    SELECT user_id, pageResult, MAX(date)
                    FROM (
                        SELECT * FROM pro.SurveyResult
                        WHERE (type = 'LifeDesignOld' or type = 'LifeDesignYoung')
                    ) AS T1
                    WHERE 1=1 GROUP BY user_id;
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = LifeScore.columnKeyList)
        return resultDataFrame
    
    def procLifeScore(self, lifeScoreRawData):
        lifeScoreData = pd.DataFrame(columns = LifeScore.columnValueList)
        
        # 8대 영역별 점수에 관한 데이터는 pageResult에서 3번째 section(index상 2번째)에 해당된다.
        sectionNum = 2 # index 기준
        
        # pageResult 컬럼은 불러온 데이터 프레임의 2번째 column(index상 1번째)에 해당된다.
        columnIndex = 1 # index 기준
        
        for value in lifeScoreRawData.values:
            # pageResult 컬럼의 string 타입 데이터를 json으로 전처리한다.
            row = dict()
            row["id"] = value[0]
            for index in range(len(LifeScore.scoreSection)):
                row["영역"] = LifeScore.scoreSection[index]
                row["영역점수"] = int(json.loads(value[columnIndex])[sectionNum][str(index)]) * 20
                lifeScoreData = lifeScoreData.append(row, ignore_index = True)
        
        return lifeScoreData
    
    def writeLifeScore(self):
        lifeScoreRawData = self.getLifeScore()
        lifeScoreData = self.procLifeScore(lifeScoreRawData)
        lifeScoreData = lifeScoreData.astype(LifeScore.columnType)
        return lifeScoreData

# + active=""
# # Only for test
# cursor = connectMySQL()
# lifeScoreInstance = LifeScore(cursor)
# lifeScoreData = lifeScoreInstance.writeLifeScore()
#
# display(lifeScoreData)
# -


