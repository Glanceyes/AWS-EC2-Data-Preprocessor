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

class Scheduling:
    
    # Class Variable
    
    # DB의 데이터와 전처리 데이터 간의 컬럼이 매칭되는 경우를 명시한다.
    
    domain = { "user_id": "id", 
                "division": "시간관리영역",
                "name": "시간관리키워드",
                "type": "시간관리활동종류",
                "date": "시간관리연월"
            }
    
    schedulingSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가']
    
    
    # type 컬럼 값에 따라 isDone 값이 매칭되는 컬럼이 목표활동완료 컬럼 또는 일반활동완료 컬럼으로 달라진다.
    
    columnKeyList = list(domain.keys()) + ["isDone"]
    columnValueList = list(domain.values()) + ["목표활동완료", "일반활동완료"]
    
    columnOrder = ["id", "시간관리영역", "시간관리키워드", "시간관리활동종류", "목표활동완료", "일반활동완료", "시간관리연월"]
    
    
    keywordExtractorInstance = KeywordExtractor()
    

    
    def __init__(self, cursor):
        self.cursor = cursor
        
        
        
    def getScheduling(self):
        # 사용자별 재무 외의 목표와 생일을 가져온다.
        sqlQuery = """
                    SELECT * FROM pro.DiarySchedule
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = Scheduling.columnKeyList)
        return resultDataFrame
    
    
    
    def updateScheduling(self, resultData):
        schedulingData = pd.DataFrame(columns = Scheduling.columnValueList)
            
        # 재무 외의 목표를 읽어서 데이터 프레임으로 만든다.
        for value in resultData.values:
            row = dict()
            
            for index in range(len(Scheduling.columnKeyList)):
                columnKey = Scheduling.columnKeyList[index]
                columnValue = Scheduling.columnValueList[index]
                
                # 시간관리키워드 컬럼은 자연어 처리를 통해 하나의 키워드를 추출해야 한다.
                if (columnKey == "name"):
                    # content 컬럼의 데이터를 NLP(자연어 처리)하여 10년 목표키워드를 구한다.
                    try:
                        row[columnValue] = Scheduling.keywordExtractorInstance.extractKeyword(value[index])
                    except:
                        continue
                
                # 시간관리영역 컬럼은 값에 따라 영역 이름에 관한 문자열을 mapping 해야 한다.
                elif (columnKey == "division"):
                    row[columnValue] = Scheduling.schedulingSection[value[index]]
                
                # 매칭되지 않는 목표활동완료와 일반활동완료 컬럼에 대한 별도의 처리가 필요하다.
                elif (columnKey == "isDone"):
                    if (value[Scheduling.columnKeyList.index('type')] == 0):
                        row["목표활동완료"] = int(value[index]) if not pd.isna(value[index]) else ""
                    else:
                        row["일반활동완료"] = int(value[index]) if not pd.isna(value[index]) else ""

                elif (columnKey == "date"):
                    row[columnValue] = parse(value[index]).strftime("%Y.%m.%d")
                        
                else:
                    row[columnValue] = value[index]
                
            schedulingData = schedulingData.append(row, ignore_index = True)
            
        schedulingData = schedulingData.fillna({"시간관리키워드": "", "목표활동완료": "", "일반활동완료" : ""})
        schedulingData = schedulingData.reindex(columns = Scheduling.columnOrder)
        
        return schedulingData
    
    
    
    def doScheduling(self):
        resultData = self.getScheduling()
        schedulingData = self.updateScheduling(resultData)
        print(schedulingData)
        return schedulingData

# + active=""
# # Only for test
# cursor = connectMySQL()
# schedulingInstance = Scheduling(cursor)
# resultDataFrame = schedulingInstance.getScheduling()
# schedulingData = schedulingInstance.updateScheduling(resultDataFrame)
# print(schedulingData)
# -


