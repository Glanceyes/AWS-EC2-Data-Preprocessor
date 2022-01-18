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

class VisionKeyword:
    
    # Class Variable
    domain = { 
            "user_id": "id", 
            }
    
    visionSection = ['직업', '학습', '건강', '관계', '주거', '사회참여', '여가']
    
    nonMappingColumnKey = ['visions']
    nonMappingColumnValue = ['비전_영역', '비전_키워드']
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    keywordExtractorInstance = KeywordExtractor()

    
    
    def __init__(self, cursor):
        self.cursor = cursor
        
        
        
    def getVisionKeyword(self):
        # 사용자별 재무 외의 목표와 생일을 가져온다.
        sqlQuery = """
                    SELECT * FROM pro.Vision
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = VisionKeyword.columnKeyList)
        return resultDataFrame
    
    
    
    def procVisionKeyword(self, resultData):
        visionKeywordData = pd.DataFrame(columns = VisionKeyword.columnValueList)
            
        # 재무 외의 목표를 읽어서 데이터 프레임으로 만든다.
        for value in resultData.values:
            defaultRow = dict()
            
            for index in range(len(VisionKeyword.domain)):
                columnKey = VisionKeyword.columnKeyList[index]
                columnValue = VisionKeyword.columnValueList[index]
                
                defaultRow[columnValue] = value[index]
            
            for index in range(len(VisionKeyword.columnKeyList)):
                columnKey = VisionKeyword.columnKeyList[index]
                columnValue = VisionKeyword.columnValueList[index]
                
                # visions 컬럼에서 각 영역별 비전 내용을 가져와서 키워드를 추출한다.
                if (columnKey == "visions"):
                    for index2 in range(len(VisionKeyword.visionSection)):
                        visionName = VisionKeyword.visionSection[index2]
                        try:
                            textlist = ast.literal_eval(value[index])
                            extractKeyword = VisionKeyword.keywordExtractorInstance.extractKeyword
                            row = defaultRow.copy()
                            row["비전_영역"] = visionName
                            row["비전_키워드"] = extractKeyword(textlist[index2])
                            visionKeywordData = visionKeywordData.append(row, ignore_index = True)
                        except:
                            continue
            
        return visionKeywordData
    
    def writeVisionKeyword(self):
        resultData = self.getVisionKeyword()
        visionKeywordData = self.procVisionKeyword(resultData)
        # print(visionKeywordData)
        return visionKeywordData

# + active=""
# # Only for test
# cursor = connectMySQL()
# visionKeywordInstance = VisionKeyword(cursor)
# resultDataFrame = visionKeywordInstance.getVisionKeyword()
# visionKeywordData = visionKeywordInstance.procVisionKeyword(resultDataFrame)
# print(visionKeywordData)
# -


