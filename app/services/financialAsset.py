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
# -

pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:.0f}'.format


class FinancialAsset:
    
    # Class Variable
    
    domain = { 
        "user_id": "id",
        "kinds_index": "자산_목표_종류",
        "cost": "자산_목표_현황"
    }
    
    assetsSection = ['부동산', '사용', '현금성', '금융', '보장']
    
    nonMappingColumnKey = []
    nonMappingColumnValue = []
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    columnType = {
        "id": 'Int64', 
        "자산_목표_종류": 'str',
        "자산_목표_현황": 'Int64'
    }
    
    

    def __init__(self, cursor):
        self.cursor = cursor


        
    def getFinancialAsset(self):
        sqlQuery = """
                    SELECT user_id, kinds_index, SUM(cost) as cost
                    FROM pro.CompositeAssets
                    GROUP BY user_id, kinds_index;
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialAsset.columnKeyList)
        return resultDataFrame
    
    
    
    def procFinancialAsset(self, financialAssetRawData):
        financialAssetData = pd.DataFrame(columns = FinancialAsset.columnValueList)
            
        # 재무 목표에서 자산에 해당되는 데이터를 가져와서 데이터프레임을 만든다.
        for value in financialAssetRawData.values:
            row = dict()
            
            for index in range(len(FinancialAsset.domain)):
                columnKey = FinancialAsset.columnKeyList[index]
                columnValue = FinancialAsset.columnValueList[index]
                if (columnKey == "kinds_index"):
                    try:
                        row[columnValue] = FinancialAsset.assetsSection[int(value[index])]
                    except:
                        continue
                else :
                    row[columnValue] = value[index]
                
            financialAssetData = financialAssetData.append(row, ignore_index = True)
        
        return financialAssetData
   
    
    def writeFinancialAsset(self):
        financialAssetRawData = self.getFinancialAsset()
    
        financialAssetData = self.procFinancialAsset(financialAssetRawData)
        financialAssetData = financialAssetData.astype(FinancialAsset.columnType)
        return financialAssetData

# + active=""
# # Only for test
#
# cursor = connectMySQL()
# financialAssetInstance = FinancialAsset(cursor)
# financialAssetData = financialAssetInstance.writeFinancialAsset()
# display(financialAssetData)
# -


