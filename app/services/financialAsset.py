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
        "kinds_index": "자산목표종류",
        "cost": "자산목표현황"
    }
    
    assetsSection = ['부동산자산', '사용자산', '현금성자산', '금융자산', '보장자산']
    
    nonMappingColumnKey = ["classification_index"]
    nonMappingColumnValue = ["assetId"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    assetIdColumnList = ["assetId", "자산현황항목", "자산현황금액"]
    
    assetItem = {
        "부동산자산": ["주거생활자산", "투자자산"],
        "사용자산": ["소멸성자산", "생활자산", "투자자산"],
        "현금성자산": ["예비생활자산", "투자자산"],
        "금융자산": ["저축자산", "투자자산"],
        "보장자산": ["연금자산", "보험자산"]
    }
    
    columnType = {
        "id": 'Int64', 
        "자산목표종류": 'str',
        "자산목표현황": 'Int64'
    }
    
    

    def __init__(self, cursor):
        self.cursor = cursor


        
    def getFinancialAsset(self):
        sqlQuery = """
                    SELECT user_id, kinds_index, classification_index, SUM(cost) as cost
                    FROM pro.CompositeAssets
                    GROUP BY user_id, kinds_index, classification_index;
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame(result, columns = FinancialAsset.columnKeyList)
        return resultDataFrame
    
    
    
    def procFinancialAsset(self, financialAssetRawData):
        financialAssetData = pd.DataFrame(columns = FinancialAsset.columnValueList)
        assetIdData = pd.DataFrame(columns = FinancialAsset.assetIdColumnList)
        
        financialAssetGroupData = financialAssetRawData.drop(["classification_index"], axis=1).groupby(["user_id", "kinds_index"], as_index = False).sum()
        
        # 재무 목표에서 자산에 해당되는 데이터를 가져와서 데이터프레임을 만든다.
        for value in financialAssetGroupData.values:
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
                
            
            if (row["자산목표종류"] != np.nan and row["자산목표종류"] is not None):
                assetsSectionIndex = FinancialAsset.assetsSection.index(row["자산목표종류"])
                assetCostList = financialAssetRawData.loc[
                    (financialAssetRawData["user_id"] == int(row["id"])) &
                    (financialAssetRawData["kinds_index"] == assetsSectionIndex)
                ]
                
                if (not assetCostList.empty):
                    assetIdRow = dict()
                    
                    assetIdValue = assetIdData["assetId"].max()
                    assetIdValue = assetIdValue + 1 if assetIdValue is not np.nan else 1
                    
                    row["assetId"] = assetIdValue
                    assetIdRow["assetId"] = assetIdValue
                    
                    for assetCost in assetCostList.to_dict('records'):
                        assetItemList = FinancialAsset.assetItem[row["자산목표종류"]]
                        assetItemIndex = assetCost["classification_index"]
                        if (int(assetItemIndex) < len(assetItemList)):
                            assetIdRow["자산현황항목"] = assetItemList[assetItemIndex]
                            assetIdRow["자산현황금액"] = assetCost["cost"]
                            assetIdData = assetIdData.append(assetIdRow, ignore_index = True)
                
            financialAssetData = financialAssetData.append(row, ignore_index = True)
            
        return financialAssetData, assetIdData
   
    
    def writeFinancialAsset(self):
        financialAssetRawData = self.getFinancialAsset()
    
        financialAssetData, assetIdData = self.procFinancialAsset(financialAssetRawData)
        financialAssetData = financialAssetData.astype(FinancialAsset.columnType)
        return financialAssetData, assetIdData

# + active=""
# # Only for test
#
# cursor = connectMySQL()
# financialAssetInstance = FinancialAsset(cursor)
# financialAssetData, assetIdData = financialAssetInstance.writeFinancialAsset()
# display(financialAssetData, assetIdData)
# -


