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

class User:
    
    # Class Variable
    domain = { 
                "id": "id", 
                "birthday": "birthday", 
                "sex": "sex",
                "createdAt": "createdAt",
                "updatedAt": "updatedAt",
                "agency_key": "기관_key",
            }
    
    nonMappingColumnKey = []
    nonMappingColumnValue = ["기관_type", "부서", "직책", "직위"]
    
    columnKeyList = list(domain.keys()) + nonMappingColumnKey
    columnValueList = list(domain.values()) + nonMappingColumnValue
    
    changeGoalCostAttribute = {-1 : "", 1 : 0, 2 : 1}
    
    columnType = {
        "id": 'Int64', 
        "sex": 'Int64', 
        "기관_key": 'Int64'
    }
    
    
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.Agency = Agency(cursor)
        self.Enterprise = Enterprise(cursor)
        self.School = School(cursor)
        
        
    def getUser(self):
        sqlQuery = """
                    SELECT * FROM pro.users
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        self.userRawData = pd.DataFrame(result, columns = User.columnKeyList)
        return self.userRawData
    
    
    
    def procUser(self):
        userRawData = self.userRawData
        agencyRawData = self.Agency.agencyRawData
        enterpriseRawData = self.Enterprise.enterpriseRawData
        schoolRawData = self.School.schoolRawData
        
        userData = pd.DataFrame(columns = User.columnValueList)
            
        for value in userRawData.values:
            row = dict()
            
            for index in range(len(User.columnKeyList)):
                columnKey = User.columnKeyList[index]
                columnValue = User.columnValueList[index]
                
                # 생일은 S3 버킷에 저장할 형식으로 변환한다.
                if (columnKey == "birthday"):
                    try:
                        row[columnValue] = parse(value[index]).strftime("%Y.%m.%d")
                    except:
                        continue
                
                # createdAt 컬럼은 "연-월-일" 형식으로 변환한다.
                elif (columnKey == "createdAt"):
                    row[columnValue] = value[index].strftime("%Y-%m-%d")
                
                # updatedAt 컬럼은 "연-월-일" 형식으로 변환한다.
                elif (columnKey == "updatedAt"):
                    row[columnValue] = value[index].strftime("%Y-%m-%d")
                
                elif (columnKey == "agency_key"):
                    if (not np.isnan(value[index])):
                        row[columnValue] = int(value[index])
                        agencyRawData = self.Agency.agencyRawData
                        agency = agencyRawData.loc[agencyRawData["key"] == value[index]]
                        
                        if (agency.empty):
                            continue
                        
                        # 기관의 종류가 기업인지 학교인지 알아야 한다.
                        agencyType = agency["type"].values[0]
                        row["기관_type"] = agencyType
                        
                        if (agencyType == "enterprise"):
                            enterpriseRawData = self.Enterprise.enterpriseRawData
                            enterprise = enterpriseRawData.loc[
                                (enterpriseRawData["agency_key"] == value[index]) &
                                (enterpriseRawData["user_id"] == value[User.columnKeyList.index("id")])
                            ]
                            
                            if (enterprise.empty):
                                continue
                            
                            row["직책"] = enterprise["role"].values[0]
                            row["부서"] = enterprise["dept"].values[0]
                            row["직위"] = enterprise["position"].values[0]
                            
                        elif (agencyType == "school"):
                            schoolRawData = self.School.schoolRawData
                            school = schoolRawData.loc[
                                (schoolRawData["agency_key"] == value[index]) &
                                (schoolRawData["user_id"] == value[User.columnKeyList.index("id")])
                            ]
                            
                            if (school.empty):
                                continue
                            
                            row["직책"] = school["role"].values[0]
                            row["부서"] = school["dept"].values[0]
                            row["직위"] = school["position"].values[0]
                        
                else:
                    row[columnValue] = value[index]
                
            userData = userData.append(row, ignore_index = True)
 
        return userData
    
    def writeUser(self):
        self.getUser()
        self.Agency.getAgency()
        self.Enterprise.getEnterprise()
        self.School.getSchool()
        
        userData = self.procUser()
        userData = userData.astype(User.columnType)
        return userData


class Agency:
    
    # Class Variable
    domain = { 
                "key": "agencyKey", 
                "type": "agencyType",
            }
    
    columnKeyList = list(domain.keys())
    columnValueList = list(domain.values())
    
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    
    def getAgency(self):
        sqlQuery = """
                    SELECT * FROM pro.Agency
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        self.agencyRawData = pd.DataFrame(result, columns = Agency.columnKeyList)
        return self.agencyRawData


class Enterprise:
    
    # Class Variable
    domain = { 
                "agency_key": "enterprise_key",
                "user_id": "user_id",
                "role": "직책", 
                "dept": "부서",
                "position": "직위"
            }
    
    columnKeyList = list(domain.keys())
    columnValueList = list(domain.values())
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    def getEnterprise(self):
        sqlQuery = """
                    SELECT * FROM pro.Enterprise
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        self.enterpriseRawData = pd.DataFrame(result, columns = Enterprise.columnKeyList)
        return self.enterpriseRawData


class School:
    
    # Class Variable
    domain = { 
                "agency_key": "school_key",
                "user_id": "user_id",
                "role": "직책", 
                "dept": "부서",
                "position": "직위",
            }
    
    columnKeyList = list(domain.keys())
    columnValueList = list(domain.values())
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    def getSchool(self):
        sqlQuery = """
                    SELECT * FROM pro.School
                    """
        self.cursor.execute(sqlQuery)
        result = self.cursor.fetchall()
        self.schoolRawData = pd.DataFrame(result, columns = School.columnKeyList)
        return self.schoolRawData


# + active=""
# # Only for test
# cursor = connectMySQL()
# userInstance = User(cursor)
# userData = userInstance.writeUser()
# display(userData)
# -


