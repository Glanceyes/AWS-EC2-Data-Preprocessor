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

import sys
import os
import requests
import mysql.connector
import base64
import json
import logging
import time
import pymysql
import boto3
import json
import pandas as pd
import csv
import import_ipynb

# 서비스 계층 class import
from services.lifeScore import LifeScore
from services.tenYearsGoal import TenYearsGoal
from services.financeScore import FinanceScore
from services.visionGoal import VisionGoal
from services.scheduling import Scheduling

# 유틸리티 계층 class import
from utils.connectDB import connectMySQL
from utils.writeCSV import writeToCSV

cursor = connectMySQL()

# lifeScore 데이터 전처리 후 csv 파일 작성 
lifeScoreInstance = LifeScore(cursor)
lifeScoreData = lifeScoreInstance.doLifeScore()
writeToCSV(lifeScoreData, "life_score", "life_score")

# tenYearsGoal 데이터 전처리 후 csv 파일 작성
tenYearsGoalInstance = TenYearsGoal(cursor)
tenYearsGoalData = tenYearsGoalInstance.doTenYearsGoal()
writeToCSV(tenYearsGoalData, "ten_years_goal", "ten_years_goal")

# financeScore 데이터 전처리 후 csv 파일 작성
financeScoreInstance = FinanceScore(cursor)
financeScoreData = financeScoreInstance.doFinanceScore()
writeToCSV(financeScoreData, "financial_score", "financial_score")

# visionGoal 데이터 전처리 후 csv 파일 작성
visionGoalInstance = VisionGoal(cursor)
visionGoalData = visionGoalInstance.doVisionGoal()
writeToCSV(visionGoalData, "vision_goal", "vision_goal")

# scheduling 데이터 전처리 후 csv 파일 작성
schedulingInstance = Scheduling(cursor)
schedulingData = schedulingInstance.doScheduling()
writeToCSV(schedulingData, "scheduling", "scheduling")
