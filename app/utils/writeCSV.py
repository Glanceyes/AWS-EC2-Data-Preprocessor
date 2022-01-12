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

import os
import csv
import pandas as pd


def makeDir(dirPath): 
    try:
        os.makedirs(dirPath) 
    except OSError: 
        if not os.path.isdir(dirPath): 
            raise


def writeToCSV(df, folderName, fileName):
    dirPath = "./data/{0}".format(folderName)
    
    if not os.path.exists(dirPath):
        makeDir(dirPath)

    if folderName is not None:
        df.to_csv("./data/{0}/{1}.csv".format(folderName, fileName), mode='w')
    else :
        df.to_csv("./data/{0}.csv".format(fileName), mode='w')


