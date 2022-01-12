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
import mysql.connector
import pymysql


def connectMySQL():
    try: 
        connection = pymysql.connect(
            user = os.environ['AWS_CONNECTION_USERNAME'], 
            passwd = os.environ['AWS_CONNECTION_PASSWORD'], 
            host = os.environ['AWS_CONNECTION_HOST'], 
            database = os.environ['AWS_CONNECTION_DATABASE'], 
            charset ='utf8',
            cursorclass = pymysql.cursors.DictCursor
        )
        cursor = connection.cursor()
        return cursor
    except Exception as e:
        print("Database connection failed due to {}".format(e))
        quit()


