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
import sys
import glob
import boto3
# import logging
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError


class SyncWithS3Bucket:
    
    def __init__(self, 
                 bucketName = os.environ['AWS_S3_BUCKETNAME'],
                 dataPathRoot = os.environ['AWS_S3_DATAPATHROOT'],
                 uploadPathRoot = os.environ['AWS_S3_UPLOADPATHROOT']
                ):
        self.bucketName = bucketName
        self.dataPathRoot = dataPathRoot
        self.uploadPathRoot = uploadPathRoot
        # logger = logging.getLogger(__name__)
        # logger.setLevel(logging.INFO)
        
    
    def searchAllFile(self, dirPath, dirName, uploadFileList):
        try:
            fileList = os.listdir(dirPath)
            for fileName in fileList:
                filePath = os.path.join(dirPath, fileName)
                if os.path.isdir(filePath):
                    self.searchAllFile(filePath, os.path.join(dirName, fileName), uploadFileList)
                else:
                    fileExtension = os.path.splitext(fileName)[-1]
                    if fileExtension == '.csv': 
                        uploadFileList.append({"dirName": dirName, "fileName": fileName, "filePath": filePath})
        except PermissionError:
            pass
    
    
    def createBucket(self, bucketName):
        print("Creating a bucket... " + bucketName)

        session = boto3.Session()
        s3 = session.client('s3')

        try:
            response = s3.create_bucket(
                Bucket = bucketName,
                CreateBucketConfiguration={
                    'LocationConstraint': os.environ['AWS_DEFAULT_REGION']
                }
            )
            print("Bucket: " + str(response))
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print("Bucket already exists.")
                print("Bucket: " + str(bucketName))
            else:
                print("Unknown error, exit.")
                sys.exit()
                
        return s3
    
    
    def uploadFile(self):
        uploadFileList = list()
        s3 = self.createBucket(self.bucketName)
        self.searchAllFile(self.dataPathRoot, self.uploadPathRoot, uploadFileList)

        for uploadFile in uploadFileList:
            try:
                # logger.info("[FROM] %s => [TO] %s\n" % (uploadFile["filePath"], uploadFile["dirName"] + "/" + uploadFile["fileName"]) )
                display("[FROM] %s => [TO] %s\n" % (uploadFile["filePath"], uploadFile["dirName"] + "/" + uploadFile["fileName"]) )
                s3.upload_file(uploadFile["filePath"], self.bucketName, uploadFile["dirName"] + "/" + uploadFile["fileName"])
            except ClientError as e:
                display(e)
                pass

# + active=""
# # Only for test
# syncWithS3BucketInstance = SyncWithS3Bucket()
# syncWithS3BucketInstance.uploadFile()

