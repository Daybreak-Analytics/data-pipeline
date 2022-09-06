import boto3
import os
import sys

s3 = boto3.client('s3')
date = sys.argv[1]

filePath = './EventData/'
for i,j,k in os.walk(filePath):
    for file in k:
        if file.endswith(".csv"): 
            print(i+file)
            s3.upload_file(i+file, "daybreak", "guisong/parsedData/" + date + "/" +file.replace('.','_'))


