#!/bin/bash
cd /home/ubuntu/jobs/extract_data
echo $USER
pwd
echo "Running Date is: $1"
/usr/local/go/bin/go run . $1