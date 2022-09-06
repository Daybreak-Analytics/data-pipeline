cd /home/ubuntu/jobs/pyspark
echo "Replace Character. --   : $1"
python3 replaceCharacter.py
echo "Upload S3,  --   : $1"
python3 uploadParsedData.py $1