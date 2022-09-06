cd /home/ubuntu/jobs/pyspark
rm -rf rawdata
rm -rf EventData
aws s3 cp /home/ubuntu/jobs/extract_data/liveEvent/$1.tsv  s3://daybreak/guisong/parsedData/$1/
cd /home/ubuntu/airflow/dags/sql
rm copy.sql
rm createTable.sql
echo "All The Work Had Upload, So Delete Them"