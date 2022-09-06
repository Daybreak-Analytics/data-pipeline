cd /home/ubuntu/jobs/pyspark
echo "Create Sql table command --  : $1"
python3 createSqlCommand.py $1
echo "Upload Sql command to S3,  : $1"
aws s3 cp ./createTable.sql  s3://daybreak/guisong/parsedData/$1/
echo "Process Raw Data and Creat SQL Command Done"
mv ./createTable.sql /home/ubuntu/airflow/dags/sql/


