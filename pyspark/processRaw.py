from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, udf, StringType, col
import json
import sys

EVENTS_TYPE = 'EventsType'
EVENTS = "Events"
DATE = "Date"
PARAMETER = 'Parameter'
EVENT_ID = 'EventId'
TIMESTAMP = 'Time'
AUTHORIZERS = 'Authorizers'
PAYER = 'Payer'
PROPOSER = 'Proposer'
TX_ID = 'TxID'
BLOCK_ID = 'BlockID'
DELIMITER = "\t"


@udf(returnType=StringType())
def parse_json(parameter):
    event_fields = json.loads(parameter)['value']['fields']
    res = []
    for item in event_fields:
        value = item['value']['value']
        if item['value']['type'] == 'Optional':
            if not value:
                res.append('*')
            elif isinstance(value['value'], str):
                res.append(value['value'].replace('\n',' ').replace('\t',' '))
            else:
                res.append('*')
        elif isinstance(value, str):
            if value=='':
                res.append('*')
            else:
                res.append(value.replace('\n',' ').replace('\t',' '))
        else:
            res.append('*')

    if not res:
        return None

    return DELIMITER.join(res)


if __name__ == '__main__':
    date = sys.argv[1]
    # date = '2022-09-01'
    print("Python Process Raw Date",date)
    read_path = "/home/ubuntu/jobs/extract_data/liveEvent/" + date + ".tsv"
    save_path = "./rawdata/"
    df = SparkSession.builder.getOrCreate().read.csv(read_path, sep=DELIMITER, header=False) \
        .toDF(BLOCK_ID, TX_ID, PROPOSER, PAYER, AUTHORIZERS, TIMESTAMP, EVENT_ID, PARAMETER) \
        .withColumn(DATE, from_unixtime(TIMESTAMP, "yyyy-MM-dd")) \
        .withColumn(EVENTS, parse_json(PARAMETER)) \
        .filter(col(EVENTS).isNotNull())\
        .select(DATE, TIMESTAMP, BLOCK_ID, TX_ID, PROPOSER, PAYER, AUTHORIZERS, EVENT_ID, EVENTS) \
        .repartition(1) \
        .write.option("sep", DELIMITER) \
        .partitionBy(EVENT_ID) \
        .format("csv") \
        .mode("overwrite") \
        .save(save_path)
