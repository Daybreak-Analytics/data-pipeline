from dataclasses import dataclass
from processRaw import *

CadenceTypeRedshiftType = {
    "Int8": "INT2",
    "Int16": "INT2",
    "Int32": "INT4",
    "Int64": "INT8",
    "Int128": "NUMERIC(38, 0)",  # 39? docs这里最大显示38?
    "Int256": "NUMERIC(38, 0)",
    "Int": "NUMERIC(38, 0)",
    "UInt8": "INT2",
    "UInt16": "INT4",
    "UInt32": "INT8",
    "UInt64": "NUMERIC(38, 0)",
    "UInt128": "NUMERIC(38, 0)",
    "UInt256": "NUMERIC(38, 0)",
    "UInt": "NUMERIC(38, 0)",
    "Word8": "INT2",
    "Word16": "INT4",
    "Word32": "INT8",
    "Word64": "NUMERIC(38, 0)",
    "Fix64": "NUMERIC(19,8)",
    "UFix64": "NUMERIC(20,8)",
    "Address": "CHAR(18)",
    "String": "VARCHAR(65535)",
    "Character": "VARCHAR(65535)",
    "None": "VARCHAR(65535)",
    "Int8NOTNULL": "INT2 NOT NULL",
    "Int16NOTNULL": "INT2 NOT NULL",
    "Int32NOTNULL": "INT4 NOT NULL",
    "Int64NOTNULL": "INT8 NOT NULL",
    "Int128NOTNULL": "NUMERIC(38, 0) NOT NULL",  # 39? docs这里最大显示38?
    "Int256NOTNULL": "NUMERIC(38, 0) NOT NULL",
    "IntNOTNULL": "NUMERIC(38, 0) NOT NULL",
    "UInt8NOTNULL": "INT2 NOT NULL",
    "UInt16NOTNULL": "INT4 NOT NULL",
    "UInt32NOTNULL": "INT8 NOT NULL",
    "UInt64NOTNULL": "NUMERIC(38, 0) NOT NULL",
    "UInt128NOTNULL": "NUMERIC(38, 0) NOT NULL",
    "UInt256NOTNULL": "NUMERIC(38, 0) NOT NULL",
    "UIntNOTNULL": "NUMERIC(38, 0) NOT NULL",
    "Word8NOTNULL": "INT2 NOT NULL",
    "Word16NOTNULL": "INT4 NOT NULL",
    "Word32NOTNULL": "INT8 NOT NULL",
    "Word64NOTNULL": "NUMERIC(38, 0) NOT NULL",
    "Fix64NOTNULL": "NUMERIC(19,8) NOT NULL",
    "UFix64NOTNULL": "NUMERIC(20,8) NOT NULL",
    "AddressNOTNULL": "CHAR(18) NOT NULL",
    "StringNOTNULL": "VARCHAR(65535) NOT NULL",
    "CharacterNOTNULL": "VARCHAR(65535) NOT NULL",
}


@udf(returnType=StringType())
def parse_type(parameter):
    event_fields = json.loads(parameter)['value']['fields']
    res = []
    for item in event_fields:
        name = item['name']
        value = item['value']['value']
        value_type = item['value']['type']
        if value_type == 'Optional':
            if not value:
                value_type = "None"
            elif isinstance(value['value'], str):
                value_type = value['type']
            else:
                value_type = 'None'
        elif isinstance(value, str):
            value_type = value_type
        else:
            value_type = "None"
        res.append(json.dumps({name: CadenceTypeRedshiftType[value_type]}))


    if not res:
        return None

    return DELIMITER.join(res)

def getValueType(datacolumns, value_type):
    # print("ValueType",value_type,type(value_type))
    for i in datacolumns:
        if i.lower() == value_type.lower():
            return 'new' + value_type
    return value_type



if __name__ == '__main__':
    date = sys.argv[1]
    print("Create Table Command, Date is ", date)
    read_path = "/home/ubuntu/jobs/extract_data/liveEvent/" + date + ".tsv"
    command_save_path = "./createTable.sql"
    table_columns = ["Date","Timestamp","BlockID","TxID","Proposer","Payer","Authorizer"]



    df = SparkSession.builder.getOrCreate().read.csv(read_path, sep=DELIMITER, header=False) \
        .toDF(BLOCK_ID, TX_ID, PROPOSER, PAYER, AUTHORIZERS, TIMESTAMP, EVENT_ID, PARAMETER) \
        .withColumn(DATE, from_unixtime(TIMESTAMP, "yyyy-MM-dd")) \
        .withColumn(EVENTS, parse_json(PARAMETER)) \
        .filter(col(EVENTS).isNotNull())\
        .withColumn(EVENTS_TYPE, parse_type(PARAMETER))

    ## 去重之后 拿到所有的{EventName:EventType}
    name_type = {}
    for i in df.dropDuplicates([EVENT_ID, EVENTS_TYPE]).select(EVENT_ID, EVENTS_TYPE).collect():
        event_name = i[EVENT_ID]
        event_type = i[EVENTS_TYPE]
        if event_name not in name_type.keys():
            name_type[event_name] = event_type
        else:
            old_type = name_type[event_name]
            new_type = event_type
            if 'None' in old_type:
                name_type[event_name] = new_type
            else:
                name_type[event_name] = old_type

    f = open(command_save_path, "w+")  # 设置文件对象
    for name in name_type:
        res = {}
        table_name = name.replace(".", "_")
        table_create_command = 'create table if not exists test.{} ( "{}" DATE NOT NULL, "{}" INT4 NOT NULL, "{}" VARCHAR(128) NOT NULL, "{}" TEXT NOT NULL, "{}" CHAR(16) NOT NULL, "{}" CHAR(16) NOT NULL, "{}" TEXT , '.format(table_name,table_columns[0],table_columns[1],table_columns[2],table_columns[3],table_columns[4],table_columns[5],table_columns[6])
        for single_type in name_type[name].split('\t'):
            item = json.loads(single_type)
            res.update(item)
        for single_type in list(res.items()):
            type_name = getValueType(table_columns,single_type[0])
            if not isinstance(single_type[1], str):
                value_type = str(single_type[1])
                table_create_command = table_create_command + \
                ' "' + type_name + '" ' + ' ' + value_type + ', ' 
            else:
                table_create_command = table_create_command + \
                ' "' + type_name + '" ' + ' ' + single_type[1] + ', ' 
        table_create_command = table_create_command[0:-2] + " );"
        f.write(table_create_command + "\n")