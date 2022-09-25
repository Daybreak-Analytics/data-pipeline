from requests import Session
import os
import json
import requests

def Login(session):
    response = session.post('http://127.0.0.1/api/v1/security/login', json={
        "username": "admin",
        "password": "admin",
        "provider": "db",
        "refresh": "true"
    })
    tokens = response.json()
    return tokens.get("access_token")


def TokenCSRF(session, access_token):
    url_get = 'http://127.0.0.1/api/v1/security/csrf_token/'
    response1 = session.get(url_get, headers={"authorization": "Bearer " + access_token})
    response_str = response1.content.decode('utf-8')
    json_csrf = json.loads(response_str)
    csrf_value = json_csrf['result']
    return (csrf_value)


def DeleteDashboard(session, id_dashb, access_token, csrf_token):
    url_delete = 'http://127.0.0.1/api/v1/dashboard/' + str(id_dashb)
    response1 = session.delete(url_delete,
                               headers={"authorization": "Bearer " + access_token,
                                        "X-CSRFToken": csrf_token
                                        })
    print(response1)


def DeleteDataSet(session, access_token, csrf_token):
    dataset = session.get("http://18.232.52.28/api/v1/dataset/?q=(filters:!((col:schema,opr:eq,value:mainnet14),(col:sql,opr:dataset_is_null_or_empty,value:!t)),order_column:changed_on_delta_humanized,order_direction:desc,page:0,page_size:100)",
        headers={"authorization": "Bearer " + access_token,
                 "X-CSRFToken": csrf_token
                 })

    dataset = json.loads(dataset.text)
    print(dataset['ids'])
    ids = str(dataset['ids']).replace("[","(").replace("]",")").replace(" ","")
    url_delete = 'http://127.0.0.1/api/v1/dataset/' + "?q=!" + ids
    response1 = session.delete(url_delete,
                               headers={"authorization": "Bearer " + access_token,
                                        "X-CSRFToken": csrf_token
                                        })
    print(response1.text)


# {"database":6,"schema":"mainnet14","table_name":"a_01ab36aaf654a13e_rariblenft_mint"}
def AddDataSet(session, access_token, csrf_token, table_name, schema):
    # print(access_token,csrf_token)
    url_add_dataset = 'http://127.0.0.1/api/v1/dataset/'
    response = session.post(
        url_add_dataset,
        headers={"authorization": "Bearer " + access_token,
                 "X-CSRFToken": csrf_token
                 },

        json={"database": 1,
              "schema": schema,
              "table_name": table_name
              }
    )

    print(response)
    print(response.text)







# DeleteDataSet(session,access_token, csrf_token)


with open('/home/ubuntu/airflow/dags/sql/createTable.sql') as f:
    tables = f.readlines()
table_names = []
for i in tables:
    table_names.append(i.split(' ')[5].replace("test.","")+ "\n")

for table_name in table_names:
    table_name = table_name.replace("\n","")
    print('Processing : ' + table_name.lower())
    session = Session()
    access_token = Login(session)
    # print(access_token)
    csrf_token = TokenCSRF(session, access_token)
    AddDataSet(session,access_token, csrf_token,table_name.lower(),"test")
