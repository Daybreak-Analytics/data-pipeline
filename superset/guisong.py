from requests import Session
import os
import json

def Login(session):
    response = session.post('http://18.232.52.28:8088/api/v1/security/login', json={
        "username": "admin",
        "password": "admin",
        "provider": "db",
        "refresh": "true"
    })
    tokens = response.json()
    return tokens.get("access_token")


def TokenCSRF(session, access_token):
    url_get = 'http://18.232.52.28:8088/api/v1/security/csrf_token/'
    response1 = session.get(url_get, headers={"authorization": "Bearer " + access_token})
    response_str = response1.content.decode('utf-8')
    json_csrf = json.loads(response_str)
    csrf_value = json_csrf['result']
    return (csrf_value)


def DeleteDashboard(session, id_dashb, access_token, csrf_token):
    url_delete = 'http://18.232.52.28:8088/api/v1/dashboard/' + str(id_dashb)
    response1 = session.delete(url_delete,
                               headers={"authorization": "Bearer " + access_token,
                                        "X-CSRFToken": csrf_token
                                        })
    print(response1)

# {"database":6,"schema":"mainnet14","table_name":"a_01ab36aaf654a13e_rariblenft_mint"}
def AddDataSet(session, access_token, csrf_token, table_name):
    url_add_dataset = 'http://18.232.52.28:8088/api/v1/dataset/'
    response = session.post(
        url_add_dataset,
        headers={"authorization": "Bearer " + access_token,
                 "X-CSRFToken": csrf_token
                 },

        json={"database": 1,
              "schema": "mainnet14",
              "table_name": table_name
              }
    )

    print(response.text)

table_names = ['A_2e1ee1e7a96826ce_FantastecNFT_Withdraw']

for table_name in table_names:
    print('Processing : ' + table_name)
    session = Session()
    access_token = Login(session)
    csrf_token = TokenCSRF(session, access_token)
    print(csrf_token)
    AddDataSet(session,access_token, csrf_token,table_name.lower())