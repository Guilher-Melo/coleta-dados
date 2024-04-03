import json
import time

import pandas as pd
import requests


def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp


i = 0


def save_data(data, option='json'):
    global i
    i += 1
    nome_arquivo = f'arquivo_{i}'
    if option == 'json':
        with open(f'./data/contents/json/{nome_arquivo}.json', 'w') as open_file:
            json.dump(data, open_file, indent=4)
    elif option == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(
            f'data/contents/parquet/{nome_arquivo}.parquet', index=False)
        i += 1


page = 1

while True:
    resp = get_response(page=page, per_page=100, strategy='new')
    if resp.status_code == 200:
        data = resp.json()
        save_data(data)

        if len(data) < 100:
            break
        page += 1
        time.sleep(1)
    else:
        print(resp.status_code)
        time.sleep(10)
