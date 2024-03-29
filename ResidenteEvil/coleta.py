import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

url = "https://www.residentevildatabase.com/personagens/albert-wesker/"

# flake8: noqa

cookies = {
    '_gid': 'GA1.2.1341172331.1711653117',
    '__gads': 'ID=05de93deafd090b3:T=1711653141:RT=1711653141:S=ALNI_MbehElRZe37adIZt06S4xDDAYlZKg',
    '__gpi': 'UID=00000a149157b291:T=1711653141:RT=1711653141:S=ALNI_MakwA1iaPqBpDpo2KwlSEbkgq_e_Q',
    '__eoi': 'ID=de465b45b58ad11d:T=1711653141:RT=1711653141:S=AA-AfjZb40H6RErmeNYcPaKJby9n',
    '_ga': 'GA1.1.1153072576.1711653117',
    'FCNEC': '%5B%5B%22AKsRol9_d4kUvW2u0QOIxGaCesGunsMqh67lW24zN8Hgmp7CtiZioUioMEfmuDtSKJLUl-qzwWrhKmKwz9VNpuwjfWo-6UnT2rPov2CJHHFycxlcilc8aIO_tRV6g9IROccDJg1uWtijr0rSolJQBZLppC6-TGrJ5w%3D%3D%22%5D%5D',
    '_ga_DJLCSW50SC': 'GS1.1.1711653116.1.1.1711653148.28.0.0',
    '_ga_D6NF5QC4QT': 'GS1.1.1711653117.1.1.1711653148.29.0.0',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    # 'cookie': '_gid=GA1.2.1341172331.1711653117; __gads=ID=05de93deafd090b3:T=1711653141:RT=1711653141:S=ALNI_MbehElRZe37adIZt06S4xDDAYlZKg; __gpi=UID=00000a149157b291:T=1711653141:RT=1711653141:S=ALNI_MakwA1iaPqBpDpo2KwlSEbkgq_e_Q; __eoi=ID=de465b45b58ad11d:T=1711653141:RT=1711653141:S=AA-AfjZb40H6RErmeNYcPaKJby9n; _ga=GA1.1.1153072576.1711653117; FCNEC=%5B%5B%22AKsRol9_d4kUvW2u0QOIxGaCesGunsMqh67lW24zN8Hgmp7CtiZioUioMEfmuDtSKJLUl-qzwWrhKmKwz9VNpuwjfWo-6UnT2rPov2CJHHFycxlcilc8aIO_tRV6g9IROccDJg1uWtijr0rSolJQBZLppC6-TGrJ5w%3D%3D%22%5D%5D; _ga_DJLCSW50SC=GS1.1.1711653116.1.1.1711653148.28.0.0; _ga_D6NF5QC4QT=GS1.1.1711653117.1.1.1711653148.29.0.0',
    'referer': 'https://www.residentevildatabase.com/personagens/',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
}


def get_content(url):
    resp = requests.get(url, cookies=cookies, headers=headers)
    return resp


def get_basic_infos(soup):
    div_page = soup.find('div', class_='td-page-content')

    paragrafo = div_page.find_all('p')[1]

    ems = paragrafo.find_all('em')

    data = {}

    for i in ems:
        chave, valor, *_ = i.text.split(':')
        chave = chave.strip(' ')
        data[chave] = valor.strip(' ')

    return data


def get_aparicoes(soup):
    lis = (soup.find(
        'div', class_='td-page-content').find('h4').find_next().find_all('li'))

    aparicoes = [i.text for i in lis]
    return aparicoes


def get_personagem_infos(url):
    resp = get_content(url)
    if resp.status_code != 200:
        print('Não foi possível obter a página')
        return {}
    else:
        soup = BeautifulSoup(resp.text, features='html.parser')
        data = get_basic_infos(soup)
        data['Aparicoes'] = get_aparicoes(soup)
        return data


def get_links():
    url_personagens = "https://www.residentevildatabase.com/personagens/"

    resp = requests.get(url_personagens, headers=headers, cookies=cookies)
    soup_personagens = BeautifulSoup(resp.text, features='html.parser')

    ancoras = soup_personagens.find(
        'div', class_='td-page-content').find_all('a')
    links = [i['href'] for i in ancoras]
    return links


links = get_links()
data = []

for i in tqdm(links):
    d = get_personagem_infos(i)
    d['Link'] = i
    nome = i.split("/")[-1].replace('-', " ").title()
    d['Nome'] = nome
    data.append(d)


df = pd.DataFrame(data)

df.to_parquet("dados_re.parquet", index=False)

df_new = pd.read_parquet("dados_re.parquet")
print(df_new)
