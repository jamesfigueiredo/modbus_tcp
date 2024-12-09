import json
import os
import pandas as pd
import requests
import pendulum
from pathlib import Path

from .get_secrets import load_secrets


# inicializa constantes
secrets = load_secrets()
SEARCH_URL = secrets['SEARCH_URL']

# Obtém o diretório do arquivo atual
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH_TOKEN = os.path.join(BASE_DIR, '..', 'token.txt')
PATH_EXPIRES_AT = os.path.join(BASE_DIR, '..', 'expires_at.txt')


def get_data_historian(lista_tags, start_date, start_hour, end_date, end_hour):
    # Ler o token de autenticação
    with open(PATH_TOKEN, 'r') as token_file:
        api_token = token_file.read().strip()
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {api_token}'}
    
    print(f"Start Date: {start_date}, End Date: {end_date}")

    # Calcular o número de amostras com base no intervalo de tempo
    start_datetime = pendulum.parse(f"{start_date}T{start_hour}", tz="America/Sao_Paulo")
    end_datetime = pendulum.parse(f"{end_date}T{end_hour}", tz="America/Sao_Paulo")
    total_minutes = start_datetime.diff(end_datetime).in_minutes() + 1

    # if media in ['***Horária***', '***Diária***']:
        # Divisão padrão em intervalos de 5 minutos
    numero_amostras = str(total_minutes // 5)
    # else:
    #     numero_amostras = str(total_minutes // 5)

    print(f"Número de Amostras: {numero_amostras}")
    
    start_iso = f"{start_date}T{start_hour}-03:00"
    end_iso = f"{end_date}T{end_hour}-03:00"

    # Listas para armazenar os dados
    tagname_list, timestamp_list, value_list, quality_list = [], [], [], []
    
    try:
        # Buscar dados para cada tag
        for tag in lista_tags:
            # Criar URL de consulta
            url = f"{SEARCH_URL}/historian-rest-api/v1/datapoints/interpolated/{tag}/{start_iso}/{end_iso}/{numero_amostras}/60000"
            print(f"URL de consulta: {url}")
            
            try:
                response = requests.get(url, headers=headers, verify=False)
                response.raise_for_status()  # Levanta erro se o status não for 200
                valores = response.json()
            except requests.exceptions.RequestException as e:
                print(f"Erro ao consultar API para a tag {tag}: {e}")
                continue
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON para a tag {tag}: {e}")
                continue

            # Processar os dados retornados
            for data in valores.get('Data', []):
                tagname = data.get("TagName", "")
                for sample in data.get('Samples', []):
                    tagname_list.append(tagname)
                    timestamp_list.append(sample.get("TimeStamp"))
                    value_list.append(sample.get("Value"))
                    quality_list.append(sample.get("Quality"))

        # Criar DataFrame a partir das listas
        df = pd.DataFrame({
            "TagName": tagname_list,
            "TimeStamp": timestamp_list,
            "Value": value_list,
            "Quality": quality_list
        })

        # Ajustar formato do timestamp
        df['TimeStamp'] = df['TimeStamp'].str.replace(r'\.\d+Z', '', regex=True)
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        df['TimeStamp'] = df['TimeStamp'] - pd.Timedelta(hours=3)
        df['TimeStamp'] = df['TimeStamp'].apply(lambda x: x.isoformat())

        # Exportar para CSV (opcional)
        # output_path = Path("historiador2.csv")
        # df.to_csv(output_path, index=False)
        # print(f"Dados exportados para: {output_path}")

        return df
    
    except AttributeError as e:
        e
        return(f'Algo deu errado!')


def get_data_sulfato(lista_tags, start_date, start_hour, end_date, end_hour):
    token_txt = open(PATH_TOKEN, 'r')
    token = token_txt.read()
    api_token = token
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer {0}'.format(api_token)}
    
    #Listas para armazenar os dados de cada propriedade
    tagname_list = []
    timestamp_list = []
    value_list = []
    quality_list = []

    numero_amostras = str((pendulum.parse(start_date+"T"+start_hour , tz = "America/Sao_Paulo")).diff(pendulum.parse(end_date+"T"+end_hour, tz = "America/Sao_Paulo")).in_minutes()+1)
    print('timestamp')
    print(f'start date: {start_date} {start_hour}')
    
    start_datetime = start_date + "T" + start_hour + '-03:00'
    end_datetime = end_date + "T" + end_hour + '-03:00'
    
    for tag in lista_tags:
        #cria o link de consulta
        parse =f'{SEARCH_URL}/historian-rest-api/v1/datapoints/interpolated/{tag}/{start_datetime}/{end_datetime}/{numero_amostras}/60000'
        print(f"PARSE:\n{parse}")
        r=requests.get(parse, headers=headers, verify=False)
        r = r.text
        valores = json.loads(r)
       
        for data in valores['Data']:
            tagname = data["TagName"]
            for sample in data['Samples']:
                tagname_list.append(tagname)
                timestamp_list.append(sample["TimeStamp"])
                value_list.append(sample["Value"])
                quality_list.append(sample["Quality"])
            # Crie o DataFrame a partir das listas
            df = pd.DataFrame({
            "TagName": tagname_list,
            "TimeStamp": timestamp_list,
            "Value": value_list,
            "Quality": quality_list
            })

    df['TimeStamp'] = df['TimeStamp'].str.replace('\.\d+Z', '', regex=True)

    #df.to_csv('Dash_sulfato.csv')
    return df