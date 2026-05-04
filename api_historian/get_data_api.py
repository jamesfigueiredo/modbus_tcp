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
        print(df)
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


def get_precipitation_data(lista_tags, start_date, start_hour, end_date, end_hour):
    # Ler o token de autenticação
    with open(PATH_TOKEN, 'r') as token_file:
        api_token = token_file.read().strip()
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {api_token}'}
    
    print(f"Start Date: {start_date}, End Date: {end_date}")

    # Calcular o número de amostras com base no intervalo de tempo
    start_datetime = pendulum.parse(f"{start_date}T{start_hour}", tz="America/Sao_Paulo")
    end_datetime = pendulum.parse(f"{end_date}T{end_hour}", tz="America/Sao_Paulo")
    # Quando recuperamos dados do WinCC, esses chegam com o trúpulo de amostras, por isso 
    total_minutes = (end_datetime.diff(start_datetime).in_minutes() * 3) + 1
    
    start = pd.to_datetime(f'{start_date} {start_hour}')#.tz_localize("America/Sao_Paulo")
    end = pd.to_datetime(f'{end_date} {end_hour}')#.tz_localize("America/Sao_Paulo")
    
    # Gerar intervalo com pandas
    datetime_range = pd.date_range(start=start, end=end, freq='T')
    # if media in ['***Horária***', '***Diária***']:
        # Divisão padrão em intervalos de 5 minutos
    numero_amostras = str(total_minutes)
    # else:
    #     numero_amostras = str(total_minutes // 5)
    print(f"start_datetime: {start_datetime}, end_datetime: {end_datetime}")
    print(f"Total Minutos: {total_minutes}\nNúmero de Amostras: {numero_amostras}")
    print(f'DateTime com Pandas: {datetime_range}')
    print(f'Lista de Tags:\n{lista_tags}')
    
    start_iso = f"{start_date}T{start_hour}-03:00"
    end_iso = f"{end_date}T{end_hour}-03:00"
    
    print(f"{start_iso} - {end_iso}")

    # Listas para armazenar os dados
    tagname_list, timestamp_list, value_list, quality_list = [], [], [], []
    
    try:
        # Buscar dados para cada tag
        for tag in lista_tags:
            # Criar URL de consulta
            # /v1/datapoints/raw/{tagNames}/{start}/{end}/{direction}/{count}
            url = f"{SEARCH_URL}/historian-rest-api/v1/datapoints/raw/{tag}/{start_iso}/{end_iso}/{1}/{numero_amostras}"
            # url = f"{SEARCH_URL}/historian-rest-api/v1/datapoints/interpolated/{tag}/{start_iso}/{end_iso}/{numero_amostras}/60000"
            #/v1/datapoints/currentvalue/{tagNames}The Current Value API queries the letest value data and reads the letest values for a list of tags
            url_veryfy_letest_value = f"{SEARCH_URL}/historian-rest-api/v1/datapoints/currentvalue/{tag}"
            
            print(f"URL último valor: {url_veryfy_letest_value}\nURL consulta dados: {url}")
    
            try:
                # Antes de qualquer consulta verificar se temos valores no período solicitado
                response_letest_value = requests.get(url_veryfy_letest_value, headers=headers, verify=False)
                response_letest_value.raise_for_status()
                letest_value = response_letest_value.json()
                sample_latest_value = letest_value['Data'][0].get('Samples', [])
                time_latest = sample_latest_value[0]['TimeStamp']
                # Converter para datetime e ajusta para UTC-3
                dt_local = pd.to_datetime(time_latest).tz_convert('America/Sao_Paulo')
                #Formatar pt-BR
                time_latest_formated = dt_local.strftime('%d/%m/%Y %H:%M')
                # print(f"sample_latest_value:\n{time_latest_formated}")
                
                response = requests.get(url, headers=headers, verify=False)
                response.raise_for_status()  # Levanta erro se o status não for 200
                valores = response.json()
                
                # com o endpoint raw foi necessário verificar se temos dados para tag no períod de consulta.
                samples = valores['Data'][0].get('Samples', [])
                # print(f'response:{response} - valores: {valores} - ')
                # print(f"Samples: {samples}")
                if not samples:
                    # st.warning(f"⚠️ Nenhum dado encontrado para a Estação no período selecionado: {tag}")
                    # st.success(f'Último dado válido em: {time_latest_formated}')
                    samples = False
                    # return False
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
        # Retirar valores negativos
        df["Value"] = pd.to_numeric(df['Value'], errors="coerce")
        df.loc[df['Value'] < 0, 'Value'] = 0

        # Ajustar formato do timestamp
        df['TimeStamp'] = df['TimeStamp'].str.replace(r'\.\d+Z', '', regex=True)
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        df['TimeStamp'] = df['TimeStamp'] - pd.Timedelta(hours=3)
        # Somente valores Good
        df = df[df['Quality'] == 3]
        # Filtrar para não mostrar amostras além do período
        df = df[df['TimeStamp'] <= end] 
        df['TimeStamp'] = df['TimeStamp'].apply(lambda x: x.isoformat())
        
        
        # Antes de devolver os dados brutos, fazer o tratamento para retirar valores bd e zeros
        # Tratamento de zeros retirada de valores com Bad.        
        return df
    
    except AttributeError as e:
        return(f'Algo deu errado!\n{e}')


def get_calculated_data_average_15min(
    lista_tags,
    start_date,
    start_hour,
    end_date,
    end_hour,
    calculation_mode=6,
):
    with open(PATH_TOKEN, 'r') as token_file:
        api_token = token_file.read().strip()
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {api_token}'}

    start = pd.to_datetime(f'{start_date} {start_hour}')
    end = pd.to_datetime(f'{end_date} {end_hour}')
    start_iso = f"{start_date}T{start_hour}-03:00"
    end_iso = f"{end_date}T{end_hour}-03:00"

    interval_ms = 15 * 60 * 1000
    count = int(((end - start).total_seconds() // (15 * 60)) + 1)

    tagname_list, timestamp_list, value_list, quality_list = [], [], [], []

    try:
        for tag in lista_tags:
            url = (
                f"{SEARCH_URL}/historian-rest-api/v1/datapoints/calculated/"
                f"{tag}/{start_iso}/{end_iso}/{calculation_mode}/{count}/{interval_ms}"
            )
            print(f"URL calculated average 15min: {url}")

            try:
                response = requests.get(url, headers=headers, verify=False)
                response.raise_for_status()
                valores = response.json()
            except requests.exceptions.RequestException as e:
                response_text = ""
                if hasattr(e, "response") and e.response is not None:
                    response_text = e.response.text
                print(f"Erro ao consultar API calculated para a tag {tag}: {e}")
                if response_text:
                    print(f"Resposta da API calculated: {response_text}")
                continue
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON calculated para a tag {tag}: {e}")
                continue

            for data in valores.get('Data', []):
                tagname = data.get('TagName', tag)
                for sample in data.get('Samples', []):
                    tagname_list.append(tagname)
                    timestamp_list.append(sample.get('TimeStamp'))
                    value_list.append(sample.get('Value'))
                    quality_list.append(sample.get('Quality'))

        df = pd.DataFrame({
            'TagName': tagname_list,
            'TimeStamp': timestamp_list,
            'Value': value_list,
            'Quality': quality_list,
        })

        if df.empty:
            return df

        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        df.loc[df['Value'] < 0, 'Value'] = 0
        df['TimeStamp'] = df['TimeStamp'].str.replace(r'\.\d+Z', '', regex=True)
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        df['TimeStamp'] = df['TimeStamp'] - pd.Timedelta(hours=3)

        df = df[(df['TimeStamp'] >= start) & (df['TimeStamp'] <= end)]
        df['TimeStamp'] = df['TimeStamp'].apply(lambda x: x.isoformat())
        return df

    except AttributeError as e:
        return(f'Algo deu errado!\n{e}')
    except Exception as e:
        return(f'Erro ao consultar dados calculated average 15min!\n{e}')