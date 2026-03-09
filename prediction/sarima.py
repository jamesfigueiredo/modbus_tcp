from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from api_historian.get_data_api import get_precipitation_data
from api_historian.get_token_api import get_token

get_token()

def prediction_sarimax():

    lista_tags = [
        'EEB.LNT.001.FIT.001.000.000',
        'EEB.LNT.003.FIT.001.000.000',
        'EEB.LNT.004.FIT.001.000.000',
        # 'EEB.ASN.002.FIT.001.000.000',
        # 'EEB.GUA.005.FIT.001.000.000',
        # 'EEB.SEN.001.FIT.001.000.000',
    ]

    start_date = str(dt.today().date() - timedelta(days=14))
    end_date   = str(dt.today().date() - timedelta(days=1))
    start_hour = "00:00:00"
    end_hour   = "23:59:59"

    df = get_precipitation_data(lista_tags, start_date, start_hour, end_date, end_hour)

    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    df = df.set_index("TimeStamp")

    # =========================
    # MÉDIA HORÁRIA
    # =========================
    media_horaria = (
        df.groupby('TagName')['Value']
        .resample('1h')
        .mean()
        .reset_index()
    )

    previsoes_tabela = pd.DataFrame()

    hoje = pd.Timestamp(dt.today().date())

    indice_previsao = pd.date_range(
        start=hoje,
        periods=24,
        freq='h'
    )

    # =========================
    # LOOP POR TAG
    # =========================
    for tag in media_horaria['TagName'].unique():

        ts = media_horaria[media_horaria['TagName'] == tag]

        ts = ts.set_index('TimeStamp')['Value']

        if len(ts) < 48:
            print(f"Poucos dados para {tag}")
            continue

        modelo = SARIMAX(
            ts,
            order=(1,1,1),
            seasonal_order=(1,1,1,24),
            enforce_stationarity=False,
            enforce_invertibility=False
        ).fit(disp=False)

        previsao_24h = modelo.forecast(steps=24)

        previsao_24h.index = indice_previsao

        previsoes_tabela[tag] = previsao_24h.round(2).values

    previsoes_tabela.index = indice_previsao


    return previsoes_tabela