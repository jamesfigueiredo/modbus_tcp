from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from api_historian.get_data_api import get_precipitation_data
from api_historian.get_token_api import get_token
from prediction.get_modbus_config import load_configs


WEEKLY_MODEL = {"order": (1, 1, 1), "seasonal_order": (1, 0, 1, 24 * 7)}
DAILY_FALLBACK_MODEL = {"order": (1, 1, 1), "seasonal_order": (1, 1, 1, 24)}


def _fit_model_with_fallback(ts: pd.Series):
    try:
        return SARIMAX(
            ts,
            order=WEEKLY_MODEL["order"],
            seasonal_order=WEEKLY_MODEL["seasonal_order"],
            enforce_stationarity=False,
            enforce_invertibility=False,
        ).fit(disp=False)
    except Exception:
        return SARIMAX(
            ts,
            order=DAILY_FALLBACK_MODEL["order"],
            seasonal_order=DAILY_FALLBACK_MODEL["seasonal_order"],
            enforce_stationarity=False,
            enforce_invertibility=False,
        ).fit(disp=False)


def prediction_sarimax_eebs_improved(days_history: int = 84, steps: int = 24) -> pd.DataFrame:
    get_token()

    lista_tags = list(load_configs().keys())
    start_date = str(dt.today().date() - timedelta(days=days_history))
    end_date = str(dt.today().date() - timedelta(days=1))
    start_hour = "00:00:00"
    end_hour = "23:59:59"

    df = get_precipitation_data(lista_tags, start_date, start_hour, end_date, end_hour)
    if df.empty:
        return pd.DataFrame()

    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df.dropna(subset=["Value"])
    df.loc[df["Value"] < 0, "Value"] = 0
    df = df.set_index("TimeStamp")

    media_horaria = (
        df.groupby("TagName")["Value"]
        .resample("1h")
        .mean()
        .reset_index()
    )

    previsoes_tabela = pd.DataFrame()
    proxima_hora = pd.Timestamp.now().floor("h") + pd.Timedelta(hours=1)
    indice_previsao = pd.date_range(start=proxima_hora, periods=steps, freq="h")

    for tag in lista_tags:
        ts = media_horaria[media_horaria["TagName"] == tag]
        ts = ts.set_index("TimeStamp")["Value"]
        ts = ts.asfreq("1h").interpolate(limit_direction="both")

        if len(ts) < 48:
            print(f"Poucos dados para {tag}")
            continue

        modelo = _fit_model_with_fallback(ts)
        previsao = modelo.forecast(steps=steps)
        previsao.index = indice_previsao
        previsoes_tabela[tag] = previsao.round(2).values

    previsoes_tabela.index = indice_previsao
    return previsoes_tabela
