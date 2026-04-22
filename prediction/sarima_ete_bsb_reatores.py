from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from api_historian.get_data_api import get_precipitation_data
from api_historian.get_token_api import get_token


TAGS_ETE_BSB_REATORES = [
    "ETE.BSB.001.REA.FIT.000.001",
    "ETE.BSB.001.REA.FIT.000.002",
    "ETE.BSB.001.REA.FIT.000.003",
    "ETE.BSB.001.REA.FIT.000.004",
]


WEEKLY_MODEL = {"order": (1, 1, 1), "seasonal_order": (1, 0, 1, 24 * 7)}
DAILY_FALLBACK_MODEL = {"order": (1, 1, 1), "seasonal_order": (1, 1, 1, 24)}


def prediction_sarimax_ete_bsb_reatores(
    days_history: int = 84,
    steps: int = 24,
    same_weekday_only: bool = False,
) -> pd.DataFrame:
    get_token()

    start_date = str(dt.today().date() - timedelta(days=days_history))
    end_date = str(dt.today().date() - timedelta(days=1))
    start_hour = "00:00:00"
    end_hour = "23:59:59"

    df = get_precipitation_data(
        TAGS_ETE_BSB_REATORES,
        start_date,
        start_hour,
        end_date,
        end_hour,
    )

    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
    if same_weekday_only:
        current_weekday = dt.today().weekday()
        df = df[df["TimeStamp"].dt.weekday == current_weekday]

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

    for tag in media_horaria["TagName"].unique():
        ts = media_horaria[media_horaria["TagName"] == tag]
        ts = ts.set_index("TimeStamp")["Value"]

        if len(ts) < 48:
            print(f"Poucos dados para {tag}")
            continue

        try:
            modelo = SARIMAX(
                ts,
                order=WEEKLY_MODEL["order"],
                seasonal_order=WEEKLY_MODEL["seasonal_order"],
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False)
        except Exception:
            modelo = SARIMAX(
                ts,
                order=DAILY_FALLBACK_MODEL["order"],
                seasonal_order=DAILY_FALLBACK_MODEL["seasonal_order"],
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False)

        previsao = modelo.forecast(steps=steps)
        previsao.index = indice_previsao
        previsoes_tabela[tag] = previsao.round(2).values

    previsoes_tabela.index = indice_previsao
    return previsoes_tabela


def prediction_next_hour_ete_bsb_reatores(
    days_history: int = 84,
    same_weekday_only: bool = False,
    print_24h: bool = False,
) -> pd.DataFrame:
    forecast_24h = prediction_sarimax_ete_bsb_reatores(
        days_history=days_history,
        steps=24,
        same_weekday_only=same_weekday_only,
    )

    if print_24h:
        print("\nPrevisao 24h (auditoria):")
        print(forecast_24h)

    if forecast_24h.empty:
        return forecast_24h

    return forecast_24h.iloc[[0]]
