from datetime import datetime as dt

import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from api_historian.get_data_api import get_precipitation_data
from api_historian.get_token_api import get_token


TAGS_ETE_BSB_001_REATORES = [
    "ETE.BSB.001.REA.FIT.000.001",
    "ETE.BSB.001.REA.FIT.000.002",
    "ETE.BSB.001.REA.FIT.000.003",
    "ETE.BSB.001.REA.FIT.000.004",
]


FORECAST_FREQ = "15min"
MIN_POINTS = 96 * 3
MAX_INTERPOLATION_GAP = 4

PRIMARY_MODEL = {"order": (1, 1, 1), "seasonal_order": (1, 1, 1, 96)}
FALLBACK_MODEL = {"order": (1, 1, 1), "seasonal_order": (0, 1, 1, 96)}
LAST_RESORT_MODEL = {"order": (1, 1, 1), "seasonal_order": (0, 0, 0, 0)}


def _resolve_collection_window(days_history: int):
    forecast_start = pd.Timestamp.now().floor(FORECAST_FREQ)
    history_end = forecast_start - pd.Timedelta(seconds=1)
    history_start = forecast_start - pd.Timedelta(days=days_history)

    return {
        "start_date": history_start.strftime("%Y-%m-%d"),
        "start_hour": history_start.strftime("%H:%M:%S"),
        "end_date": history_end.strftime("%Y-%m-%d"),
        "end_hour": history_end.strftime("%H:%M:%S"),
        "forecast_start": forecast_start,
        "history_end": history_end,
    }


def _prepare_15min_series(
    df: pd.DataFrame,
    tag: str,
    history_end: pd.Timestamp,
    same_weekday_only: bool,
) -> pd.Series:
    ts = df[df["TagName"] == tag].copy()
    if ts.empty:
        return pd.Series(dtype=float)

    ts["TimeStamp"] = pd.to_datetime(ts["TimeStamp"])
    ts["Value"] = pd.to_numeric(ts["Value"], errors="coerce")
    ts = ts.dropna(subset=["TimeStamp", "Value"])
    ts.loc[ts["Value"] < 0, "Value"] = 0

    if same_weekday_only:
        current_weekday = dt.today().weekday()
        ts = ts[ts["TimeStamp"].dt.weekday == current_weekday]

    if ts.empty:
        return pd.Series(dtype=float)

    ts = ts.sort_values("TimeStamp").set_index("TimeStamp")["Value"]
    ts = ts.resample(FORECAST_FREQ).mean()

    expected_start = ts.index.min().floor(FORECAST_FREQ)
    expected_end = history_end.floor(FORECAST_FREQ)
    expected_index = pd.date_range(start=expected_start, end=expected_end, freq=FORECAST_FREQ)

    ts = ts.reindex(expected_index)
    ts = ts.interpolate(limit=MAX_INTERPOLATION_GAP, limit_direction="both")
    ts = ts.dropna()

    return ts


def _fit_model_with_fallback(ts: pd.Series):
    last_error = None

    for model_config in (PRIMARY_MODEL, FALLBACK_MODEL, LAST_RESORT_MODEL):
        try:
            return SARIMAX(
                ts,
                order=model_config["order"],
                seasonal_order=model_config["seasonal_order"],
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False)
        except Exception as error:
            last_error = error

    raise last_error


def prediction_sarimax_ete_bsb_001_reatores(
    days_history: int = 28,
    steps: int = 1,
    same_weekday_only: bool = False,
) -> pd.DataFrame:
    get_token()

    window = _resolve_collection_window(days_history)
    df = get_precipitation_data(
        TAGS_ETE_BSB_001_REATORES,
        window["start_date"],
        window["start_hour"],
        window["end_date"],
        window["end_hour"],
        check_current_value=False,
    )

    if df.empty:
        return pd.DataFrame()

    forecast_index = pd.date_range(
        start=window["forecast_start"],
        periods=steps,
        freq=FORECAST_FREQ,
    )
    previsoes_tabela = pd.DataFrame(index=forecast_index)

    for tag in TAGS_ETE_BSB_001_REATORES:
        ts = _prepare_15min_series(
            df,
            tag,
            history_end=window["history_end"],
            same_weekday_only=same_weekday_only,
        )

        if len(ts) < MIN_POINTS:
            print(f"Poucos dados para {tag}")
            continue

        try:
            modelo = _fit_model_with_fallback(ts)
            previsao = modelo.forecast(steps=steps)
        except Exception as error:
            print(f"Falha ao ajustar modelo para {tag}: {error}")
            continue

        previsao.index = forecast_index
        previsoes_tabela[tag] = previsao.round(2).values

    return previsoes_tabela


def prediction_next_15min_ete_bsb_001_reatores(
    days_history: int = 28,
    same_weekday_only: bool = False,
    print_horizon: bool = False,
    horizon_steps: int = 8,
) -> pd.DataFrame:
    forecast = prediction_sarimax_ete_bsb_001_reatores(
        days_history=days_history,
        steps=horizon_steps,
        same_weekday_only=same_weekday_only,
    )

    if print_horizon:
        print("\nPrevisao 15 min (auditoria):")
        print(forecast)

    if forecast.empty:
        return forecast

    return forecast.iloc[[0]]
