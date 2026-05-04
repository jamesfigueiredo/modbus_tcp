import json
import warnings

import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning

from api_historian.get_data_api import PATH_TOKEN, SEARCH_URL
from api_historian.get_token_api import get_token

warnings.simplefilter("ignore", InsecureRequestWarning)

TAG = "ETE.BSB.001.REA.FIT.000.001"
CALCULATION_MODE = 15
COUNT = 1
INTERVAL_MS = 15 * 60 * 1000
DAYS_HISTORY = 7


def build_window(days_history: int):
    forecast_start = pd.Timestamp.now().floor("15min")
    history_end = forecast_start - pd.Timedelta(seconds=1)
    history_start = forecast_start - pd.Timedelta(days=days_history)
    return history_start, history_end, forecast_start


def main():
    get_token()

    history_start, history_end, forecast_start = build_window(DAYS_HISTORY)
    start_iso = history_start.strftime("%Y-%m-%dT%H:%M:%S-03:00")
    end_iso = history_end.strftime("%Y-%m-%dT%H:%M:%S-03:00")

    with open(PATH_TOKEN, "r") as token_file:
        api_token = token_file.read().strip()

    headers = {"Accept": "application/json", "Authorization": f"Bearer {api_token}"}
    url = (
        f"{SEARCH_URL}/historian-rest-api/v1/datapoints/calculated/"
        f"{TAG}/{start_iso}/{end_iso}/{CALCULATION_MODE}/{COUNT}/{INTERVAL_MS}"
    )

    print("forecast_start:", forecast_start)
    print("history_start:", history_start)
    print("history_end:", history_end)
    print("url:", url)

    response = requests.get(url, headers=headers, verify=False)
    print("status:", response.status_code)
    print("content-type:", response.headers.get("content-type"))
    print("body preview:")
    print(response.text[:8000])

    try:
        payload = response.json()
    except Exception as exc:
        print("json parse error:", exc)
        return

    if isinstance(payload, dict):
        print("top-level keys:", list(payload.keys()))
        data = payload.get("Data", [])
        print("data items:", len(data))

        if data:
            first = data[0]
            if isinstance(first, dict):
                print("first data keys:", list(first.keys()))
                samples = first.get("Samples", [])
                print("sample count:", len(samples))
                if samples:
                    print("first sample:")
                    print(json.dumps(samples[0], indent=2, ensure_ascii=False))
                    print("last sample:")
                    print(json.dumps(samples[-1], indent=2, ensure_ascii=False))
    else:
        print("payload type:", type(payload).__name__)


if __name__ == "__main__":
    main()
