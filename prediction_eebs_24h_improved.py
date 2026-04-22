#!/usr/bin/env python
# -*- coding: utf-8 -*-

from modbus.client import ModbusTCPClient
from prediction.get_modbus_config import load_configs
from prediction.sarima_eebs_improved import prediction_sarimax_eebs_improved


data_mudbus = load_configs()


def write_values(df):
    for tag, config in data_mudbus.items():
        if df.empty or tag not in df.columns:
            print(f"Sem previsao para {tag}, escrita ignorada.")
            continue

        clp = ModbusTCPClient(config["ip"], config["rack"], config["slot"], config["db"])
        clp.connect()
        valores = df[tag].tolist()
        print(valores)
        clp.write(valores, start_offset=0)
        clp.disconnect()


def main():
    df = prediction_sarimax_eebs_improved(days_history=84, steps=24)
    if df.empty:
        print("Sem dados para previsao.")
        return

    print("\nPrevisao 24h (EEBs - improved):")
    print(df)
    write_values(df)


if __name__ == "__main__":
    main()
