#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Script isolado para gerar previsao do proximo slot de 15 minutos

from modbus.client import ModbusTCPClient
from prediction.sarima_ete_bsb_001_reatores import prediction_next_15min_ete_bsb_001_reatores


IP_CLP = "172.16.51.30"
RACK = 0
SLOT = 3
DB = 3

TAG_OFFSETS = {
    "ETE.BSB.001.REA.FIT.000.001": 14,
    "ETE.BSB.001.REA.FIT.000.002": 18,
    "ETE.BSB.001.REA.FIT.000.003": 22,
    "ETE.BSB.001.REA.FIT.000.004": 26,
}


def write_next_15min_to_clp(next_slot_df):
    if next_slot_df.empty:
        print("Sem previsao para escrita no CLP.")
        return

    clp = ModbusTCPClient(IP_CLP, RACK, SLOT, DB)
    clp.connect()
    try:
        for tag, offset in TAG_OFFSETS.items():
            if tag not in next_slot_df.columns:
                print(f"Tag sem previsao: {tag}")
                continue

            value = float(next_slot_df[tag].iloc[0])
            clp.write([value], start_offset=offset)
            print(f"Escrito {tag}={value} em start_offset={offset}")
    finally:
        clp.disconnect()


def main():
    next_slot_df = prediction_next_15min_ete_bsb_001_reatores(
        days_history=7,
        same_weekday_only=False,
        print_horizon=False,
        horizon_steps=1,
    )
    print("\nSaida operacional (proximo slot de 15 min):")
    print(next_slot_df)
    write_next_15min_to_clp(next_slot_df)


if __name__ == "__main__":
    main()
