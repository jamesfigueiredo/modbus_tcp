import pandas as pd
from modbus.client import ModbusTCPClient
from prediction.get_modbus_config import load_configs
from prediction.sarima import prediction_sarimax
import json
import os

data_mudbus = load_configs()
    
def read_values():
    for tag, config in data_mudbus.items():
        clp = ModbusTCPClient(config["ip"], config["rack"], config["slot"], config["db"])
        clp.connect()
        tags = [f"{tag} [{i}]" for i in range(24)]
        clp.read(tags, start_offset=0)
        clp.disconnect()

def write_values(df):
    for tag, config in data_mudbus.items():
        clp = ModbusTCPClient(config["ip"], config["rack"], config["slot"], config["db"])
        clp.connect()
        valores = df[tag].tolist()
        print(valores)
        clp.write(valores, start_offset=0)
        clp.disconnect()
        
        
def main():
    df = prediction_sarimax()
    read_values()
    write_values(df)
    read_values()
        

if __name__ == '__main__':
    main()

