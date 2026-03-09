#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Criado segunda-feira set 26/02/2025
# @author: James Batista Figueiredo
# script para ler pluvi�metros e gravar dados no GE Proficy Historian a partir de arquivos .csv

import csv
import os
import shutil
import time
from modbus.client import ModbusTCPClient
from datetime import datetime, timedelta

from api_historian.get_token_api import get_token
from api_historian.get_data_api import get_data_historian

import logging

from pathlib import Path


# logging.getLogger("pycomm3").setLevel(logging.WARNING)  # <--- reduz verbosidade
logging.getLogger("pycomm3").setLevel(logging.CRITICAL)


from pycomm3 import LogixDriver, CommError # Biblioteca para EthernetIP da Rockwell


# Exemplo: IP_CLP: [RACK, SLOT, DB, start_offset, "TAG_HISTORIAN"], Dicion�rio gen�rico para unidades de pluviometria.
clp_data = {
    "172.16.52.30": [0, 3, 501, 6, "ETE.BSB.002.HID.PLU.000.001"], # pluviometria ETE.BSB.002
    "172.16.53.6": ["MM_CHUVA", "ETE.SB1.001.HID.PLU.000.001"],
}

# Função para gerar timestamp
def gera_timestamp():
    return datetime.now().strftime('%m/%d/%Y %H:%M')  # Formato DD/MM/YY HH:MM:SS

def pluviometric_data_save(pluviometric_data, csv_path, backup_path):# Fun��o para salvar dados na tag do historiador.
    # csv_path = 'dados_eab_brc.csv'  
    try: # primeiro tenta salvar no historian
        with open(csv_path, mode='w', newline='') as arquivo_csv:
            writer = csv.writer(arquivo_csv)
            writer.writerow(['[Data]'])  # Cabe�alho do arquivo
            writer.writerow(['Tagname', 'TimeStamp', 'Value', 'DataQuality'])  # Cabe�alho do CSV
            writer.writerows(pluviometric_data)  # Escreve os dados
            print(f"\nCSV gerado com sucesso: {csv_path}\n")
    except: # caso não consiga salva um bkp
        print(f"\nFalha ao tentar salvar arquvio: {csv_path}. Tentando salvar arquivo de bakcup...")
        try:
            with open(backup_path, mode='w', newline='') as arquivo_csv:
                writer = csv.writer(arquivo_csv)
                writer.writerow(['[Data]'])  # Cabe�alho do arquivo
                writer.writerow(['Tagname', 'TimeStamp', 'Value', 'DataQuality'])  # Cabe�alho do CSV
                writer.writerows(pluviometric_data)  # Escreve os dados
                print(f"CSV backup salvo com sucesso: {backup_path}\n")
        except:
            print(f"\nFalha ao salvar arquvio de bkp!\n")
            
def backup_push_historian(origem, destino):
    """
    Copia todos os arquivos .csv do diretório origem para o diretório destino,
    caso o diretório origem exista e esteja acessível.
    """
    try: 
        if os.path.isdir(origem):
                print(f"\nDiretório de origem encontrado: {origem}")
        else:
            print(f"\nDiretório de destino não encontrado: {origem}")
    except OSError as e:
        print(f"Erro ao acessar {origem}: {e}")
        
    try:
        if os.path.isdir(destino):
            print(f"\nDiretório de destino encontrado: {destino}. Se houver arquivos de backup, esses serão enviados ao Historian\n")
            # Percorre os arquivos do diretório de origem
            for arquivo in os.listdir(origem):
                if arquivo.lower().endswith(".csv"):
                    caminho_origem = os.path.join(origem, arquivo)
                    caminho_destino = os.path.join(destino, arquivo)
                    shutil.copy2(caminho_origem, caminho_destino)
                    print(f"\nCopiado: {arquivo}")
                    os.remove(caminho_origem)
                    print(f"Removido: {arquivo}\n")
        else:
            print(f"\nDiretório de destino não encontrado: {destino}\n")

    except OSError as e:
        print(f"Erro ao acessar {destino}: {e}")
   
def main():
    for ip, data in clp_data.items():
        
        pluviometric_data = []
        
        # print(f"Tamanho do DATA: {len(data)} - {data}")
        
        if len(data) == 5: # Se True aplicar Modbus/TCP
        
            clp = ModbusTCPClient(ip, data[0], data[1], data[2]) #ip, RACK, SLOT, DB
            clp.connect()
        
            start_offset = data[3]
            tag = [data[4]]
            
            value = clp.read(tag, start_offset)
            
            pluviometric_data.append([tag[0], gera_timestamp(), value[0], "Good"])

            print(f'CLP: {ip}\nValor lido:{pluviometric_data}')
            
            print(f'\nCLP: {ip}\nTags: {tag}\nstart_offset: {start_offset} \nvalor lido: {value}\n')


            file_name = f"{tag[0]}_{time.time()}"
            csv_path = f'/media/sht6007_incoming/{file_name}.csv'
            backup_path = f'/home/pmia/projetos/scripts_caesb/modbus_tcp/bkp_csv/{file_name}.csv'
            
            pluviometric_data_save(pluviometric_data, csv_path, backup_path)
            
        
        else:
            variable = data[0]
            tag = [data[1]]
            clp = ip
            print(f"CLP: {ip} - Tag: {tag}\n")
            try:
                with LogixDriver(clp) as plc:
                    result = plc.read(variable)
                    value = result.value
                    
                print(f"\nValor Lido: {value}\n")
                pluviometric_data.append([tag[0], gera_timestamp(), value, "Good"])

                print(f'CLP: {ip}\nValor lido:{pluviometric_data}')

                file_name = f"{tag[0]}_{time.time()}"
                csv_path = f'/media/sht6007_incoming/{file_name}.csv'
                backup_path = f'/home/pmia/projetos/scripts_caesb/modbus_tcp/bkp_csv/{file_name}.csv'
                
                pluviometric_data_save(pluviometric_data, csv_path, backup_path)
                
        
            except CommError as e:
                print( f"Erro de comunicação com CLP Rockwell: {e}")
                
    # caminhos dos arquivos de  bkp e padrao do csv para o historian
    destino = f"/media/sht6007_incoming"
    origem = f'/home/pmia/projetos/scripts_caesb/modbus_tcp/bkp_csv'
    backup_push_historian(origem, destino) # args: origem e destino

if __name__ == '__main__':
    get_token()
    main()           
