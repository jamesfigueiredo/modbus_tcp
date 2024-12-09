from mudbus.client import ModbusTCPClient
from datetime import datetime, timedelta

from api_historian.get_token_api import get_token
from api_historian.get_data_api import get_data_historian

RACK = 0
SLOT = 0
DB = 3000

clp_data = {
    "172.16.98.97": ['REL.AGQ.001.FIT.002.000.000', 'REL.AGQ.001.FIT.003.000.000', 'REL.AGQ.002.FIT.001.000.000', 'REL.AGQ.002.FIT.002.000.000', 'REL.AGQ.002.PIT.001.000.000'],
    "172.16.98.98": ['UDA.AGQ.002.FIT.001.000.000', 'UDA.AGQ.002.FIT.002.000.000', 'UDA.AGQ.002.PIT.001.000.000'],
}

# Data e hora atual
now = datetime.now()

# Variáveis para amostra de 1 minuto
start_date = now.strftime("%Y-%m-%d")
start_hour = now.strftime("%H:%M:%S")
end_date = start_date
end_hour = (now + timedelta(minutes=-1)).strftime("%H:%M:%S")
   
if __name__ == '__main__':
    get_token()
    
    for ip, tags in clp_data.items():
        clp = ModbusTCPClient(ip, RACK, SLOT, DB)
        clp.connect()
    
        data_historian = get_data_historian(tags, start_date, start_hour, end_date, end_hour)
        data = data_historian["Value"]
    
        print(data_historian["Value"])
        clp.write(data)
        
        print(f'\nCLP: {ip}  -  TAGs: {tags}\n')
        
        data_length = int(len(tags)*4)
        
        clp.read(tags)
    