import snap7
from snap7.util import set_real, get_real
import logging


class ModbusTCPClient:
    def __init__(self, ip_clp: str, rack: int, slot: int, db_number: int):
        """
        Inicializa o cliente com os parâmetros básicos para conexão.
        """
        self.ip_clp = ip_clp
        self.rack = rack
        self.slot = slot
        self.db_number = db_number
        self.client = snap7.client.Client()
        self.connected = False

        # Configuração do logger
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

    def connect(self):
        """
        Realiza a conexão com o CLP.
        """
        try:
            self.logger.info(f"Tentando conectar ao CLP {self.ip_clp}...")
            self.client.connect(self.ip_clp, self.rack, self.slot)
            self.connected = self.client.get_connected()
            if self.connected:
                self.logger.info("Conexão estabelecida com sucesso.")
            else:
                raise ConnectionError("Conexão não foi bem-sucedida.")
        except Exception as e:
            self.logger.error(f"Erro ao conectar: {e}")
            raise

    def disconnect(self):
        """
        Desconecta do CLP.
        """
        if self.connected:
            self.client.disconnect()
            self.logger.info("Conexão encerrada.")
            self.connected = False

    def read(self, tag_list: list, start_offset: int = 0) -> list:
        """
        Lê valores do CLP com base nos tags fornecidos.
        """
        
        # Calcula o comprimento dos dados com base no número de tags
        data_length = len(tag_list) * 4  # Cada tag ocupa 4 bytes

        if not self.connected:
            raise ConnectionError("Cliente não está conectado ao CLP.")

        try:
            self.logger.info(f"Lendo dados do CLP {self.ip_clp}")
            data_raw = self.client.db_read(self.db_number, start_offset, data_length)
            read_list = []
            offset = 0

            for tag in tag_list:
                value = get_real(data_raw, offset)
                read_list.append(f"{tag} -> {value}")
                self.logger.debug(f"Lido: {tag} -> {value}")
                offset += 4
            print (read_list)
            return read_list
        except Exception as e:
            self.logger.error(f"Erro na leitura: {e}")
            raise

    def write(self, data_list: list, start_offset: int = 0):
        """
        Escreve valores no CLP a partir da lista de dados fornecida.
        """
        if not self.connected:
            raise ConnectionError("Cliente não está conectado ao CLP.")

        try:
            self.logger.info(f"Escrevendo dados no CLP {self.ip_clp}")
            data = bytearray(len(data_list) * 4)
            offset = start_offset

            for value in data_list:
                set_real(data, offset, value)
                self.logger.debug(f"Escrito no offset {offset}: {value}")
                offset += 4

            self.client.db_write(self.db_number, start_offset, data)
            self.logger.info("Escrita realizada com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro na escrita: {e}")
            raise

    def __del__(self):
        """
        Finaliza a conexão ao destruir o objeto.
        """
        self.disconnect()
