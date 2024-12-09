import time
import pandas as pd
import json
import os

#autenticação token 
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from .get_secrets import load_secrets

# inicializa constantes
secrets = load_secrets()
CLIENT_ID = secrets['CLIENT_ID']
CLIENT_SECRET = secrets['CLIENT_SECRET']
USERNAME = secrets['USERNAME']
PASSWORD = secrets['PASSWORD']
TOKEN_URL = secrets['TOKEN_URL']
SEARCH_URL = secrets['SEARCH_URL']
# Teste (não exibir em produção informações sensíveis)
print(f"CLIENT_ID: {CLIENT_ID}, USERNAME: {USERNAME}, TOKEN_URL: {TOKEN_URL}, SEARCH_URL: {SEARCH_URL}")

# Obtém o diretório do arquivo atual
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH_TOKEN = os.path.join(BASE_DIR, '..', 'token.txt')
PATH_EXPIRES_AT = os.path.join(BASE_DIR, '..', 'expires_at.txt')


def get_token():
    
    try:
        # Abre o arquivo com o token no formato txt
        with open(PATH_TOKEN, 'r') as token_file:
            token = token_file.read()

        # Abre o arquivo com o tempo para expiração do token
        with open(PATH_EXPIRES_AT, 'r') as expires_at_file:
            expires_at = float(expires_at_file.read())
    except FileNotFoundError:
        print("\n*** Arquivos de token ou expiração não encontrados. Gerando novo token... ***")
        return renew_token()
    
    # Verifica se o token está próximo de expirar
    if time.time() > (expires_at - 10):
        print("\n*** Token expirado ou próximo de expirar. Gerando novo token... ***")
        return renew_token()
    else:
        print("\n*** Token atual ainda é válido. ***")
        return token


def renew_token():
    try:
        # Configuração do cliente OAuth2
        oauth = OAuth2Session(client=LegacyApplicationClient(client_id=CLIENT_ID))
        token = oauth.fetch_token(
            token_url=TOKEN_URL,
            username=USERNAME,
            password=PASSWORD,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            verify=False
        )

        # Salva o tempo de expiração do token
        expires_at = str(token['expires_at'])
        with open(PATH_EXPIRES_AT, 'w') as expires_at_file:
            expires_at_file.write(expires_at)

        # Salva o novo token
        access_token = str(token['access_token'])
        with open(PATH_TOKEN, 'w') as token_file:
            token_file.write(access_token)

        print("\n*** Novo token gerado com sucesso! ***")
        return access_token
    except Exception as e:
        print(f"\n*** Erro ao gerar novo token: {e} ***")
        raise
    
    
# if __name__ == "__main__":
    # print(get_token())