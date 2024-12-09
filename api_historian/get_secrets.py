import json
import os


def load_secrets():
    # Caminho do diretório atual do script
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Caminho para o diretório pai (Dashboards-CAESB)
    project_root = os.path.abspath(os.path.join(current_dir, os.pardir))

    # Caminho completo para o arquivo secrets.json
    secrets_path = os.path.join(project_root, 'secrets.json')

    # Verifica se o arquivo existe
    if not os.path.exists(secrets_path):
        raise FileNotFoundError(f"Arquivo secrets.json não encontrado: {secrets_path}")

    # Carrega o arquivo JSON
    with open(secrets_path, 'r') as f:
        return json.load(f)
