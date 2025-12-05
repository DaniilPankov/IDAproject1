# Установите библиотеку
# pip install pyhocon

from pyhocon import ConfigFactory
import os


def load_hocon_config(config_path: str = 'application.conf'):
    """Загрузка конфигурации из HOCON файла"""
    try:
        config = ConfigFactory.parse_file(config_path)
        return config
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")
        return None


config = load_hocon_config('application.conf')

if config:
    client_id = config.get('api.gigachat.client_id')
    auth = config.get('api.gigachat.auth')
    auth_url = config.get('api.gigachat.auth_url')
    base_url  = config.get('api.gigachat.base_url')
    cert_path = config.get('api.gigachat.cert_path')
    api_key = config.get('api.gigachat.client_id')
    num_of_vacancies_to_analyse = config.get('api.gigachat.num_of_vacancies_to_analyse')

    db_name = config.get('database.name')