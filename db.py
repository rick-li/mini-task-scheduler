import psycopg2
import os

password = os.environ.get('DB_PASSWORD')

db_params = {
    'dbname': 'rickstest',
    'user': 'avnadmin',
    'password': password,
    'host': 'pg-3dfb3b5c-racke1983cn-ebca.a.aivencloud.com',
    'port': '24346'
}

def get_conn():
    conn = psycopg2.connect(**db_params)
    return conn