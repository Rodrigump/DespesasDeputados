"""Projeto em Python que extrai informações de despesas dos deputados federais da 57ª legislatura (2023-2026)
com base na API Dados Abertos da Câmara dos Deputados

Autor: Rodrigo Rossi dos Santos
Data: 04/09/2024
"""

from dotenv import load_dotenv
import json
import os
import pandas
import requests
import sqlite3

def get_dataframe(query_string): #Função que converte a resposta da API para um dataframe Pandas

    load_dotenv() #Carrega variáveis de ambiente

    url = os.getenv('url')

    endpoint = f'{url}{query_string}'

    r = requests.get(endpoint) #Faz a requisição à API

    response_json = json.loads(r.text) #Converte o texto da request para um dict

    df = pandas.DataFrame(response_json['dados']) #Converte o dict para o dataframe Pandas

    return df

def get_despesas(deputados_df): #Função que obtém os gastos de cada deputado por ano

    deputados_id = deputados_df['id'] #Lista de ids dos deputados na API para obter as despesas

    despesas = []

    for did in deputados_id: #Para cada deputado e ano da legislatura (2023-2026), obtém as despesas
        for ano in range(2023, 2027):
            despesa_deputado = get_dataframe(f'deputados/{did}/despesas?ano={ano}')
            despesa_deputado['id'] = did
            despesas.append(despesa_deputado)

    return pandas.concat(despesas) #Concatena em um único dataframe Pandas

def df_to_sql(conn, df, tablename): #Função que converte o dataframe Pandas para uma tabela no SQLite

    df.to_sql(name=tablename, con=conn, if_exists='replace')

if __name__ == '__main__':

    conn = sqlite3.connect('dados.db') #Cria conexão SQLite

    deputados = get_dataframe('deputados') #Cria dataframe deputados

    despesas = get_despesas(deputados) #Cria dataframe despesas

    df_to_sql(conn, deputados, 'deputados') #Cria tabela deputados

    df_to_sql(conn, despesas, 'despesas') #Cria tabela despesas

