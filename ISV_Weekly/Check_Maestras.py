#!/usr/bin/env python
# coding: utf-8

# libraries

#%% 

import mysql.connector
import jupyternotify
import pandas as pd
import calendar
import requests
import pyodbc
import json
import os
from mysql.connector import errorcode
from mysql.connector import Error
from urllib.request import urlopen
from datetime import timedelta
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO
from time import sleep

ip = get_ipython()
ip.register_magics(jupyternotify.JupyterNotifyMagics)

# # Paramaters

# In[2]:


country = 'Honduras'
path = '../Params/'

# jsons
for json_file in [file for file in os.listdir(path) if file.endswith('.json')]:  
    with open(path + json_file, encoding='utf8') as f:
        globals()[json_file.split('.')[0].split('_')[1]] = json.load(f)

query_dates = countries['query_dates']
query_skus = countries['query_skus']
query_sucs = countries['query_sucs']


# In[3]:


# API parameters
## Sales
sales['headers_sales']['Authorization'] = 'ISVToken ' + tokens[country]
body_sales = sales['body_sales']
body_sales['views'] = sales['views'][country]
body_sales['hierarchy'] = sales['hierarchy_sales'][country]
body_sales['view_type'] = 'semana'

## Stock
url_stock = stocks['url_stock']
headers_stock = stocks['headers_stock']
headers_stock['Authorization'] = 'ISVToken ' + tokens[country]
hierarchy_stock = stocks['hierarchy_stock']
body_stock = stocks['body_stock']
body_stock['hierarchy'] = hierarchy_stock

## Store
url_stores = stores['url_stores']
headers_stores = stores['headers_stores']
headers_stores['Authorization'] = 'ISVToken ' + tokens[country]

# Conection to SQL Server
conn1 = pyodbc.connect('Driver={SQL Server};Server=' + serversdbs['server'] + ';Database=' + serversdbs['database1'] + ';Trusted_Connection=yes;')
conn2 = pyodbc.connect('Driver={SQL Server};Server=' + serversdbs['server'] + ';Database=' + serversdbs['database2'] + ';Trusted_Connection=yes;')


# # Load data

# In[4]:


# TmpID
df_tmpid = pd.read_sql(query_dates, conn1)
df_tmpid['TmpFecha'] = df_tmpid['TmpFecha'].astype(str).copy()

# ProPstID
df_ppst = pd.read_sql(query_skus.replace("''", "'" + country + "'"), conn2)
df_ppst['ProPstCodBarras'] = df_ppst['ProPstCodBarras'].astype(str).copy()

# Sucursales
df_sucs = pd.read_sql(query_sucs.replace("''", "'" + country + "'"), conn2)

# In[10]:


def download_stores(url, header):
    resp_stores = requests.get(url, headers=header)
    if resp_stores.status_code != 200:
        print("Oh, oh, problems body")
    else:
        df = pd.read_excel(resp_stores.json()['url'], sep='\t')
        ind_min = df[df.iloc[:,7].notnull()].index.min()
        cols = df.iloc[9,:].tolist()
        data = df.loc[ind_min + 2:].copy()
        data.reset_index(drop=True, inplace=True)
        data.rename(dict(zip(df.columns.tolist(), cols)), inplace=True, axis=1)
    return data

#%%

def check_sucs(lista):
    
    if len(lista) == 0:
        print('No hay sucursales nuevas')
        return([])
    
    #Read data
    stores_list = pd.read_sql(query_sucs.replace("''", "'" + country + "'"), conn2)
    stores_list.dropna(subset=['SucId'],inplace=True)
    stores_list['key'] = stores_list['SucCodCliente'] + '-' + stores_list['CadID'].astype(str)
    stores_list['SucId'] = stores_list['SucId'].astype('int64')
    cat_clientes = pd.read_excel('../Params/Cat_Cadenas.xlsx')
    cat_clientes = cat_clientes[cat_clientes['Pais'] == country]

    missing_stores = df_stores[df_stores.Local.isin(lista)]
    missing_stores = pd.merge(missing_stores,cat_clientes[['GrpNombre','Sub Cadena','CadNombre','CadID','SubCadenaNombre','Pais']],on='Sub Cadena',how='left')

    if missing_stores['GrpNombre'].isnull().sum() > 0:
        print('CLIENTE NO ENCONTRADO :\n')
        print(missing_stores[missing_stores['GrpNombre'].isnull()][['Sub Cadena']])
        return([])
    
    missing_stores['key'] = missing_stores['Codigo Local'].astype(str) + '-' + missing_stores['CadID'].astype(str)
    missing_stores = pd.merge(missing_stores,stores_list[['key','SucId']],on='key',how='left')

    if missing_stores['SucId'].isnull().sum() > 0:
        sucs_alta = missing_stores[missing_stores['SucId'].isnull()][['Codigo Local','Local','CadID','SubCadenaNombre']].copy()
        sucs_alta['SucNombre'] = sucs_alta['SubCadenaNombre'] + ' (' + sucs_alta['Codigo Local'].astype(str) + ')'
        sucs_alta['DirColonia'] = 'X'
        sucs_alta['DirEntreCalles'] = 'X'
        sucs_alta['CodPstID'] = '00000'
        sucs_alta['ClaTndID'] = 3

        for col in ['SucUn','SucDescripcion','SucFechaApertura','SucMetros','SucTelPrincipal','SucTelAlterno','SucFax','SucMail','SucRFC','SucURL','SucFechaIng','StaGenId','CidID','DirNumExterior','DirNumInterior']:
            sucs_alta[col] = None

        sucs_alta.rename({'Codigo Local':'SucCodCliente','Local':'DirCalle'},axis=1,inplace=True)
        sucs_alta = sucs_alta[['SucUn','SucNombre','SucDescripcion','SucFechaApertura','SucMetros','SucTelPrincipal','SucTelAlterno','SucFax','SucMail','SucRFC','SucURL','SucFechaIng','SucCodCliente','ClaTndID','CadID','StaGenId','CidID','DirCalle','DirNumExterior','DirNumInterior','DirColonia','DirEntreCalles','CodPstID']]

        sucs_alta.to_excel('Cargar Sucursales ' + country + ' (' + str(week) + ' - 2021).xlsx',index=False,encoding='latin1')
        print('Favor de dar de alta las sucursales faltantes antes de continuar...')
        return([])

    else:
        missing_stores['Insert'] = "final.loc[final['Local'] == '" + missing_stores['Local'] + "','Suc. ID'] = '" + missing_stores['SucId'].astype('int64').astype(str) + "'"
        print('Favor de evaluar los comandos para agregar las sucursales')
        return(missing_stores)




#%%
df_stores = download_stores(url_stores, headers_stores)
df_stores.rename({'Suc.ID':'Suc. ID'},axis=1,inplace=True)
stores = df_stores[['Local', 'Suc. ID']].copy()

#%%
df_stores

#%%
l = df_stores['Local'][(df_stores['Suc. ID'].astype(str).str.contains('No'))|(df_stores['Suc. ID'].astype(str)=='0')].unique().tolist()
l

#%%
week = 6
x = check_sucs(l)
x.to_csv('Sucs_honduras.csv', encoding='latin1')

#%%
x


