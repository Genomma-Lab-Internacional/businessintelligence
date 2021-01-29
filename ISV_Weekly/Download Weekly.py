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


country = 'Ecuador'
path = '../Params/'

# jsons
for json_file in [file for file in os.listdir(path) if file.endswith('.json')]:  
    with open(path + json_file, encoding='utf8') as f:
        globals()[json_file.split('.')[0].split('_')[1]] = json.load(f)

with open('../../../01Code/01ISV/Params/ISV_tokens.json', encoding='utf8') as f:
        tokens = json.load(f)

query_dates = countries['query_dates']
query_skus = countries['query_skus']


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


# ## Sales

# In[5]:


weekly_format = lambda number: '0' + str(number) if (number < 10) else str(number)

clean_date = lambda str_date: datetime(int(str_date.split('-')[2]), int(str_date.split('-')[1]), int(str_date.split('-')[0]))

clean_numbers = lambda str_numb: float(str(str_numb).replace(',',''))


# In[6]:


def unpack_data(response):
    file = urlopen(response.json()['download_url'])
    zip_file = ZipFile(BytesIO(file.read()))
    df = pd.read_csv(zip_file.open(zip_file.namelist()[0]), encoding='latin-1', sep=';')
    return df


# In[7]:


def check_status(status, date):
    if status != 200:
        return (date, status)


# In[8]:


def download_sales(anio, num_week, url, header, body):
    body["dates"] = [str(anio) + "-W" + weekly_format(num_week)]
    resp_sales = requests.post(url, data=json.dumps(body), headers=header)
    if resp_sales.status_code != 200:
        print("Oh, oh, adventurous, problems  in week " + str(num_week) + " :S")
        pass
    else:
        df = unpack_data(resp_sales)
    return df


# In[9]:


def clean_sales(data):    
    data['EAN'] = data['EAN'].astype(str).copy()
    for col in ['Unidades', 'Costos B2B']:
        data[col] = data[col].map(clean_numbers).copy()
    data['ID_SaSt'] = data['EAN'].astype(str) + data['Cadena'] + data['Local']  
    return data


# ## Stores

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


# ## Stock

# In[11]:


def download_stock(dates, chain, url, header, body):
    df_stock = pd.DataFrame()
    hierarchy_stock['Cadena'] = chain
    for date in dates:
        body["dates"] = [date.strftime('%Y-%m-%d')]
        resp_stock = requests.post(url, data=json.dumps(body), headers=header)
        if resp_stock.status_code != 200:
            print("Oh, oh, adventurous, problems in " + date.strftime('%d/%m/%Y') + ' :S')
            pass
        else:
            resp_page = urlopen(resp_stock.json()['download_url'])
            zip_file = ZipFile(BytesIO(resp_page.read()))
            df = pd.read_csv(zip_file.open(zip_file.namelist()[0]), encoding='latin-1', sep=';')
            df_stock = pd.concat([df_stock, df], axis=0)
    df_stock.reset_index(drop=True, inplace=True)
    return df_stock


# In[12]:


def iterate_stock(dates, chains):
    df = pd.DataFrame()
    for date in dates:
        df = pd.concat([df, download_stock(dates=[date], chain=[], url=url_stock, header=headers_stock, body=body_stock)], axis=0)
        for chain in chains:
            chain_stock = pd.DataFrame()
            days = 0
            if chain not in df['Cadena'].unique():                
                while chain_stock.shape[0] == 0:
                    days += 1            
                    chain_stock = download_stock(dates=[date - timedelta(days=days)], chain=[chain], url=url_stock, header=headers_stock, body=body_stock)
                    sleep(17)
                    if days > 5:
                        break
            try:
                df = pd.concat([df, chain_stock[chain_stock['Cadena'] == chain]], axis=0)
            except:
                pass
    df.reset_index(inplace=True, drop=True)
    return df


# ### Clean data

# In[13]:


def clean_stock(data):
    data_clean = data[['Fechas', 'EAN', 'Cadena', 'Sub Cadena', 'Local', 'Stock Locales en Unidades', 'Stock CD en Unidades', 'Stock en Tránsito en Unidades', 'Stock Total en Unidades']].copy()
    for col in ['Stock Locales en Unidades', 'Stock CD en Unidades', 'Stock en Tránsito en Unidades', 'Stock Total en Unidades']:
        data_clean[col] = data_clean[col].map(clean_numbers)
    data_clean['ID_SaSt'] = data['EAN'].astype(str) + data['Cód. Cadena'].astype(str) + data['Cadena'] + data['Local']
    data_clean['ID_StSt'] = data['Cadena'] + data['Sub Cadena'] + data['Local']
    return data_clean


# # Join

# In[14]:


fill = lambda col1, col2: col1 if pd.isnull(col2) else (col2 if pd.isnull(col1) else col1)


# In[15]:


def relate_dates(df1, df2):
    d = {}
    for date1 in df1:
        for date2 in df2:
            d11 = clean_date(date1.split(' ')[2])
            d12 = clean_date(date1.split(' ')[-1])
            d2 = clean_date(date2)
            if d11 <= d2 <= d12:
                d[d2.strftime('%d-%m-%Y')] = d12.strftime('%d-%m-%Y')
    return d


# In[16]:


def join_data(sales, stock, stores, df_ppst):
        join = pd.merge(sales, stock, on='ID_SaSt', how='outer')
        join['Cadena'] = join['Cadena_y'].combine(join['Cadena_x'], fill)
        join['Sub Cadena'] = join['Sub Cadena_y'].combine(join['Sub Cadena_x'], fill)
        join['Local'] = join['Local_y'].combine(join['Local_x'], fill)
        join['EAN'] = join['EAN_y'].combine(join['EAN_x'], fill)
        join['Fecha'] = join['Fecha_y'].combine(join['Fecha_x'], fill)
        join.drop(['Cadena_x', 'Cadena_y', 'Sub Cadena_x', 'Sub Cadena_y', 'Local_x', 'Local_y', 'EAN_x', 'EAN_y', 'Fecha_x', 'Fecha_y', 'ID_SaSt', 'ID_StSt'], axis=1, inplace=True)
        join.fillna(0, inplace=True)
        join2 = pd.merge(join, stores, on='Local', how='left')
        join2.rename({'EAN':'ProPstCodBarras'}, axis=1, inplace=True)
        join2 = join2[join2['ProPstCodBarras'] != 'No Definido'].copy()
        join2['ProPstCodBarras'] = join2['ProPstCodBarras'].astype('int64').astype(str)
        join3 = pd.merge(join2, df_ppst, on='ProPstCodBarras', how='left')
        return join3


# In[17]:


def status(df1, df2, df3):
    cols = ['Unidades', 'Costos B2B', 'Stock Locales en Unidades',
           'Stock CD en Unidades', 'Stock en Tránsito en Unidades',
           'Stock Total en Unidades']

    for col in cols:
        try:
            sum_equals = (df1[col].sum() == df2[col].sum())
            if sum_equals == False:
                diff = (df1[col].sum() - df2[col].sum())
        except:
            sum_equals = df1[col].sum() == df3[col].sum()
            if sum_equals == False:
                diff = (df1[col].sum() - df3[col].sum())
        if sum_equals == False:
            print(sum_equals, " - ", col, '  -  Difference:', diff)
        else:
            print(sum_equals, " - ", col)


#%%
%%time
aux = download_sales(2020, 3, url=sales['url_sales'], body=body_sales, header=sales['headers_sales'])

#%%
aux[:3]

# In[ ]:

%%time
# Semana ISV = Semana Genomma - 1
final = pd.DataFrame()
df_sales = {}
data_sales = {}
df_stock = {}
data_stock = {}
#df_stores = pd.read_excel('../../1Data/2Catalogue/SucID_41_43.xlsx')
df_stores = download_stores(url_stores, headers_stores)
stores = df_stores[['Local', 'Suc. ID']].copy()
#weeks = [datetime.today().isocalendar()[1] - i for i in range(3, 1, -1)]
weeks = [(2021, 1), (2021, 2), (2021, 3)]
#weeks = [43]
for year, week in weeks:
    ## Download the sales data
    df_sales[str(week)] = download_sales(year, week, url=sales['url_sales'], body=body_sales, header=sales['headers_sales'])
    ## Clean it
    data_sales[str(week)] = clean_sales(df_sales[str(week)])
    ## Download the stock data
    df_stock[str(week)] = iterate_stock([clean_date(df_sales[str(week)]['Semanas'].unique()[0][-10:])], df_sales[str(week)]['Cadena'].unique().tolist())       
    ## Clean it
    data_stock[str(week)] = clean_stock(df_stock[str(week)])
    ## Verify the stock data
    print("Total de stock a la semana " + str(week + 1) + ":", "\n")
    print(pd.pivot_table(data_stock[str(week)], index=['Cadena'], columns=['Fechas'], values=['Stock Locales en Unidades'], aggfunc='sum'))
    ## Assign a date to sales data and stock data
    dict_dates = relate_dates(data_sales[str(week)]['Semanas'].unique(), data_stock[str(week)]['Fechas'].unique())
    ## Format the date to sales data
    sellout = data_sales[str(week)][['Semanas', 'Cadena', 'Sub Cadena', 'Local','EAN','Unidades', 'Costos B2B', 'ID_SaSt']].copy()
    sellout['Fecha'] = sellout['Semanas'].apply(lambda x: x[-10:]).copy()
    sellout.drop(['Semanas'], axis=1, inplace=True)
    ## Format the date to stock data
    stock = data_stock[str(week)].copy()
    stock['Fecha'] = data_stock[str(week)]['Fechas'].map(dict_dates)
    stock.drop(['Fechas'], axis=1, inplace=True)
    ## Merge sales, stock and stores data
    all_data = join_data(sellout, stock, stores, df_ppst)
    ## We verify some fields
    print('\n', 'Missings por columna:\n', all_data.isnull().sum())
    print('\n', 'Códigos de Barra sin ProPstID  ', all_data[all_data['ProPstID'].isnull()]['ProPstCodBarras'].unique(), '\n')
    status(all_data, data_stock[str(week)], data_sales[str(week)])
    ## We concant into a single variable
    final = pd.concat([final, all_data], axis=0)
    print('\n\n')
    print('-----------------------------------------------------------------------')
    print('\n\n')
final.reset_index(drop=True, inplace=True)

# ## Validations

# Usualmente cuando las sucursales (_Suc. ID_) no están en ISV, vienen como _"No Definidas"_:

# In[40]:


final.shape


# In[55]:


print('min:', final['Suc. ID'].astype('int64').min(), '\n','max:', final['Suc. ID'].astype('int64').max())


# # Last Transformation

# In[60]:


new_names = {'Unidades':'SoutCantDesp',
             'Stock Locales en Unidades':'SoutCantExist',
             'Stock CD en Unidades':'SoutCantCedis',
             'Stock en Tránsito en Unidades':'SoutCantTrans',
             'Stock Total en Unidades':'SoutCantInv',
             'Costos B2B':'SoutMontoDesp',
             'Stock Locales en Precio Lista':'SoutMontoExist',
             'Stock CD en Precio Lista':'SoutMontoCedis',
             'Stock Total en Precio Lista':'SoutMontoInv',             
             'EAN_x':'ProPstCodBarras',
             'Suc. ID':'SucID',
             'Pro. Pst. ID':'ProPstID'}

final.rename(new_names, inplace=True, axis=1)


# In[61]:


final['TmpFecha'] = final['Fecha'].map(clean_date).astype(str)

final = pd.merge(final, df_tmpid, on='TmpFecha', how='left')


# In[62]:


final['SoutCantStaple'] = 0
final['SoutMontoTrans'] = 0
final['SoutMontoStaple'] = 0
final['SoutMontoExist'] = 0
final['SoutMontoCedis'] = 0
final['SoutMontoTrans'] = 0
final['SoutMontoStaple'] = 0
final['SoutMontoInv'] = 0
final['SoutMontoCteDesp'] = 0


# In[1]:


X = pd.pivot_table(final, index=['ProPstID', 'SucID', 'TmpID'], values=['SoutCantDesp', 'SoutCantExist', 'SoutCantCedis', 'SoutCantTrans', 'SoutCantStaple', 'SoutCantInv', 'SoutMontoDesp', 'SoutMontoExist', 'SoutMontoCedis', 'SoutMontoTrans', 'SoutMontoStaple', 'SoutMontoInv', 'SoutMontoCteDesp'], aggfunc='sum').reset_index().copy()


# In[2]:


print('min:', X['ProPstID'].min(), '\n','max:', X['ProPstID'].max())


# In[3]:


print('min:', X['SucID'].astype('int64').min(), '\n','max:', X['SucID'].astype('int64').max())


# **Ojo, falta agregar la parte del reproceso, los layouts y los diferentes tipos de exportación**

# # Reproceso de stock

# ***La tabla A y la tabla B no debes ser importados***

# In[ ]:


A = pd.read_excel('table_A.xlsx', sheet_name='tabla')
B = pd.read_excel('table_B.xlsx', sheet_name='tabla')

A['ID'] = A['Año_GL'].astype(str) + A['Semana_GL'].astype(str) + A['Canal'] + A['GrpNombre'] + A['ProPstCodBarras'].astype(str)
B['ID'] = B['Año_GL'].astype(str) + B['Semana_GL'].astype(str) + B['Canal'] + B['GrpNombre'] + B['ProPstCodBarras'].astype(str)

join = pd.merge(A, B, on='ID', how='right')


# In[ ]:


def fill(data, stock_old, stock_new):
    df = data.copy()
    df['Difference'] = df[stock_old] - df[stock_new]
    df['Inv_final'] = df[[stock_old, stock_new, 'Difference']].apply(lambda x: compare_stocks(*x), axis=1)
    df.drop(['Difference'], axis=1, inplace=True)
    return df


# In[ ]:


def fill(data, stock_old, stock_new):
    df = data.copy()
    df['Difference'] = df[stock_old] - df[stock_new]
    df['Inv_final'] = df[[stock_old, stock_new, 'Difference']].apply(lambda x: compare_stocks(*x), axis=1)
    df.drop(['Difference'], axis=1, inplace=True)
    return df


# aux = fill(join, 'Inventario Unidades_x', 'Inventario Unidades_y')
# 
# pd.pivot_table(aux, index=['GrpNombre_y'], columns=['Semana_GL_y'], values=['Inv_final'], aggfunc='sum', margins=True)

# In[ ]:

### En este archivo plano falta la parte de reproceso, no olvidar agregarlo y editarlo




