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

#%%
path = '../Params/'

# jsons
for json_file in [file for file in os.listdir(path) if file.endswith('.json')]:  
    with open(path + json_file, encoding='utf8') as f:
        globals()[json_file.split('.')[0].split('_')[1]] = json.load(f)

conn1 = pyodbc.connect('Driver={SQL Server};Server=SFEDWH01;Database=Gnm_MasterOp;Trusted_Connection=yes;')
conn2 = pyodbc.connect('Driver={SQL Server};Server=APPSGL;Database=Gnm_CatMaestros;Trusted_Connection=yes;')
conn3 = pyodbc.connect('Driver={SQL Server};Server=APPSGL;Database=Gnm_CIG;Trusted_Connection=yes;')


#%%
#QUERYS
comSucursales_query = '''
    SELECT TOP (1) * FROM dbo.ComSucursalesTienda order by SucID desc
'''

comDirecciones_query = '''
    SELECT TOP (1) * FROM dbo.ComDirecciones order by DirEntidadID desc
'''

#%%
#READ TABLES
CIG_comSucursales = pd.read_sql(comSucursales_query,conn3)
CIG_comDirecciones = pd.read_sql(comDirecciones_query,conn3)
CAT_comSucursales = pd.read_sql(comSucursales_query,conn2)
CAT_comDirecciones = pd.read_sql(comDirecciones_query,conn2)
MAS_comSucursales = pd.read_sql(comSucursales_query,conn1)
MAS_comDirecciones = pd.read_sql(comDirecciones_query,conn1)

#%%
alta_sucs = pd.read_excel('Cargar Sucursales CHL (2 - 2021).xlsx')

#%%
for col in ['SucUn','SucDescripcion','SucTelPrincipal','SucTelAlterno','SucFax','SucRFC','SucMail','DirNumExterior','DirNumInterior','SucURL']:
    alta_sucs.loc[alta_sucs[col].isnull(),col] = ''

# for col in ['SucFechaApertura','SucMetros','SucFechaIng']:
#     alta_sucs.loc[alta_sucs[col].isnull(),col] = None

for col in ['SucMetros']:
    alta_sucs.loc[alta_sucs[col].isnull(),col] = 0

for col in ['SucFechaApertura','SucFechaIng']:
    alta_sucs[col] = None

alta_sucs['CodPstID'] = alta_sucs['CodPstID'].apply(lambda x: x if x != 0 else '00000')

alta_sucs['StaGenID'] = 1
alta_sucs['CidID'] = 216

alta_sucs['SucNitAtlas'] = ''
alta_sucs['SucEsCedis'] = 0
alta_sucs['TipoAndID'] = None
alta_sucs['AsenID'] = None
alta_sucs['TipoEntID'] = 3
alta_sucs['TipoDirID'] = 1

#%%
#DATAFRAMES PARA GNM_CIG
n_sucs = alta_sucs.shape[0]

#COM DIRECCIONES
CIG_alta_comDirecciones = alta_sucs[['DirCalle','DirNumExterior','DirNumInterior','CodPstID','AsenID','CidID','TipoEntID','TipoDirID','StaGenID']].copy()

CIG_alta_comDirecciones['DirID'] = range(CIG_comDirecciones['DirID'][0] + 1,CIG_comDirecciones['DirID'][0] + 1 + n_sucs)
CIG_alta_comDirecciones['DirEntidadID'] = range(CIG_comDirecciones['DirEntidadID'][0] + 1,CIG_comDirecciones['DirEntidadID'][0] + 1 + n_sucs)

CIG_alta_comDirecciones = CIG_alta_comDirecciones[['DirID','DirCalle','DirNumExterior','DirNumInterior','CodPstID','AsenID','CidID','TipoEntID','TipoDirID','DirEntidadID','StaGenID']].copy()


#COM SUCURSALES
CIG_alta_comSucursales = alta_sucs[['SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucMail',	'SucRFC',	'SucURL',	'SucFechaIng',	'SucCodCliente',	'ClaTndID',	'CadID',	'StaGenID']].copy()

CIG_alta_comSucursales['SucID'] = range(CIG_comSucursales['SucID'][0] + 1,CIG_comSucursales['SucID'][0] + 1 + n_sucs)
CIG_alta_comSucursales['StaGenId'] = CIG_alta_comSucursales['StaGenID']
CIG_alta_comSucursales['SucCodCliente'] = CIG_alta_comSucursales['SucCodCliente'].astype(str)

CIG_alta_comSucursales = CIG_alta_comSucursales[['SucID','SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucMail',	'SucRFC',	'SucURL',	'SucFechaIng',	'SucCodCliente',	'ClaTndID',	'CadID',	'StaGenId']].copy()

#%%
#DATAFRAMES PARA GNM_CAT_MAESTROS
#COM DIRECCIONES
CAT_alta_comDirecciones = alta_sucs[['DirCalle',	'DirNumExterior',	'DirNumInterior',	'DirColonia',	'DirEntreCalles',	'CodPstID',	'AsenID',	'CidID',	'TipoEntID',	'TipoDirID','StaGenID']].copy()

CAT_alta_comDirecciones['DirID'] = range(CAT_comDirecciones['DirID'][0] + 1,CAT_comDirecciones['DirID'][0] + 1 + n_sucs)
CAT_alta_comDirecciones['DirEntidadID'] = range(CAT_comDirecciones['DirEntidadID'][0] + 1,CAT_comDirecciones['DirEntidadID'][0] + 1 + n_sucs)

CAT_alta_comDirecciones = CAT_alta_comDirecciones[['DirCalle',	'DirNumExterior',	'DirNumInterior',	'DirColonia',	'DirEntreCalles',	'CodPstID',	'AsenID',	'CidID',	'TipoEntID',	'TipoDirID',	'DirEntidadID',	'StaGenID']].copy()

#COM SUCURSALES
CAT_alta_comSucursales = alta_sucs[['SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenID']].copy()

CAT_alta_comSucursales['SucID'] = range(CAT_comSucursales['SucID'][0] + 1,CAT_comSucursales['SucID'][0] + 1 + n_sucs)
CAT_alta_comSucursales['StaGenId'] = CAT_alta_comSucursales['StaGenID']
CAT_alta_comSucursales['SucCodCliente'] = CAT_alta_comSucursales['SucCodCliente'].astype(str)

CAT_alta_comSucursales = CAT_alta_comSucursales[['SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenId']].copy()

#%%
#DATAFRAMES PARA GNM_MASTEROP
#COM DIRECCIONES
MAS_alta_comDirecciones = alta_sucs[['DirCalle',	'DirNumExterior',	'DirNumInterior',	'DirColonia',	'DirEntreCalles',	'CodPstID',	'AsenID',	'CidID',	'TipoEntID',	'TipoDirID','StaGenID']].copy()

MAS_alta_comDirecciones['DirID'] = range(MAS_comDirecciones['DirID'][0] + 1,MAS_comDirecciones['DirID'][0] + 1 + n_sucs)
MAS_alta_comDirecciones['DirEntidadID'] = range(MAS_comDirecciones['DirEntidadID'][0] + 1,MAS_comDirecciones['DirEntidadID'][0] + 1 + n_sucs)

MAS_alta_comDirecciones = MAS_alta_comDirecciones[['DirID',	'DirCalle',	'DirNumExterior',	'DirNumInterior',	'DirColonia',	'DirEntreCalles',	'CodPstID',	'AsenID',	'CidID',	'TipoEntID',	'TipoDirID',	'DirEntidadID',	'StaGenID']].copy()

#COM SUCURSALES
MAS_alta_comSucursales = alta_sucs[['SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenID']].copy()

MAS_alta_comSucursales['SucID'] = range(MAS_comSucursales['SucID'][0] + 1,MAS_comSucursales['SucID'][0] + 1 + n_sucs)
MAS_alta_comSucursales['StaGenId'] = MAS_alta_comSucursales['StaGenID']
MAS_alta_comSucursales['SucPromotor'] = 0
MAS_alta_comSucursales['SucCodCliente'] = MAS_alta_comSucursales['SucCodCliente'].astype(str)

MAS_alta_comSucursales = MAS_alta_comSucursales[['SucID',	'SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenId','SucPromotor']].copy()

#%%
#TABLAS
#MAS_comSucursales
#alta_sucs
#CIG_alta_comDirecciones
#CIG_alta_comSucursales
# CAT_alta_comDirecciones
# CAT_alta_comSucursales
# MAS_alta_comDirecciones

#%%
# Carga de datos a la BD
# directory = [(num_conn, nombre del df, ubicación en la BD)]
directory = [('3', 'CIG_alta_comSucursales', 'Gnm_CIG.dbo.ComSucursalesTienda'),
             ('2', 'CAT_alta_comSucursales', 'Gnm_CatMaestros.dbo.ComSucursalesTienda'),
             ('1', 'MAS_alta_comSucursales', 'Gnm_MasterOp.dbo.ComSucursalesTienda'),
             ('3', 'CIG_alta_comDirecciones', 'Gnm_CIG.dbo.ComDirecciones'),
             ('2', 'CAT_alta_comDirecciones', 'Gnm_CatMaestros.dbo.ComDirecciones'),
             ('1', 'MAS_alta_comDirecciones', 'Gnm_MasterOp.dbo.ComDirecciones')]

for num_conn, table, path in directory:
    # Generamos el insert query para cada tabla
    query_insert = 'INSERT INTO ' + path + ' ('
    num = 0
    part_columns = ''
    part_rows = ''
    for col in eval(table + '.columns'):
        part_columns += col +', '
        part_rows += 'row.' + col + ', '
        num += 1
    part_values = '?,'*num
    query_insert += part_columns[:-2] + ')\nVALUES (' + part_values[:-1] + ')'
    # Instanciamos el conector de la BD
    exec('cursor = conn' + num_conn + '.cursor()')
    # Cargamos los datos
    try:
        for row in eval(table + '.itertuples()'):
            cursor.execute(query_insert, eval(part_rows[:-2]))
        cursor.commit()
        print(str(eval(table + '.shape[0]')) + " registros cargados en", path)
    except Exception as e:
        print('Error en', path)
        print(e, '\n')
    cursor.close()