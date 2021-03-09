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
alta_sucs = pd.read_excel('Cargar Sucursales Honduras (6 - 2021).xlsx')

#%%
for col in ['SucUn','SucDescripcion','SucTelPrincipal','SucTelAlterno','SucFax','SucRFC','SucMail','DirNumExterior','DirNumInterior','SucURL']:
    alta_sucs.loc[alta_sucs[col].isnull(),col] = ''

for col in ['SucFechaApertura','SucMetros','SucFechaIng']:
    alta_sucs.loc[alta_sucs[col].isnull(),col] = 'NULL'

alta_sucs['CodPstID'] = alta_sucs['CodPstID'].apply(lambda x: x if x != 0 else '00000')

alta_sucs['StaGenID'] = 1
alta_sucs['CidID'] = 216

alta_sucs['SucNitAtlas'] = ''
alta_sucs['SucEsCedis'] = 0
alta_sucs['TipoAndID'] = 'NULL'
alta_sucs['AsenID'] = 'NULL'
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

CAT_alta_comDirecciones = CAT_alta_comDirecciones[['DirID',	'DirCalle',	'DirNumExterior',	'DirNumInterior',	'DirColonia',	'DirEntreCalles',	'CodPstID',	'AsenID',	'CidID',	'TipoEntID',	'TipoDirID',	'DirEntidadID',	'StaGenID']].copy()

#COM SUCURSALES
CAT_alta_comSucursales = alta_sucs[['SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenID']].copy()

CAT_alta_comSucursales['SucID'] = range(CAT_comSucursales['SucID'][0] + 1,CAT_comSucursales['SucID'][0] + 1 + n_sucs)
CAT_alta_comSucursales['StaGenId'] = CAT_alta_comSucursales['StaGenID']
CAT_alta_comSucursales['SucCodCliente'] = CAT_alta_comSucursales['SucCodCliente'].astype(str)

CAT_alta_comSucursales = CAT_alta_comSucursales[['SucID',	'SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenId']].copy()

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
MAS_alta_comSucursales['SucPromotor'] = 1
MAS_alta_comSucursales['SucCodCliente'] = MAS_alta_comSucursales['SucCodCliente'].astype(str)

MAS_alta_comSucursales = MAS_alta_comSucursales[['SucID',	'SucUn',	'SucNombre',	'SucDescripcion',	'SucFechaApertura',	'SucMetros',	'SucTelPrincipal',	'SucTelAlterno',	'SucFax',	'SucRFC',	'SucFechaIng',	'SucCodCliente',	'SucNitAtlas',	'SucEsCedis',	'ClaTndID',	'CadID',	'TipoAndID',	'StaGenId','SucPromotor']].copy()

#%%
#TABLAS

# CIG_alta_comDirecciones
# CIG_alta_comSucursales
# CAT_alta_comDirecciones
# CAT_alta_comSucursales
# MAS_alta_comDirecciones
# MAS_alta_comSucursales
