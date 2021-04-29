# IGXX - Personal a honorarios (proveedores) por años.

#  Fecha creación: 15.12.2020

# Importar librerías
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import string

# ## Utilidades


def clean_blankspace(df):
    # eliminar espacios en nombres de columnas
    columns = list(df.columns)
    columns = [w.replace(' ', '') for w in columns]
    df.columns = columns
    # Eliminar espacios en cada celda del dataframe
    for columna in list(df.columns):
        if df.dtypes[columna] == np.dtype('O'):
            df[columna] = df[columna].str.replace(' ', '')
    return df


def clean_columns(txt):
    df = txt.drop(columns=txt.columns[0:2])\
        .drop(columns=txt.columns[-1])\
        .dropna(how='all').reset_index(drop=True)
    return df


def num_format(df, col):
    df[col] = df[col].str.replace('.', '').str.replace(',', '.').astype(float)
    if (col == 'DMBTR') or (col == 'ERFMG') or (col == 'VERPR'):
        df[col] = df[col]*100
    return df


def conversion_clp(waers, rlwrt):
    if waers == 'CLP':
        valor = rlwrt*100
    if waers == 'EUR':
        valor = rlwrt*906
    if waers == 'GBP':
        valor = rlwrt*1004.62
    if waers == 'UF':
        valor = (rlwrt/1000)*28915
    if waers == 'USD':
        valor = rlwrt*766
    if waers == 'UTM':                 
        valor = (rlwrt/100)*50674
    if waers == 'AUD':   
        valor = rlwrt*500.11
    if waers == 'CAD':
        valor = rlwrt*600.8
    if (waers == '') or (waers == 'nan'):
        valor = rlwrt
    return valor


def str_format(df, col):

    # Elimina letras, sinbolos de puntuacion, u otros simbolos de columnas numericas

    punctuation = list(string.punctuation)   # lista de simbolos de puntuacion      
    letters = list(string.ascii_letters)     # Lista de letras mayusculas y minusculas        
    symbols = ['^', '°']                      # Lista de simbolos. Si aparece uno nuevo, habría que agregarlo a la lista!

    for i in range(len(punctuation)):
        df[col] = df[col].str.replace(punctuation[i], '')   # Elimina letras

    for i in range(len(letters)):
        df[col] = df[col].str.replace(letters[i], '')       # Elimina simbolos de puntuación

    for i in range(len(symbols)):
        df[col] = df[col].str.replace(symbols[i], '')       # Elimina otros simbolos

    df = df[df[col] != '']                                 # Elimina celdas sin contenido

    return df


# ## Cargar tablas

input_path = 'C:/projects/auditoria_continua/data/input/current/'
output_path = 'C:/projects/auditoria_continua/data/output/current/'

rbkp_df = pd.read_csv(input_path + 'rbkp.txt', sep='|', header=3, encoding='latin1', low_memory=False)
lfa1_df = pd.read_csv(output_path + 'lfa1.txt', sep='|', header=3, encoding='latin1', low_memory=False)


# ## Limpieza

# **Eliminar columnas y filas nulas**
rbkp_df = clean_columns(rbkp_df)
lfa1_df = clean_columns(lfa1_df)


# **Eliminar espacios en columnas y cada celda**
rbkp_df = clean_blankspace(rbkp_df)
lfa1_df = clean_blankspace(lfa1_df)


# **Cambio de formato de columnas**
rbkp_df = str_format(rbkp_df, 'XBLNR')                              # elimina letras o simbolos de columnas numericas  
rbkp_df = num_format(rbkp_df, 'RMWWR')                              # reemplaza "," por "", y "," por "."
rbkp_df = rbkp_df.replace(r'^\s*$', np.nan, regex=True)            # reemplaza valores vacios con np.nan


# **Establecer esquema de datos**
schema_rbkp = {'BELNR': 'float64',     # Doc_material 
               'GJAHR': str,          # Año 
                'BLART': str,          # Clase_doc_material 
                'BUDAT': str,          # Fecha_contabilidad 
                'USNAM': str,          # Usuario 
                'XBLNR': 'float64',    # Referencia 
                'LIFNR': 'float64',    # Proveedor 
                'WAERS': str,          # Moneda 
                'RMWWR': 'float64'}    # Monto_bruto

schema_lfa1 = {'LIFNR': 'float64',     # Proveedor
                'NAME1': str,          # Nombre_proveedor
                'SORTL': str,          # Rut_proveedor
                'BRSCH': str,          # Tipo_proveedor
                'KTOKK': str}          # Grupo_de_cuentas

rbkp_df = rbkp_df.astype(schema_rbkp)
lfa1_df = lfa1_df.astype(schema_lfa1)


# **Cambio de formato de columnas en su respectiva moneda**
rbkp_df['RMWWR_CLP'] = rbkp_df.apply(lambda x: conversion_clp(x['WAERS'],x['RMWWR']),axis=1)


# **Eliminar columnas que no se utilizan en este indicador**
rbkp_df.drop(columns= ['RMWWR'], inplace= True)


# ## Ejecutar indicador
df = pd.merge(rbkp_df, lfa1_df, on='LIFNR', how='inner')
df = df.drop_duplicates()


# **Filtrar los tipos de documentos a analizar**
df = df[(df['BLART']=='BE') |
        (df['BLART']=='BM') |
        (df['BLART']=='KA') |
        (df['BLART']=='KT') |
        (df['BLART']=='KU') |
        (df['BLART']=='KV') |
        (df['BLART']=='KK') |
        (df['BLART']=='KR') |
        (df['BLART']=='KS') |
        (df['BLART']=='KW') |
        (df['BLART']=='KX')]


# **Hacer filtro por rut de personas naturales o que sean EIRL**
p_natural = df[(df['SORTL'].str.startswith('1')) | 
                 (df['SORTL'].str.startswith('2')) |
                 (df['SORTL'].str.startswith('3'))].reset_index(drop=True)
eirl = df[(df['NAME1'].str.contains('EIRL')) |
          (df['NAME1'].str.contains('E.I.R.L'))].reset_index(drop=True)

pnat_eirl = pd.concat([p_natural, eirl], axis='index')


# **Columnas Auxiliares**
mes = []
for i in pnat_eirl['BUDAT']:
    aux = i[3:5]
    mes.append(aux)

pnat_eirl['Mes'] = mes

dias = []

for i in pnat_eirl['BUDAT']:
    dia = i[0:2]
    dias.append(dia)
    
pnat_eirl['Dia'] = dias
pnat_eirl['Auxiliar'] = pnat_eirl['GJAHR'].str.replace('.', '') + pnat_eirl['Mes'] + pnat_eirl['Dia']
pnat_eirl['Auxiliar'] = pnat_eirl['Auxiliar'].astype(int)


# **Calcular cantidad de documentos dentro de 1 año y de 2 años**
fecha_anio = int((datetime.now() - timedelta(days=366)).strftime('%Y0%m%d'))
fecha_anio2 = int((datetime.now() - timedelta(days=734)).strftime('%Y0%m%d'))


# 2 Años (24 meses)
meses24_df = pnat_eirl[pnat_eirl['Auxiliar']>=fecha_anio2].reset_index(drop=True)
meses24_group = meses24_df.groupby(['LIFNR', 'SORTL', 'NAME1', 'Mes']).agg({'BELNR':'count'}).reset_index().sort_values(by='LIFNR')

mask = meses24_group.LIFNR.duplicated(keep=False)
meses24_group['duplicado'] = meses24_group.LIFNR.mask(mask, 0)
meses24_group = meses24_group.drop(meses24_group[meses24_group['duplicado']!=0].index).sort_values(by='LIFNR')
meses24_group = meses24_group.drop(['duplicado'], axis=1)


# **Indicador**
indicador_1 = meses24_group.groupby(['LIFNR', 'SORTL', 'NAME1']).agg({'Mes':'count', 'BELNR':'sum'}).sort_values(by='LIFNR').reset_index()
indicador_1 = indicador_1[indicador_1['Mes']>=22].reset_index(drop=True)
indicador_1.columns = ['Numero_Proveedor',
                        'RUT_proveedor',
                        'Nombre_Proveedor',
                        'N_meses_con documento',
                        'Cantidad_documentos']

detalle_1 = pd.merge(pnat_eirl, indicador_1, left_on='LIFNR', right_on='Numero_Proveedor', how='inner')
detalle_1 = detalle_1[['LIFNR', 'SORTL', 'NAME1', 'BELNR', 'BLART', 'XBLNR', 'BUDAT', 'RMWWR_CLP']].sort_values(by='LIFNR').reset_index(drop=True)
detalle_1.columns = ['Numero_Proveedor',
                     'RUT_Proveedor',
                     'Nombre_Proveedor',
                     'Documento_material',
                     'Clase_documento',
                     'Referencia',
                     'fecha_ingreso',
                     'Monto_bruto_clp']

# 1 Año (12 meses)
meses12_df = pnat_eirl[pnat_eirl['Auxiliar']>=fecha_anio].reset_index()
meses12_group = meses12_df.groupby(['LIFNR', 'SORTL', 'NAME1', 'Mes']).agg({'BELNR':'count'}).reset_index().sort_values(by='LIFNR')

mask = meses12_group.LIFNR.duplicated(keep=False)
meses12_group['duplicado'] = meses12_group.LIFNR.mask(mask, 0)
meses12_group = meses12_group.drop(meses12_group[meses12_group['duplicado']!=0].index).sort_values(by='LIFNR')
meses12_group = meses12_group.drop(['duplicado'], axis=1)

indicador_2 = meses12_group.groupby(['LIFNR', 'SORTL', 'NAME1']).agg({'Mes':'count', 'BELNR':'sum'}).sort_values(by='LIFNR').reset_index()
indicador_2 = indicador_2[indicador_2['Mes']>=11].reset_index(drop=True)
indicador_2.columns = ['Numero_Proveedor',
                       'RUT_proveedor',
                       'Nombre_Proveedor',
                       'N_meses_con documento',
                       'Cantidad_documentos']

detalle_2 = pd.merge(pnat_eirl, indicador_2, left_on='LIFNR', right_on='Numero_Proveedor', how='inner')
detalle_2 = detalle_2[['LIFNR', 'SORTL', 'NAME1', 'BELNR', 'BLART', 'XBLNR', 'BUDAT', 'RMWWR_CLP']].sort_values(by='LIFNR').reset_index(drop=True)
detalle_2.columns = ['Numero_Proveedor',
                     'RUT_Proveedor',
                     'Nombre_Proveedor',
                     'Documento_material',
                     'Clase_documento',
                     'Referencia',
                     'fecha_ingreso',
                     'Monto_bruto_clp']

# ## Guardar en Excel
nombre_archivo = 'ac12_' + datetime.now().strftime("%d-%m-%y_%Hh%Mm") + '.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')

indicador_1.to_excel(writer, sheet_name='Proveedores_22meses')
detalle_1.to_excel(writer, sheet_name='Detalle_22meses')
indicador_2.to_excel(writer, sheet_name='Proveedores_11meses')
detalle_2.to_excel(writer, sheet_name='Detalle_11meses')

writer.save()
