
# Importar librerías
import pandas as pd
import numpy as np
from datetime import datetime


input_path = 'C:/projects/auditoria_continua/data/input/current/'
output_path = 'C:/projects/auditoria_continua/data/output/current/'


# Utilidades
def clean_blankspace(df):
    # Eliminar espacios en nombres de columnas
    columns = list(df.columns)
    columns = [w.replace(' ', '') for w in columns]
    df.columns = columns
    # Eliminar espacios en cada celda del dataframe
    for columna in list(df.columns):
        if df.dtypes[columna] == np.dtype('O'):
            df[columna] = df[columna].str.replace(' ', '')
    return df


def clean_columns(txt):
    df = txt.drop(columns=txt.columns[0:2]).drop(columns=txt.columns[-1])\
        .dropna(how='all').reset_index(drop=True)
    return df


def conversion_clp(waers, rlwrt):
    if waers == 'CLP':
        valor = rlwrt*100
    if waers == 'EUR':
        valor = rlwrt*786
    if waers == 'GBP':
        valor = rlwrt*1004.62
    if waers == 'UF':
        valor = (rlwrt/1000)*27854
    if waers == 'USD':
        valor = rlwrt*703
    if waers == 'UTM':
        valor = (rlwrt/100)*48832
    if waers == 'AUD':
        valor = rlwrt*500.11
    if waers == 'CAD':
        valor = rlwrt*600.8
    if (waers == '') or (waers == 'nan'):
        valor = rlwrt
    return valor


def num_format(df, col):
    df[col] = df[col].str.replace('.', '')\
        .str.replace(',', '.').astype(float)
    if (col == 'DMBTR') or (col == 'ERFMG') or (col == 'VERPR'):
        df[col] = df[col]*100
    return df


# Cargar datos
pa0001_txt = pd.read_csv(
    input_path + 'PA0001.txt',
    sep='|',
    header=3,
    encoding='latin1')
pa0015_txt = pd.read_csv(
    input_path + 'PA0015.txt',
    sep='|',
    header=3,
    encoding='latin1')
t528t_txt = pd.read_csv(
    input_path + 'T528T.txt',
    sep='|',
    header=3,
    encoding='latin1')


# Limpieza de datos
pa0001_df = clean_columns(pa0001_txt)
pa0015_df = clean_columns(pa0015_txt)
t528t_df = clean_columns(t528t_txt)

# Eliminar espacios en nombres de columnas y cada celda
pa0001_df = clean_blankspace(pa0001_df)
pa0015_df = clean_blankspace(pa0015_df)
t528t_df = clean_blankspace(t528t_df)

# Cambio de formato de columnas
# Reemplaza "," por "", y "," por "."
pa0015_df = num_format(pa0015_df, 'BETRG')

# Reemplaza valores vacios con np.nan
pa0015_df = pa0015_df.replace(r'^\s*$', np.nan, regex=True)

# Establecer esquema de datos
schema_pa0001 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_validez
    'ENDDA': str,
    # Inicio_validez
    'BEGDA': str,
    # Sociedad
    'BUKRS': str,
    # Div_personal
    'WERKS': str,
    # Clave_org
    'VDSK1': str,
    # Centro_coste (CC)
    'KOSTL': str,
    # Posicion
    'PLANS': 'float64',
    # Nombre
    'SNAME': str}

schema_pa0015 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_validez
    'ENDDA': str,
    # Inicio_validez
    'BEGDA': str,
    # Fecha_modif
    'AEDTM': str,
    # Usuario_modificador (Mod_por)
    'UNAME': str,
    # CC_nomina
    'LGART': 'float64',
    # Monto
    'BETRG': 'float64',
    # Moneda
    'WAERS': str}

schema_t528t = {
    # Idioma
    'SPRSL': str,
    # Objeto
    'OTYPE': str,
    # Posicion
    'PLANS': 'float64',
    # Desc_posicion
    'PLSTX': str}

pa0001_df = pa0001_df.astype(schema_pa0001)
pa0015_df = pa0015_df.astype(schema_pa0015)
t528t_df = t528t_df.astype(schema_t528t)

# Cambio de formato de columna RLWRT de EKKO
pa0015_df['BETRG_CLP'] = pa0015_df.apply(
    lambda x: conversion_clp(x['WAERS'], x['BETRG']),
    axis=1)

# Filtrar tablas
# Filtro de T528T: Filtrar todas las celdas que en la columna SPRSL
# sean igual a S
#                 Filtrar todas las celdas que en la columna OTYPE
# sean igual a S
t528t_df = t528t_df[(t528t_df['SPRSL'] == 'S') & (t528t_df['OTYPE'] == 'S')]

# **Eliminar columnas que no se utilizarán**
pa0001_df.drop(columns=['KOSTL'], inplace=True)
t528t_df.drop(columns=['SPRSL', 'OTYPE'], inplace=True)
pa0015_df.drop(columns=['BETRG'], inplace=True)

# Tabla base
# **Combinamos las tablas**
# Creamos la tabla base combinando las dos primeras tablas
base = pd.merge(pa0001_df, pa0015_df, on='PERNR', how='inner')

# **Eliminar y ordenar columnas**
# Eliminamos columnas que no son de interés
base = base.drop(['ENDDA_x', 'BEGDA_x', 'ENDDA_y', 'BEGDA_y'], axis=1)

# Reordenamos columnas
base = base[
    [
        'PERNR',
        'SNAME',
        'AEDTM',
        'UNAME',
        'LGART',
        'BETRG_CLP',
        'WAERS',
        'BUKRS',
        'VDSK1',
        'WERKS',
        'PLANS']]

# Combinamos la tabla anterior con la última para agregar
# información sobre el cargo
base = pd.merge(base, t528t_df, on='PLANS', how='inner')

# **Tabla base para el IG20**
base_IG20_jefe = base[base.PLSTX.str.contains('jefe', case=False)]
base_IG20_subge = base[base.PLSTX.str.contains('Subgerente', case=False)]
base_IG20_gerente = base[base.PLSTX.str.contains('Gerente', case=False)]
base_IG20 = pd.concat(
    [
        base_IG20_jefe,
        base_IG20_subge,
        base_IG20_gerente], axis=0).reset_index(drop=True)

# **Tabla base para el IG21**
base_IG21_ejecutivo = base[
    base.PLSTX.str.contains('Ejecutivo de capta', case=False)]
base_IG21_supervisor = base[
    base.PLSTX.str.contains('Supervisor de capta', case=False)]
base_IG21 = pd.concat([base_IG21_ejecutivo, base_IG21_supervisor], axis=0)
base_IG21.drop(
    columns=[
        'SNAME', 'AEDTM', 'UNAME', 'LGART', 'BETRG_CLP',
        'WAERS', 'BUKRS', 'VDSK1', 'WERKS', 'PLSTX'], inplace=True)

# Ejecutar IG20
IG20 = base_IG20.loc[(base_IG20['LGART'] == 2054)]
IG20 = IG20.groupby(
    [
        'PERNR', 'SNAME', 'AEDTM', 'UNAME', 'LGART',
        'BUKRS', 'VDSK1', 'WERKS', 'PLANS', 'PLSTX']).agg({'BETRG_CLP': 'sum'})
IG20.reset_index(inplace=True)

# Ejecutar IG21
# Primero definimos una función que nos permite obtener los datos que no están
# en ambas tablas.
IG21 = pd.merge(
    base,
    base_IG21,
    on=['PERNR', 'PLANS'],
    how="outer",
    indicator=True).query('_merge=="left_only"')
IG21 = IG21.loc[(IG21['LGART'] == 2060)].drop(['_merge'], axis=1)

# Cambio de nombre de columnas
IG20.rename(
    columns={
        'PERNR': 'N_personal',
        'SNAME': 'Nombre',
        'AEDTM': 'Fecha_modif',
        'UNAME': 'Mod_por',
        'LGART': 'CC_nomina',
        'BUKRS': 'Sociedad',
        'VDSK1': 'Clave_org',
        'WERKS': 'Div_personal',
        'PLANS': 'Posicion',
        'PLSTX': 'Desc_posicion',
        'BETRG_CLP': 'Monto_CLP'},
    inplace=True)

IG20.rename(
    columns={
        'PERNR': 'N_personal',
        'SNAME': 'Nombre',
        'AEDTM': 'Fecha_modif',
        'UNAME': 'Mod_por',
        'LGART': 'CC_nomina',
        'BETRG_CLP': 'Monto_CLP',
        'WAERS': 'Moneda',
        'BUKRS': 'Sociedad',
        'VDSK1': 'Clave_org',
        'WERKS': 'Div_personal',
        'PLANS': 'Posicion',
        'PLSTX': 'Desc_posicion'},
    inplace=True)

# Guardar en Excel
# **IG20**
nombre_archivo = output_path + 'IG20_'+datetime.now()\
    .strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
IG20.to_excel(writer, sheet_name='IG20')

writer.save()

# **IG21**
nombre_archivo = output_path + 'IG21_'+datetime.now()\
    .strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
IG21.to_excel(writer, sheet_name='IG21')

writer.save()
