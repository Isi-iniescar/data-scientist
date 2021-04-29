
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
    df = txt.drop(columns=txt.columns[0:2]).drop(columns=txt.columns[-1]).\
        dropna(how='all').reset_index(drop=True)
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
    df[col] = df[col].str.replace('.', '').str.replace(',', '.').astype(float)
    if (col == 'DMBTR') or (col == 'ERFMG') or (col == 'VERPR'):
        df[col] = df[col]*100
    return df


def soc(row):
    if row['BUKRS'] == 'A000':
        soc = 'ACHS'
    elif row['BUKRS'] == 'C200':
        soc = 'CEM'
    elif row['BUKRS'] == 'D100':
        soc = 'DEPORTIVO CEM'
    elif row['BUKRS'] == 'F200':
        soc = 'FUCYT'
    elif row['BUKRS'] == 'OTEC':
        soc = 'OTEC'
    elif row['BUKRS'] == 'P200':
        soc = 'ESACHS S.A.'
    elif row['BUKRS'] == 'T200':
        soc = 'ESACHS TRANSPORTE S.A.'
    elif row['BUKRS'] == 'W100':
        soc = 'FOSACHS'
    elif row['BUKRS'] == 'X100':
        soc = 'FAJ'
    elif row['BUKRS'] == 'Y100':
        soc = 'FIJ'
    elif row['BUKRS'] == 'Z100':
        soc = 'FIN'
    else:
        soc = 'SIN INFORMACION'
    return soc


def contrato(row):
    if row['CTTYP'] == '01':
        contrato = 'Indefinido'
    elif row['CTTYP'] == '02':
        contrato = 'Contrato Temporal'
    elif row['CTTYP'] == 'A1':
        contrato = 'General Indefinido'
    elif row['CTTYP'] == 'A2':
        contrato = 'General Plazo Fijo'
    elif row['CTTYP'] == 'A3':
        contrato = 'Médico Indefinido'
    elif row['CTTYP'] == 'A4':
        contrato = 'Médico Plazo Fijo'
    elif row['CTTYP'] == 'A5':
        contrato = 'Experto Indefinido'
    elif row['CTTYP'] == 'A6':
        contrato = 'Experto Plazo Fijo'
    elif row['CTTYP'] == 'Q1':
        contrato = 'Definido'
    elif row['CTTYP'] == 'Q2':
        contrato = 'Indefinido'
    elif row['CTTYP'] == 'P1':
        contrato = 'Full-time'
    elif row['CTTYP'] == 'P2':
        contrato = 'Part-time'
    else:
        contrato = 'SIN INFORMACION'
    return contrato


# Cargar tablas
pa0000 = pd.read_csv(
    input_path + 'PA0000.txt',
    sep='|',
    header=3,
    encoding='latin1')
pa0001 = pd.read_csv(
    input_path + 'PA0001.txt',
    sep='|',
    header=3,
    encoding='latin1')
pa0007 = pd.read_csv(
    input_path + 'PA0007.txt',
    sep='|',
    header=3,
    encoding='latin1')
pa0016 = pd.read_csv(
    input_path + 'PA0016.txt',
    sep='|',
    header=3,
    encoding='latin1')
pa0185 = pd.read_csv(
    input_path + 'PA0185.txt',
    sep='|',
    header=3,
    encoding='latin1')
t528t = pd.read_csv(
    input_path + 'T528T.txt',
    sep='|',
    header=3,
    encoding='latin1')

# Limpieza de tablas
# Eliminar filas y columnas NaN
pa0000 = clean_columns(pa0000)
pa0001 = clean_columns(pa0001)
pa0007 = clean_columns(pa0007)
pa0016 = clean_columns(pa0016)
pa0185 = clean_columns(pa0185)
t528t = clean_columns(t528t)

# Eliminar espacios en nombres de columnas y cada celda
pa0000 = clean_blankspace(pa0000)
pa0001 = clean_blankspace(pa0001)
pa0007 = clean_blankspace(pa0007)
pa0016 = clean_blankspace(pa0016)
pa0185 = clean_blankspace(pa0185)
t528t = clean_blankspace(t528t)

# Cambio de formato de columnas
# Reemplaza "," por "", y "," por "."
pa0007 = num_format(pa0007, 'MOSTD')
# Reemplaza valores vacios con np.nan
pa0007 = pa0007.replace(r'^\s*$', np.nan, regex=True)

# Reemplaza "," por "", y "," por "."
pa0007 = num_format(pa0007, 'WOSTD')
# Reemplaza valores vacios con np.nan
pa0007 = pa0007.replace(r'^\s*$', np.nan, regex=True)

# Reemplaza "," por "", y "," por "."
pa0007 = num_format(pa0007, 'ARBST')
# Reemplaza valores vacios con np.nan
pa0007 = pa0007.replace(r'^\s*$', np.nan, regex=True)

# Establecer esquema de datos
schema_pa0000 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_periodo
    'ENDDA': str,
    # Inicio_periodo
    'BEGDA': str,
    # Fecha_mod
    'AEDTM': str,
    # Mod_por
    'UNAME': str,
    # Estatus
    'MASSN': str,
    # Motivo_medida
    'MASSG': str}

schema_pa0001 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_periodo
    'ENDDA': str,
    # Inicio_periodo
    'BEGDA': str,
    # Sociedad (Cod_sociedad)
    'BUKRS': str,
    # Div_personal
    'WERKS': str,
    # Clave_org
    'VDSK1': str,
    # Centro_costo
    'KOSTL': str,
    # Posicion
    'PLANS': 'float64',
    # Nombre
    'SNAME': str}

schema_pa0007 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_periodo
    'ENDDA': str,
    # Inicio_periodo
    'BEGDA': str,
    # Horas_mes
    'MOSTD': 'float64',
    # Horas_semana
    'WOSTD': 'float64',
    # Horas_dia
    'ARBST': 'float64'}

schema_pa0016 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_periodo
    'ENDDA': str,
    # Inicio_periodo
    'BEGDA': str,
    # Tipo_contrato (Contrato)
    'CTTYP': str}

schema_pa0185 = {
    # N_personal
    'PERNR': 'float64',
    # Fin_periodo
    'ENDDA': str,
    # Inicio_periodo
    'BEGDA': str,
    # Rut
    'ICNUM': str}

schema_t528t = {
    # Idioma
    'SPRSL': str,
    # Objeto
    'OTYPE': str,
    # Posicion
    'PLANS': 'float64',
    # Desc_posicion
    'PLSTX': str}

pa0000 = pa0000.astype(schema_pa0000)
pa0001 = pa0001.astype(schema_pa0001)
pa0007 = pa0007.astype(schema_pa0007)
pa0016 = pa0016.astype(schema_pa0016)
pa0185 = pa0185.astype(schema_pa0185)
t528t = t528t.astype(schema_t528t)

# **Filtrar tablas**
# Filtro de T528T: Filtrar todas las celdas que en la columna
# SPRSL sean igual a S (español)
#                 Filtrar todas las celdas que en la columna
# OTYPE sean igual a S
t528t = t528t[(t528t['SPRSL'] == 'S') & (t528t['OTYPE'] == 'S')]

# Filtrar de PA0000 para obtener los colaboradores vigentes.
# MASSN != 'XE','XJ','XI','XF'
pa0000 = pa0000[~pa0000.MASSN.isin(['XE', 'XJ', 'XI', 'XF'])]

# Asignar los nombres a cada código de sociedad
pa0001['BUKRS'] = pa0001.apply(soc, axis=1)

# Asignar los tipo de contrato a cada código de contrato
pa0016['CTTYP'] = pa0016.apply(contrato, axis=1)

# **Eliminar columnas no necesarias**
pa0000.drop(columns=['ENDDA', 'BEGDA', 'AEDTM', 'UNAME'], inplace=True)
pa0001.drop(columns=['ENDDA', 'BEGDA', 'WERKS'], inplace=True)
pa0007.drop(columns=['ENDDA', 'BEGDA'], inplace=True)
pa0016.drop(columns=['ENDDA', 'BEGDA'], inplace=True)
pa0185.drop(columns=['ENDDA', 'BEGDA'], inplace=True)
t528t.drop(columns=['SPRSL', 'OTYPE'], inplace=True)

# Ejecución indicador
# Luego de realizados estos pasos previos, comenzamos a cruzar las tablas
# para ejecutar el indicador. Los cruces que vamos a hacer son:
#  - **pa0000 y pa0185** por `N_personal` > aux1
#  - **aux2 y pa0007** por `N_personal` > aux3
#  - **aux3 y pa0016** por `N_personal` > aux4
#  - **aux4 y pa0001** por `N_personal` > aux5
#  - **aux5 y t528t** por `Posicion` > aux6
#  - **aux6** vemos que las horas mensuales sean mayores 180

# **Paso 1: pa0000 y pa0185**
aux1 = pd.merge(pa0000, pa0185, on='PERNR', how='inner')

# **Paso 3: aux2 y pa0007**
aux3 = pd.merge(aux1, pa0007, on='PERNR', how='inner')

# **Paso 4: aux3 y pa0016**
aux4 = pd.merge(aux3, pa0016, on='PERNR', how='inner')

# **Paso 5: aux4 y pa0001**
aux5 = pd.merge(aux4, pa0001, on='PERNR', how='inner')

# **Paso 6: aux5 y t528t**
aux6 = pd.merge(aux5, t528t, on='PLANS', how='inner')

# Agrupar por RUT la suma de MOSTD
grouped = aux6.groupby('ICNUM').agg({'MOSTD': 'sum'})

# Filtrar los RUT que sumen más de 180 horas mensuales
filtro = grouped[grouped['MOSTD'] > 180]
filtro.reset_index(inplace=True)

# Obtener los datos de los RUT que suman más de 180 horas
IG22 = pd.merge(aux6, filtro[['ICNUM']], on='ICNUM', how='right')

# Reordenamos las columnas
IG22 = IG22[
    [
        'PERNR', 'ICNUM', 'SNAME', 'BUKRS', 'CTTYP', 'PLANS',
        'PLSTX', 'MASSN', 'MOSTD', 'WOSTD', 'ARBST']]

# Cambio de nombre de columnas
IG22.rename(
    columns={
        'PERNR': 'N_personal',
        'ICNUM': 'Rut',
        'SNAME': 'Nombre',
        'BUKRS': 'Sociedad',
        'CTTYP': 'Contrato',
        'PLANS': 'Posicion',
        'PLSTX': 'Desc_posicion',
        'MASSN': 'Estatus',
        'MOSTD': 'Horas_mes',
        'WOSTD': 'Horas_semana',
        'ARBST': 'Horas_dia'},
    inplace=True)


# Guardamos el resultado en Excel
nombre_archivo = output_path + 'IG22_'+datetime.now().\
    strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
IG22.to_excel(writer, sheet_name='IG22')

writer.save()
