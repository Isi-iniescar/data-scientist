
# Importar librerías
import pandas as pd
import numpy as np
from datetime import datetime
import string


input_path = 'C:/projects/auditoria_continua/data/input/current/'
output_path = 'C:/projects/auditoria_continua/data/output/current/'


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


def num_format(df, col):
    df[col] = df[col].str.replace('.', '').str.replace(',', '.').astype(float)
    if (col == 'DMBTR') or (col == 'ERFMG') or (col == 'VERPR'):
        df[col] = df[col]*100
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


def str_format(df, col):

    # Elimina letras, sinbolos de puntuacion,
    # u otros simbolos de columnas numericas
    # Lista de simbolos de puntuacion
    punctuation = list(string.punctuation)

    # Lista de letras mayusculas y minusculas
    letters = list(string.ascii_letters)

    # Lista de simbolos. Si aparece uno nuevo, habría que agregarlo a la lista!
    symbols = ['^', '°']

    # Elimina letras
    for i in range(len(punctuation)):
        df[col] = df[col].str.replace(punctuation[i], '')

    # Elimina simbolos de puntuación
    for i in range(len(letters)):
        df[col] = df[col].str.replace(letters[i], '')

    # Elimina otros simbolos
    for i in range(len(symbols)):
        df[col] = df[col].str.replace(symbols[i], '')

    # Elimina celdas sin contenido
    df = df[df[col] != '']

    return df


def val(row):
    # Validar que los proveedores de la tabla ekko no cambiaron con
    # posterioridad para un mismo documento de compra y material
    if row['LIFNR_ekko'] == row['LIFNR']:
        val = 'Igual'
    else:
        val = 'Distinto'
    return val


# Cargar tablas
ekko_df = pd.read_csv(
    input_path + 'EKKO.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
ekbe_df = pd.read_csv(
    input_path + 'EKBE.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
rbkp_df = pd.read_csv(
    input_path + 'RBKP.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
lfa1_df = pd.read_csv(
    input_path + 'LFA1.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)

# Limpieza de tablas
# **Eliminar columnas y filas nulas**
ekko_df = clean_columns(ekko_df)
ekbe_df = clean_columns(ekbe_df)
rbkp_df = clean_columns(rbkp_df)
lfa1_df = clean_columns(lfa1_df)

# Eliminar espacios en columnas y cada celda
ekko_df = clean_blankspace(ekko_df)
ekbe_df = clean_blankspace(ekbe_df)
rbkp_df = clean_blankspace(rbkp_df)
lfa1_df = clean_blankspace(lfa1_df)

# Cambio de formato de columnas
# Reemplaza "," por "", y "," por "."
ekko_df = num_format(ekko_df, 'RLWRT')

# Reemplaza valores vacios con np.nan
ekko_df = ekko_df.replace(r'^\s*$', np.nan, regex=True)

# Elimina letras o simbolos de columnas numericas
ekbe_df = str_format(ekbe_df, 'EBELN')

# Elimina letras o simbolos de columnas numericas
rbkp_df = str_format(rbkp_df, 'XBLNR')

# Reemplaza "," por "", y "," por "."
rbkp_df = num_format(rbkp_df, 'RMWWR')

# Reemplaza valores vacios con np.nan
rbkp_df = rbkp_df.replace(r'^\s*$', np.nan, regex=True)

# Establecer esquema de datos
schema_ekko = {
    # Doc_compra
    'EBELN': 'float64',
    # Sociedad
    'BUKRS': str,
    # Clase_doc_compra
    'BSART': str,
    # Fecha_creacion
    'AEDAT': str,
    # Proveedor_ekko
    'LIFNR': 'float64',
    # Moneda
    'WAERS': str,
    # Ind_liberacion
    'FRGKE': str,
    # Estatus_doc
    'PROCSTAT': 'float64',
    # Monto_neto
    'RLWRT': 'float64'}

schema_ekbe = {
    # Doc_compra
    'EBELN': 'float64',
    # Posicion
    'EBELP': 'float64',
    # Doc_material
    'BELNR': 'float64'}

schema_rbkp = {
    # Doc_material
    'BELNR': 'float64',
    # Año
    'GJAHR': 'float64',
    # Clase_doc_material
    'BLART': str,
    # Fecha_contabilidad
    'BUDAT': str,
    # Usuario
    'USNAM': str,
    # Referencia
    'XBLNR': 'float64',
    # Proveedor
    'LIFNR': 'float64',
    # Moneda
    'WAERS': str,
    # Monto_bruto
    'RMWWR': 'float64'}

schema_lfa1 = {
    # Proveedor
    'LIFNR': 'float64',
    # Nombre_proveedor
    'NAME1': str,
    # Rut_proveedor
    'SORTL': str,
    # Tipo_proveedor
    'BRSCH': str,
    # Grupo_de_cuentas
    'KTOKK': str}

ekko_df = ekko_df.astype(schema_ekko)
ekbe_df = ekbe_df.astype(schema_ekbe)
rbkp_df = rbkp_df.astype(schema_rbkp)
lfa1_df = lfa1_df.astype(schema_lfa1)

# **Aplicar filtros que se utilizan para este indicador**
# Filtro por sociedad A000 = ACHS, y por clase de documento
ekko_df = ekko_df[
    (ekko_df['BUKRS'] == 'A000')
    & (
        (ekko_df['BSART'] == 'ZMAT')
        | (ekko_df['BSART'] == 'ZSER')
        | (ekko_df['BSART'] == 'ZSCL')
        | (ekko_df['BSART'] == 'ZCDP'))]


# Cambio de formato de columnas en su respectiva moneda
ekko_df['RLWRT_CLP'] = ekko_df.apply(
    lambda x: conversion_clp(x['WAERS'], x['RLWRT']), axis=1)
rbkp_df['RMWWR_CLP'] = rbkp_df.apply(
    lambda x: conversion_clp(x['WAERS'], x['RMWWR']), axis=1)


# **Eliminar columnas que no se utilizan en este indicador**
ekko_df.drop(columns=['PROCSTAT', 'WAERS', 'RLWRT', 'FRGKE'], inplace=True)
rbkp_df.drop(columns=['RMWWR'], inplace=True)


# Cambio de nombre de columnas (para diferenciar en cruce)
ekko_df.rename(columns={'LIFNR': 'LIFNR_ekko'}, inplace=True)

# Ejecución indicador
# Primero, hacemos el siguente cruce:
#  - ekko y ekbe: utilizando como atributo clave el campo `Doc_compra`
#  - rbkp y lfa1: utilizando como atributo clave el campo `Proveedor`
# **ekko y ekbe**
ekko_ekbe = pd.merge(ekko_df, ekbe_df, on='EBELN', how='inner')
ekko_ekbe = ekko_ekbe.drop_duplicates()


# **rbkp y lfa1**
rbkp_lfa1 = pd.merge(rbkp_df, lfa1_df, on='LIFNR', how='inner')
rbkp_lfa1 = rbkp_lfa1.drop_duplicates()


# **Luego, cruzamos ambas tablas (ekko_ekbe y rbkp_lfa1) por `Doc_material`**
IG11 = pd.merge(ekko_ekbe, rbkp_lfa1, on='BELNR', how='inner')
IG11 = IG11.drop_duplicates()


# Reordenamiento de columnas
# Ordenamos las columnas para mejor entendimiento
IG11 = IG11[
    [
        'BUKRS',
        'BSART',
        'EBELN',
        'EBELP',
        'LIFNR_ekko',
        'AEDAT',
        'RLWRT_CLP',
        'BELNR',
        'LIFNR',
        'NAME1',
        'BUDAT',
        'USNAM',
        'RMWWR_CLP',
        'WAERS']]

# Funcion val() para validar que los proveedores de la tabla ekko no cambiaron
# con posterioridad para un mismo documento de compra y material. Aplicamos la
# función sobre la tabla con todos los cruces
IG11['Validacion'] = IG11.apply(val, axis=1)

# Por último, extraemos los valores que tengan **"Distinto"** en la columna
# Validación
IG11 = IG11.loc[(IG11['Validacion'] == 'Distinto'), :]

# Cambio de nombre de columnas
IG11.rename(columns={
    'BUKRS': 'Sociedad',
    'EBELN': 'Orden_Compra',
    'BSART': 'Clase_doc_compra',
    'BELNR': 'Factura',
    'BLART': 'Clase_doc_material',
    'LIFNR': 'Proveedor_Factura',
    'NAME1': 'Nombre_proveedor',
    'GJAHR': 'Año',
    'AEDAT': 'Fecha_creacion_OC',
    'BUDAT': 'Fecha_cont_Fact',
    'EBELP': 'Posicion',
    'USNAM': 'Usuario',
    'RLWRT_CLP': 'Monto_Total_OC',
    'RMWWR_CLP': 'Monto_Total_Fact',
    'WAERS': 'Moneda',
    'LIFNR_ekko': 'Proveedor_OC',
    'Validacion': 'Validacion'}, inplace=True)

# Guardar en Excel
nombre_archivo = output_path + 'IG11_'+datetime.now().\
    strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
IG11.to_excel(writer, sheet_name='IG11')

writer.save()
