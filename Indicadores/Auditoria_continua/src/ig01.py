
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


def num_format(df, col):
    df[col] = df[col].str.replace('.', '').str.replace(',', '.').astype(float)
    if (col == 'DMBTR') or (col == 'ERFMG') or (col == 'VERPR'):
        df[col] = df[col]*100
    return df


# Cargar tablas
T001L_txt = pd.read_csv(
    input_path+'T001L.txt',
    sep='|', header=3,
    encoding='latin1')
T001K_txt = pd.read_csv(
    input_path+'T001K.txt',
    sep='|',
    header=3,
    encoding='latin1')
MSEG_txt = pd.read_csv(
    input_path+'MSEG.txt',
    sep='|',
    header=3,
    encoding='latin1')

# Eliminar filas y columnas NaN
T001L_df = clean_columns(T001L_txt)
T001K_df = clean_columns(T001K_txt)
MSEG_df = clean_columns(MSEG_txt)

# Eliminar espacios en nombres de columnas y cada celda
T001L_df = clean_blankspace(T001L_df)
T001K_df = clean_blankspace(T001K_df)
MSEG_df = clean_blankspace(MSEG_df)

# Cambio de formato a columnas de MSEG
MSEG_df = num_format(MSEG_df, 'DMBTR')
MSEG_df = num_format(MSEG_df, 'ERFMG')

# Establecer esquema de datos
schema_T001L = {
    # Centro
    'WERKS': str,
    # Almacen
    'LGORT': str,
    # Denominacion
    'LGOBE': str
        }

schema_T001K = {
    # Centro
    'BWKEY': str,
    # Sociedad
    'BUKRS': str
    }

schema_MSEG = {
    # Doc_material
    'MBLNR': 'float64',
    # Posicion
    'ZEILE': 'float64',
    # Clase_mov
    'BWART': 'float64',
    # Material
    'MATNR': 'float64',
    # Centro
    'WERKS': str,
    # Almacen
    'LGORT': str,
    # Lote
    'CHARG': str,
    # Monto
    'DMBTR': 'float64',
    # Cantidad
    'ERFMG': 'float64',
    # Fecha_contabilidad
    'BUDAT_MKPF': str}

T001L_df = T001L_df.astype(schema_T001L)
T001K_df = T001K_df.astype(schema_T001K)
MSEG_df = MSEG_df.astype(schema_MSEG)

# Cambio de nombre columna BWKEY de T001K a WERKS
T001K_df.rename(columns={'BWKEY': 'WERKS'}, inplace=True)

# Ejecución de indicador
# Primero es necesario realizar cruces entre las tablas
aux_1 = pd.merge(T001K_df, T001L_df, on='WERKS', how='right')

# Cruce MSEG y aux_1
# Unimos las dos tablas
aux_2 = pd.merge(MSEG_df, aux_1, on=['WERKS', 'LGORT'], how='left')
aux_2 = aux_2.drop([
    'MBLNR',
    'ZEILE',
    'MATNR',
    'CHARG',
    'ERFMG',
    'BUDAT_MKPF'], axis=1).rename(columns={'WERKS': 'WERKS'})

# Calculo de sobrantes y faltantes por almacen y centro
# Sobrantes
aux_3 = aux_2.loc[(aux_2['BWART'] == 701), :]
sobrantes = aux_3.groupby(
    ['LGORT', 'WERKS', 'BWART']).agg({'DMBTR': 'sum'})
sobrantes = sobrantes.rename(columns={'DMBTR': 'Sobrantes'})

# Faltantes
aux_3 = aux_2.loc[(aux_2['BWART'] == 702), :]
faltantes = aux_3.groupby(
    ['LGORT', 'WERKS', 'BWART']).agg({'DMBTR': 'sum'})
faltantes = faltantes.rename(columns={'DMBTR': 'Faltantes'})

# Juntar información para crear el indicador
# Quitamos la columna monto de la tabla aux_2 y duplicados
aux_2 = aux_2.drop(['DMBTR', 'BWART'], axis=1)
aux_2 = aux_2.drop_duplicates()

# Combinamos tabla aux_2 con la de sobrantes
IG01_df = pd.merge(aux_2, sobrantes, on=['WERKS', 'LGORT'], how='outer')

# Combinamos la tabla anterior con la de faltantes
IG01_df = pd.merge(IG01_df, faltantes, on=['WERKS', 'LGORT'], how='outer')

# Rellenamos valores nulos con 0
IG01_df = IG01_df.fillna(0)

# Creamos nueva columna con el neto
IG01_df['Ajuste_neto'] = IG01_df['Sobrantes']-IG01_df['Faltantes']

# Reordenar columnas
IG01_df = IG01_df[[
    'BUKRS',
    'WERKS',
    'LGORT',
    'LGOBE',
    'Sobrantes',
    'Faltantes',
    'Ajuste_neto']]

# Ordenamos por Almacén
IG01_df = IG01_df.sort_values(
    by=('LGORT'),
    ascending=True
    ).reset_index(drop=True)

# Crear tabla resumen por sociedad
# Agrupamos por sociedad
# Eliminamos columnas que no se utilizan
IG01_aux = IG01_df.drop(['WERKS', 'LGORT', 'LGOBE'], axis=1)

# Sumamos los sobrantes por Sociedad
IG01_aux_sob = IG01_aux.groupby('BUKRS').agg({'Sobrantes': 'sum'})

# Sumamos los faltantes por Sociedad
IG01_aux_fal = IG01_aux.groupby('BUKRS').agg({'Faltantes': 'sum'})

# Sumamos los ajustes netos por Sociedad
IG01_aux_aj = IG01_aux.groupby('BUKRS').agg({'Ajuste_neto': 'sum'})

# Unir información de las sumas por sociedad
IG01_Sociedad = pd.merge(IG01_aux_sob, IG01_aux_fal, on='BUKRS')
IG01_Sociedad = pd.merge(IG01_Sociedad, IG01_aux_aj, on='BUKRS')
IG01_Sociedad.loc['Total'] = IG01_Sociedad.sum()

# Cambio de nombre de columnas
IG01_df.rename(columns={
    'BUKRS': 'Sociedad',
    'WERKS': 'Centro',
    'LGORT': 'Almacen',
    'LGOBE': 'Denominacion'}, inplace=True)
IG01_Sociedad.reset_index().rename(
    columns={'BUKRS': 'Sociedad'}, inplace=True)

# Guardamos el resultado en Excel
nombre_archivo = output_path + 'IG01_' + datetime.now().\
    strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
IG01_df.to_excel(writer, sheet_name='IG01')
IG01_Sociedad.to_excel(writer, sheet_name='IG01_Sociedad')
writer.save()
