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
n1meevent = pd.read_csv(
    input_path + 'N1MEEVENT.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
nfal = pd.read_csv(
    input_path + 'NFAL.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
n1formulary = pd.read_csv(
    input_path + 'N1FORMULARY.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
mara = pd.read_csv(
    input_path + 'MARA.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
mbew = pd.read_csv(
    input_path + 'MBEW.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
n1odrug = pd.read_csv(
    input_path + 'N1ODRUG.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)
n1meorder = pd.read_csv(
    input_path + 'N1MEORDER.txt',
    sep='|',
    header=3,
    encoding='latin1',
    low_memory=False)

# Limpieza
n1meevent = clean_columns(n1meevent)
nfal = clean_columns(nfal)
n1formulary = clean_columns(n1formulary)
mara = clean_columns(mara)
mbew = clean_columns(mbew)
n1odrug = clean_columns(n1odrug)
n1meorder = clean_columns(n1meorder)

# Eliminar espacios en nombres de columnas y cada celda
n1meevent = clean_blankspace(n1meevent)
nfal = clean_blankspace(nfal)
n1formulary = clean_blankspace(n1formulary)
mara = clean_blankspace(mara)
mbew = clean_blankspace(mbew)
n1odrug = clean_blankspace(n1odrug)
n1meorder = clean_blankspace(n1meorder)

# Cambio de formato de columnas
mbew = num_format(mbew, 'VERPR')
nfal['FALNR'] = nfal['FALNR'].str.replace('R', '')

# Reemplaza valores vacios con np.nan
nfal = nfal.replace(r'^\s*$', np.nan, regex=True)
n1formulary = n1formulary.replace(r'^\s*$', np.nan, regex=True)

# Establecer esquema de datos
schema_n1meevent = {
    # N_prescripcion
    'MEORDID': 'float64',
    # Episodio
    'FALNR': 'float64',
    # Desc_evento
    'MESID': 'float64',
    # N_evento
    'MASTEV': 'float64',
    # Fecha_creacion
    'ERDAT': str}

schema_nfal = {
    # Episodio
    'FALNR': 'float64',
    # Clase_episodio
    'FALAR': 'float64',
    # Tipo_episodio
    'FATYP': 'float64'}

schema_n1formulary = {
    # ID_medicamento
    'DRUGID': str,
    # Material
    'MATNR': 'float64'}

schema_mara = {
    # Material
    'MATNR': 'float64',
    # Tipo_material
    'MTART': str}

schema_mbew = {
    # Material
    'MATNR': 'float64',
    # Precio_variable
    'VERPR': 'float64'}

schema_n1odrug = {
    # N_prescripcion
    'MEORDID': 'float64',
    # ID_medicamento
    'DRUGID': str}

schema_n1meorder = {
    # N_prescripcion
    'MEORDID': 'float64',
    # Paciente
    'PATNR': 'float64',
    # Episodio
    'FALNR': 'float64',
    # Texto_presc
    'MOTX': str,
    # UO_UE
    'ORGPF': str,
    # Tipo_presc
    'MOTYPID': 'float64'}

n1meevent = n1meevent.astype(schema_n1meevent)
nfal = nfal.astype(schema_nfal)
n1formulary = n1formulary.astype(schema_n1formulary)
mara = mara.astype(schema_mara)
mbew = mbew.astype(schema_mbew)
n1odrug = n1odrug.astype(schema_n1odrug)
n1meorder = n1meorder.astype(schema_n1meorder)

# Filtrar tablas
# Filtrar tipos de episodio y clase de episodio a analizar
nfal = nfal.loc[(nfal['FALAR'] == 1) & (nfal['FATYP'] == 2)]\
    .reset_index(drop=True)

# Filtrar tipos de prescripción a analizar
n1meorder = n1meorder.loc[(n1meorder['MOTYPID'] == 1)]\
    .reset_index(drop=True)

# Eliminar columnas innecesarias
# Eliminamos la columna "Clase_episodio"
# porque no la utilizaremos más adelante
nfal = nfal.drop(['FALAR'], axis=1)

# Eliminamos la columna "Tipo_presc"
# porque no la utilizaremos más adelante
n1meorder = n1meorder.drop(['MOTYPID'], axis=1)

# Ejecutar indicador
# Para poder llegar a la información necesaria,
# es necesario hacer los siguientes cruces en orden:
#  - **n1meveent y nfal** por `Episodio` >> parte1
#  - **parte1 y n1odrug** por `N_prescripcion` >> aux1
#  - **aux1 y n1formulary** por `ID_medicamento` >> aux2
#  - **aux2 y mara** por `Material` >> aux3
#  - **aux3 y mbew** por `Material` >> aux4
#  - **aux4 y n1meorder** por `N_prescripcion` >> IG04

# n1meveent y nfal
parte1 = pd.merge(n1meevent, nfal, on='FALNR', how='inner')

# Parte1 y n1odrug
aux1 = pd.merge(parte1, n1odrug, on='MEORDID', how='inner')

# aux1 y n1formulary
aux2 = pd.merge(aux1, n1formulary, on='DRUGID', how='left')

# aux2 y mara
aux3 = pd.merge(aux2, mara, on='MATNR', how='left')

# aux3 y mbew
aux4 = pd.merge(aux3, mbew, on='MATNR', how='left')

# aux4 y n1meorder
IG04 = pd.merge(aux4, n1meorder, on=['MEORDID', 'FALNR'], how='inner').\
    drop_duplicates()
suma_precio = IG04['VERPR'].sum().reshape(1,)
prescripciones_sin_duplicado = IG04['MEORDID'].drop_duplicates().\
    count().reshape(1,)
lista = [suma_precio, prescripciones_sin_duplicado]

Totales = pd.DataFrame({'Suma': lista[0], 'Cantidad': lista[1]})
Totales

# Cambio de nombre de columnas
IG04.rename(
    columns={
        'MEORDID': 'N_prescripcion',
        'FALNR': 'Episodio ',
        'MESID': 'Desc_evento',
        'MASTEV': 'N_evento',
        'ERDAT': 'Fecha_creacion',
        'FATYP': 'Tipo_episodio',
        'DRUGID': 'ID_medicamento',
        'MATNR': 'Material',
        'MTART': 'Tipo_material',
        'VERPR': 'Precio_variable',
        'PATNR': 'Paciente',
        'MOTX': 'Texto_presc',
        'ORGPF': 'UO_UE'}, inplace=True)

# Guardar en Excel
nombre_archivo = output_path + 'IG04_'+datetime.now().\
    strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')

IG04.to_excel(writer, sheet_name='IG04')
Totales.to_excel(writer, sheet_name='Totales')
writer.save()
