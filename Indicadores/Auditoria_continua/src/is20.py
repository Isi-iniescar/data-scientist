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
    df = txt.drop(columns=txt.columns[0:1]).drop(
        columns=txt.columns[-1]).dropna(how='all').reset_index(drop=True)
    return df

# Cargar archivos


is20_txt = pd.read_csv(
    input_path+'IS20.txt',
    sep='|',
    header=8,
    encoding='latin1')

# Eliminar filas y columnas nulas (NaN)
is20_df = clean_columns(is20_txt)

# Eliminar espacios en nombres de columnas y cada celda
is20_df = clean_blankspace(is20_df)

# Análisis para indicador
is20_df = is20_df[
    ((is20_df['Tipo'] == 'ADiálogo') |
        (is20_df['Tipo'] == 'SServ.')) &
    (is20_df['Usuario'] != 'SAP*')]

is20_df

# Crear archivo de resultados xlsx
nombre_archivo = output_path+'IS20 '+datetime.now().strftime(
    "%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
is20_df.to_excel(writer, sheet_name='IS20')
writer.save()
