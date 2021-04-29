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
is21_txt = pd.read_csv(input_path+'IS21.txt', sep='|', header=13, encoding='latin1')


# Eliminar filas y columnas nulas (NaN)
is21_df = clean_columns(is21_txt)
columnas_vacias = is21_df.columns[is21_df.isnull().sum() > 1].values.tolist()
is21_df = is21_df.drop(columns=columnas_vacias).dropna().reset_index(drop=True)

# Eliminar espacios en nombres de columnas y cada celda
is21_df = clean_blankspace(is21_df)
is21_df

# Crear archivo de resultados xlsx
nombre_archivo = output_path+'IS21 '+datetime.now().strftime(
    "%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
is21_df.to_excel(writer, sheet_name='IS21')
writer.save()
