
# Importar librerías

import pandas as pd
from datetime import datetime


input_path = 'C:/projects/auditoria_continua/data/input/current/'
output_path = 'C:/projects/auditoria_continua/data/output/current/'


# Utilidades
def clean_columns(txt):
    df = txt.drop(columns=txt.columns[0:1])\
        .drop(columns=txt.columns[-1]).dropna(how='all').reset_index(drop=True)
    return df


# Cargar archivos
is26_txt = pd.read_csv(
    input_path + 'IS26.txt',
    sep='|',
    header=12,
    encoding='latin1')

# Limpieza
# Eliminar filas y columnas nulas (NaN)
is26_df = clean_columns(is26_txt)

# Análisis para indicador
# Filtra df para que sólo aparezcan los usuarios cuyo valor
# anterior sea B (Sistema) ó C (Comunicación)
is26_df = is26_df[
    (is26_df['Valor ant.'] == 'B         ')
    | (is26_df['Valor ant.'] == 'C         ')]

# Filtra df para que sólo aparezcan los usuarios cuyo valor
# nuevo sea A (Dialogo) ó S (Servicio)
is26_df = is26_df[
    (is26_df['Valor nvo.'] == 'A         ')
    | (is26_df['Valor nvo.'] == 'S         ')]

# Crear archivo de resultados xlsx
nombre_archivo = output_path + 'IS26_'+datetime.now()\
    .strftime("%d-%m-%y_%Hh%Mm") + '.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')
is26_df.to_excel(writer, sheet_name='IS26')

writer.save()
