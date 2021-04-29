
# Importar librerias
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date


input_path = 'C:/projects/auditoria_continua/data/input/current/'
history = 'C:/projects/auditoria_continua/data/output/seg_funcional/'

# Filas a saltar
skip_rows = np.arange(10).tolist()
columns_list_a = [
    'IDdeCC',
    'TextoCC',
    'Válidode',
    'Valideza',
    'Grupo',
    'Nºliquid.']
columns_list_b = [
    'IDdeCC',
    'Textocombin.crítica',
    'Válidode',
    'Valideza',
    'Grupo',
    'Nºliquid.']
columns_list_c = [
    'IDdeCC',
    'Textodelacombinacióncrítica(CC)',
    'Valideza',
    'Grupo',
    'Válidode',
    'Nºliquid.']
columns_list_d = [
    'IDdeAC',
    'TextoAC',
    'Válidode',
    'Valideza',
    'Grupo',
    'Nºliquid.']
columns_list_e = [
    'IDdeAC',
    'Textoautoriz.crít.',
    'Válidode',
    'Valideza',
    'Grupo',
    'Nºliquid.']
columns_dev_details = ['Indicador', 'Variante', 'Fecha']


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
    df = txt.drop(columns=txt.columns[0:1]).drop(columns=txt.columns[-1]).\
        dropna(how='all').reset_index(drop=True)
    return df


def num_format(df, col):
    df[col] = df[col].str.replace('.', '').str.replace(',', '.').astype(float)
    if (col == 'DMBTR') or (col == 'ERFMG') or (col == 'VERPR'):
        df[col] = df[col]*100
    return df


def val(row):
    if Fecha > row['Valideza']:
        val = 'CADUCADOS'
    else:
        val = 'ACTIVOS'
    return val


# Cargar tablas
ZBC_BC_BC_01 = pd.read_csv(
    input_path + 'ZBC_BC_BC_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_02 = pd.read_csv(
    input_path + 'ZBC_BC_BC_02.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_03 = pd.read_csv(
    input_path + 'ZBC_BC_BC_03.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_04 = pd.read_csv(
    input_path + 'ZBC_BC_BC_04.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_05 = pd.read_csv(
    input_path + 'ZBC_BC_BC_05.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_06 = pd.read_csv(
    input_path + 'ZBC_BC_BC_06.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_07 = pd.read_csv(
    input_path + 'ZBC_BC_BC_07.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_08 = pd.read_csv(
    input_path + 'ZBC_BC_BC_08.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZBC_BC_BC_09 = pd.read_csv(
    input_path + 'ZBC_BC_BC_09.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_MA_01 = pd.read_csv(
    input_path + 'ZFI_EF_MA_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_PA_01 = pd.read_csv(
    input_path + 'ZFI_EF_PA_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_PA_02 = pd.read_csv(
    input_path + 'ZFI_EF_PA_02.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_PA_03 = pd.read_csv(
    input_path + 'ZFI_EF_PA_03.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_PA_04 = pd.read_csv(
    input_path + 'ZFI_EF_PA_04.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_PA_05 = pd.read_csv(
    input_path + 'ZFI_EF_PA_05.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_EF_RM_01 = pd.read_csv(
    input_path + 'ZFI_EF_RM_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZFI_MA_PA_01 = pd.read_csv(
    input_path + 'ZFI_MA_PA_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZMM_OC_OL_01 = pd.read_csv(
    input_path + 'ZMM_OC_OL_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZMM_OC_RS_01 = pd.read_csv(
    input_path + 'ZMM_OC_RS_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
ZMM_SL_OL_01 = pd.read_csv(
    input_path + 'ZMM_SL_OL_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')

# Autorizaciones críticas
Z_ZF_BC_12 = pd.read_csv(
    input_path + 'Z_ZF_BC_12.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_BC_13 = pd.read_csv(
    input_path + 'Z_ZF_BC_13.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_BC_14 = pd.read_csv(
    input_path + 'Z_ZF_BC_14.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_HR_01 = pd.read_csv(
    input_path + 'Z_ZF_HR_01.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_HR_02 = pd.read_csv(
    input_path + 'Z_ZF_HR_02.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_HR_03 = pd.read_csv(
    input_path + 'Z_ZF_HR_03.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_HR_04 = pd.read_csv(
    input_path + 'Z_ZF_HR_04.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_HR_05 = pd.read_csv(
    input_path + 'Z_ZF_HR_05.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_HR_06 = pd.read_csv(
    input_path + 'Z_ZF_HR_06.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_EF_09 = pd.read_csv(
    input_path + 'Z_ZF_EF_09.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_EF_10 = pd.read_csv(
    input_path + 'Z_ZF_EF_10.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_EF_13 = pd.read_csv(
    input_path + 'Z_ZF_EF_13.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_EF_14 = pd.read_csv(
    input_path + 'Z_ZF_EF_14.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_EF_15 = pd.read_csv(
    input_path + 'Z_ZF_EF_15.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')
Z_ZF_EF_16 = pd.read_csv(
    input_path + 'Z_ZF_EF_16.txt',
    sep='|',
    skiprows=skip_rows,
    encoding='latin1')

# Base con infomación histórica
base_cc = pd.read_excel(
    history + 'Monitoreo IS.xlsx',
    sheet_name='Combinaciones críticas')
base_ac = pd.read_excel(
    history + 'Monitoreo IS.xlsx',
    sheet_name='Autorizaciones críticas')
detalle_des = pd.read_excel(
    history + 'Monitoreo IS.xlsx',
    sheet_name='Detalle Desviaciones')

# Limpieza de tablas
# Combinaciones críticas
# Eliminar filas y columnas NaN
ZBC_BC_BC_01 = clean_columns(ZBC_BC_BC_01)
ZBC_BC_BC_02 = clean_columns(ZBC_BC_BC_02)
ZBC_BC_BC_03 = clean_columns(ZBC_BC_BC_03)
ZBC_BC_BC_04 = clean_columns(ZBC_BC_BC_04)
ZBC_BC_BC_05 = clean_columns(ZBC_BC_BC_05)
ZBC_BC_BC_06 = clean_columns(ZBC_BC_BC_06)
ZBC_BC_BC_07 = clean_columns(ZBC_BC_BC_07)
ZBC_BC_BC_08 = clean_columns(ZBC_BC_BC_08)
ZBC_BC_BC_09 = clean_columns(ZBC_BC_BC_09)
ZFI_EF_MA_01 = clean_columns(ZFI_EF_MA_01)
ZFI_EF_PA_01 = clean_columns(ZFI_EF_PA_01)
ZFI_EF_PA_02 = clean_columns(ZFI_EF_PA_02)
ZFI_EF_PA_03 = clean_columns(ZFI_EF_PA_03)
ZFI_EF_PA_04 = clean_columns(ZFI_EF_PA_04)
ZFI_EF_PA_05 = clean_columns(ZFI_EF_PA_05)
ZFI_EF_RM_01 = clean_columns(ZFI_EF_RM_01)
ZFI_MA_PA_01 = clean_columns(ZFI_MA_PA_01)
ZMM_OC_OL_01 = clean_columns(ZMM_OC_OL_01)
ZMM_OC_RS_01 = clean_columns(ZMM_OC_RS_01)
ZMM_SL_OL_01 = clean_columns(ZMM_SL_OL_01)

# Eliminar espacios en nombres de columnas y cada celda
ZBC_BC_BC_01 = clean_blankspace(ZBC_BC_BC_01)
ZBC_BC_BC_02 = clean_blankspace(ZBC_BC_BC_02)
ZBC_BC_BC_03 = clean_blankspace(ZBC_BC_BC_03)
ZBC_BC_BC_04 = clean_blankspace(ZBC_BC_BC_04)
ZBC_BC_BC_05 = clean_blankspace(ZBC_BC_BC_05)
ZBC_BC_BC_06 = clean_blankspace(ZBC_BC_BC_06)
ZBC_BC_BC_07 = clean_blankspace(ZBC_BC_BC_07)
ZBC_BC_BC_08 = clean_blankspace(ZBC_BC_BC_08)
ZBC_BC_BC_09 = clean_blankspace(ZBC_BC_BC_09)
ZFI_EF_MA_01 = clean_blankspace(ZFI_EF_MA_01)
ZFI_EF_PA_01 = clean_blankspace(ZFI_EF_PA_01)
ZFI_EF_PA_02 = clean_blankspace(ZFI_EF_PA_02)
ZFI_EF_PA_03 = clean_blankspace(ZFI_EF_PA_03)
ZFI_EF_PA_04 = clean_blankspace(ZFI_EF_PA_04)
ZFI_EF_PA_05 = clean_blankspace(ZFI_EF_PA_05)
ZFI_EF_RM_01 = clean_blankspace(ZFI_EF_RM_01)
ZFI_MA_PA_01 = clean_blankspace(ZFI_MA_PA_01)
ZMM_OC_OL_01 = clean_blankspace(ZMM_OC_OL_01)
ZMM_OC_RS_01 = clean_blankspace(ZMM_OC_RS_01)
ZMM_SL_OL_01 = clean_blankspace(ZMM_SL_OL_01)

# ***Reemplazar valores de Valideza del año 9999***
hoy = datetime.now().strftime('%d.%m.%Y')

ZBC_BC_BC_01['Valideza'] = ZBC_BC_BC_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_02['Valideza'] = ZBC_BC_BC_02.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_03['Valideza'] = ZBC_BC_BC_03.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_04['Valideza'] = ZBC_BC_BC_04.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_05['Valideza'] = ZBC_BC_BC_05.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_06['Valideza'] = ZBC_BC_BC_06.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_07['Valideza'] = ZBC_BC_BC_07.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_08['Valideza'] = ZBC_BC_BC_08.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZBC_BC_BC_09['Valideza'] = ZBC_BC_BC_09.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_MA_01['Valideza'] = ZFI_EF_MA_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_PA_01['Valideza'] = ZFI_EF_PA_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_PA_02['Valideza'] = ZFI_EF_PA_02.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_PA_03['Valideza'] = ZFI_EF_PA_03.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_PA_04['Valideza'] = ZFI_EF_PA_04.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_PA_05['Valideza'] = ZFI_EF_PA_05.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_EF_RM_01['Valideza'] = ZFI_EF_RM_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZFI_MA_PA_01['Valideza'] = ZFI_MA_PA_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZMM_OC_OL_01['Valideza'] = ZMM_OC_OL_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZMM_OC_RS_01['Valideza'] = ZMM_OC_RS_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)
ZMM_SL_OL_01['Valideza'] = ZMM_SL_OL_01.Valideza.str.\
    replace(r'(^.*9999.*$)', hoy)

# ***Transformar los valores de Valideza de str en float***
ZBC_BC_BC_01 = num_format(ZBC_BC_BC_01, 'Valideza')
ZBC_BC_BC_02 = num_format(ZBC_BC_BC_02, 'Valideza')
ZBC_BC_BC_03 = num_format(ZBC_BC_BC_03, 'Valideza')
ZBC_BC_BC_04 = num_format(ZBC_BC_BC_04, 'Valideza')
ZBC_BC_BC_05 = num_format(ZBC_BC_BC_05, 'Valideza')
ZBC_BC_BC_06 = num_format(ZBC_BC_BC_06, 'Valideza')
ZBC_BC_BC_07 = num_format(ZBC_BC_BC_07, 'Valideza')
ZBC_BC_BC_08 = num_format(ZBC_BC_BC_08, 'Valideza')
ZBC_BC_BC_09 = num_format(ZBC_BC_BC_09, 'Valideza')
ZFI_EF_MA_01 = num_format(ZFI_EF_MA_01, 'Valideza')
ZFI_EF_PA_01 = num_format(ZFI_EF_PA_01, 'Valideza')
ZFI_EF_PA_02 = num_format(ZFI_EF_PA_02, 'Valideza')
ZFI_EF_PA_03 = num_format(ZFI_EF_PA_03, 'Valideza')
ZFI_EF_PA_04 = num_format(ZFI_EF_PA_04, 'Valideza')
ZFI_EF_PA_05 = num_format(ZFI_EF_PA_05, 'Valideza')
ZFI_EF_RM_01 = num_format(ZFI_EF_RM_01, 'Valideza')
ZFI_MA_PA_01 = num_format(ZFI_MA_PA_01, 'Valideza')
ZMM_OC_OL_01 = num_format(ZMM_OC_OL_01, 'Valideza')
ZMM_OC_RS_01 = num_format(ZMM_OC_RS_01, 'Valideza')
ZMM_SL_OL_01 = num_format(ZMM_SL_OL_01, 'Valideza')

# ***Transformar a formato fecha la columna Valideza***
ZBC_BC_BC_01['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_01['Valideza'], format='%d%m%Y')
ZBC_BC_BC_02['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_02['Valideza'], format='%d%m%Y')
ZBC_BC_BC_03['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_03['Valideza'], format='%d%m%Y')
ZBC_BC_BC_04['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_04['Valideza'], format='%d%m%Y')
ZBC_BC_BC_05['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_05['Valideza'], format='%d%m%Y')
ZBC_BC_BC_06['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_06['Valideza'], format='%d%m%Y')
ZBC_BC_BC_07['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_07['Valideza'], format='%d%m%Y')
ZBC_BC_BC_08['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_08['Valideza'], format='%d%m%Y')
ZBC_BC_BC_09['Valideza'] = pd.to_datetime(
    ZBC_BC_BC_09['Valideza'], format='%d%m%Y')
ZFI_EF_MA_01['Valideza'] = pd.to_datetime(
    ZFI_EF_MA_01['Valideza'], format='%d%m%Y')
ZFI_EF_PA_01['Valideza'] = pd.to_datetime(
    ZFI_EF_PA_01['Valideza'], format='%d%m%Y')
ZFI_EF_PA_02['Valideza'] = pd.to_datetime(
    ZFI_EF_PA_02['Valideza'], format='%d%m%Y')
ZFI_EF_PA_03['Valideza'] = pd.to_datetime(
    ZFI_EF_PA_03['Valideza'], format='%d%m%Y')
ZFI_EF_PA_04['Valideza'] = pd.to_datetime(
    ZFI_EF_PA_04['Valideza'], format='%d%m%Y')
ZFI_EF_PA_05['Valideza'] = pd.to_datetime(
    ZFI_EF_PA_05['Valideza'], format='%d%m%Y')
ZFI_EF_RM_01['Valideza'] = pd.to_datetime(
    ZFI_EF_RM_01['Valideza'], format='%d%m%Y')
ZFI_MA_PA_01['Valideza'] = pd.to_datetime(
    ZFI_MA_PA_01['Valideza'], format='%d%m%Y')
ZMM_OC_OL_01['Valideza'] = pd.to_datetime(
    ZMM_OC_OL_01['Valideza'], format='%d%m%Y')
ZMM_OC_RS_01['Valideza'] = pd.to_datetime(
    ZMM_OC_RS_01['Valideza'], format='%d%m%Y')
ZMM_SL_OL_01['Valideza'] = pd.to_datetime(
    ZMM_SL_OL_01['Valideza'], format='%d%m%Y')


# ***Crear columna para verificar grupo***
Fecha = date.today()

ZBC_BC_BC_01['Estado_calculado'] = ZBC_BC_BC_01.apply(val, axis=1)
ZBC_BC_BC_02['Estado_calculado'] = ZBC_BC_BC_02.apply(val, axis=1)
ZBC_BC_BC_03['Estado_calculado'] = ZBC_BC_BC_03.apply(val, axis=1)
ZBC_BC_BC_04['Estado_calculado'] = ZBC_BC_BC_04.apply(val, axis=1)
ZBC_BC_BC_05['Estado_calculado'] = ZBC_BC_BC_05.apply(val, axis=1)
ZBC_BC_BC_06['Estado_calculado'] = ZBC_BC_BC_06.apply(val, axis=1)
ZBC_BC_BC_07['Estado_calculado'] = ZBC_BC_BC_07.apply(val, axis=1)
ZBC_BC_BC_08['Estado_calculado'] = ZBC_BC_BC_08.apply(val, axis=1)
ZBC_BC_BC_09['Estado_calculado'] = ZBC_BC_BC_09.apply(val, axis=1)
ZFI_EF_MA_01['Estado_calculado'] = ZFI_EF_MA_01.apply(val, axis=1)
ZFI_EF_PA_01['Estado_calculado'] = ZFI_EF_PA_01.apply(val, axis=1)
ZFI_EF_PA_02['Estado_calculado'] = ZFI_EF_PA_02.apply(val, axis=1)
ZFI_EF_PA_03['Estado_calculado'] = ZFI_EF_PA_03.apply(val, axis=1)
ZFI_EF_PA_04['Estado_calculado'] = ZFI_EF_PA_04.apply(val, axis=1)
ZFI_EF_PA_05['Estado_calculado'] = ZFI_EF_PA_05.apply(val, axis=1)
ZFI_EF_RM_01['Estado_calculado'] = ZFI_EF_RM_01.apply(val, axis=1)
ZFI_MA_PA_01['Estado_calculado'] = ZFI_MA_PA_01.apply(val, axis=1)
ZMM_OC_OL_01['Estado_calculado'] = ZMM_OC_OL_01.apply(val, axis=1)
ZMM_OC_RS_01['Estado_calculado'] = ZMM_OC_RS_01.apply(val, axis=1)
ZMM_SL_OL_01['Estado_calculado'] = ZMM_SL_OL_01.apply(val, axis=1)

# Eliminar columnas que no son de interés y valores duplicados
ZBC_BC_BC_01 = ZBC_BC_BC_01.drop(columns_list_a, axis=1).drop_duplicates()
ZBC_BC_BC_02 = ZBC_BC_BC_02.drop(columns_list_b, axis=1).drop_duplicates()
ZBC_BC_BC_03 = ZBC_BC_BC_03.drop(columns_list_b, axis=1).drop_duplicates()
ZBC_BC_BC_04 = ZBC_BC_BC_04.drop(columns_list_b, axis=1).drop_duplicates()
ZBC_BC_BC_05 = ZBC_BC_BC_05.drop(columns_list_b, axis=1).drop_duplicates()
ZBC_BC_BC_06 = ZBC_BC_BC_06.drop(columns_list_b, axis=1).drop_duplicates()
ZBC_BC_BC_07 = ZBC_BC_BC_07.drop(columns_list_b, axis=1).drop_duplicates()
ZBC_BC_BC_08 = ZBC_BC_BC_08.drop(columns_list_c, axis=1).drop_duplicates()
ZBC_BC_BC_09 = ZBC_BC_BC_09.drop(columns_list_c, axis=1).drop_duplicates()
ZFI_EF_MA_01 = ZFI_EF_MA_01.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_EF_PA_01 = ZFI_EF_PA_01.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_EF_PA_02 = ZFI_EF_PA_02.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_EF_PA_03 = ZFI_EF_PA_03.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_EF_PA_04 = ZFI_EF_PA_04.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_EF_PA_05 = ZFI_EF_PA_05.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_EF_RM_01 = ZFI_EF_RM_01.drop(columns_list_b, axis=1).drop_duplicates()
ZFI_MA_PA_01 = ZFI_MA_PA_01.drop(columns_list_b, axis=1).drop_duplicates()
ZMM_OC_OL_01 = ZMM_OC_OL_01.drop(columns_list_b, axis=1).drop_duplicates()
ZMM_OC_RS_01 = ZMM_OC_RS_01.drop(columns_list_b, axis=1).drop_duplicates()
ZMM_SL_OL_01 = ZMM_SL_OL_01.drop(columns_list_b, axis=1).drop_duplicates()

# Autoizaciones críticas
# Eliminar filas y columnas NaN
Z_ZF_BC_12 = clean_columns(Z_ZF_BC_12)
Z_ZF_BC_13 = clean_columns(Z_ZF_BC_13)
Z_ZF_BC_14 = clean_columns(Z_ZF_BC_14)
Z_ZF_HR_01 = clean_columns(Z_ZF_HR_01)
Z_ZF_HR_02 = clean_columns(Z_ZF_HR_02)
Z_ZF_HR_03 = clean_columns(Z_ZF_HR_03)
Z_ZF_HR_04 = clean_columns(Z_ZF_HR_04)
Z_ZF_HR_05 = clean_columns(Z_ZF_HR_05)
Z_ZF_HR_06 = clean_columns(Z_ZF_HR_06)
Z_ZF_EF_09 = clean_columns(Z_ZF_EF_09)
Z_ZF_EF_10 = clean_columns(Z_ZF_EF_10)
Z_ZF_EF_13 = clean_columns(Z_ZF_EF_13)
Z_ZF_EF_14 = clean_columns(Z_ZF_EF_14)
Z_ZF_EF_15 = clean_columns(Z_ZF_EF_15)
Z_ZF_EF_16 = clean_columns(Z_ZF_EF_16)

# Eliminar espacios en nombres de columnas y cada celda
Z_ZF_BC_12 = clean_blankspace(Z_ZF_BC_12)
Z_ZF_BC_13 = clean_blankspace(Z_ZF_BC_13)
Z_ZF_BC_14 = clean_blankspace(Z_ZF_BC_14)
Z_ZF_HR_01 = clean_blankspace(Z_ZF_HR_01)
Z_ZF_HR_02 = clean_blankspace(Z_ZF_HR_02)
Z_ZF_HR_03 = clean_blankspace(Z_ZF_HR_03)
Z_ZF_HR_04 = clean_blankspace(Z_ZF_HR_04)
Z_ZF_HR_05 = clean_blankspace(Z_ZF_HR_05)
Z_ZF_HR_06 = clean_blankspace(Z_ZF_HR_06)
Z_ZF_EF_09 = clean_blankspace(Z_ZF_EF_09)
Z_ZF_EF_10 = clean_blankspace(Z_ZF_EF_10)
Z_ZF_EF_13 = clean_blankspace(Z_ZF_EF_13)
Z_ZF_EF_14 = clean_blankspace(Z_ZF_EF_14)
Z_ZF_EF_15 = clean_blankspace(Z_ZF_EF_15)
Z_ZF_EF_16 = clean_blankspace(Z_ZF_EF_16)

# ***Reemplazar valores de Valideza del año 9999***
Z_ZF_BC_12['Valideza'] = Z_ZF_BC_12.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_BC_13['Valideza'] = Z_ZF_BC_13.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_BC_14['Valideza'] = Z_ZF_BC_14.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_HR_01['Valideza'] = Z_ZF_HR_01.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_HR_02['Valideza'] = Z_ZF_HR_02.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_HR_03['Valideza'] = Z_ZF_HR_03.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_HR_04['Valideza'] = Z_ZF_HR_04.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_HR_05['Valideza'] = Z_ZF_HR_05.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_HR_06['Valideza'] = Z_ZF_HR_06.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_EF_09['Valideza'] = Z_ZF_EF_09.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_EF_10['Valideza'] = Z_ZF_EF_10.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_EF_13['Valideza'] = Z_ZF_EF_13.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_EF_14['Valideza'] = Z_ZF_EF_14.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_EF_15['Valideza'] = Z_ZF_EF_15.Valideza.str.replace(r'(^.*9999.*$)', hoy)
Z_ZF_EF_16['Valideza'] = Z_ZF_EF_16.Valideza.str.replace(r'(^.*9999.*$)', hoy)

# ***Transformar los valores de Valideza de str en float***
Z_ZF_BC_12 = num_format(Z_ZF_BC_12, 'Valideza')
Z_ZF_BC_13 = num_format(Z_ZF_BC_13, 'Valideza')
Z_ZF_BC_14 = num_format(Z_ZF_BC_14, 'Valideza')
Z_ZF_HR_01 = num_format(Z_ZF_HR_01, 'Valideza')
Z_ZF_HR_02 = num_format(Z_ZF_HR_02, 'Valideza')
Z_ZF_HR_03 = num_format(Z_ZF_HR_03, 'Valideza')
Z_ZF_HR_04 = num_format(Z_ZF_HR_04, 'Valideza')
Z_ZF_HR_05 = num_format(Z_ZF_HR_05, 'Valideza')
Z_ZF_HR_06 = num_format(Z_ZF_HR_06, 'Valideza')
Z_ZF_EF_09 = num_format(Z_ZF_EF_09, 'Valideza')
Z_ZF_EF_10 = num_format(Z_ZF_EF_10, 'Valideza')
Z_ZF_EF_13 = num_format(Z_ZF_EF_13, 'Valideza')
Z_ZF_EF_14 = num_format(Z_ZF_EF_14, 'Valideza')
Z_ZF_EF_15 = num_format(Z_ZF_EF_15, 'Valideza')
Z_ZF_EF_16 = num_format(Z_ZF_EF_16, 'Valideza')

# ***Transformar a formato fecha la columna Valideza***
Z_ZF_BC_12['Valideza'] = pd.to_datetime(
    Z_ZF_BC_12['Valideza'], format='%d%m%Y')
Z_ZF_BC_13['Valideza'] = pd.to_datetime(
    Z_ZF_BC_13['Valideza'], format='%d%m%Y')
Z_ZF_BC_14['Valideza'] = pd.to_datetime(
    Z_ZF_BC_14['Valideza'], format='%d%m%Y')
Z_ZF_HR_01['Valideza'] = pd.to_datetime(
    Z_ZF_HR_01['Valideza'], format='%d%m%Y')
Z_ZF_HR_02['Valideza'] = pd.to_datetime(
    Z_ZF_HR_02['Valideza'], format='%d%m%Y')
Z_ZF_HR_03['Valideza'] = pd.to_datetime(
    Z_ZF_HR_03['Valideza'], format='%d%m%Y')
Z_ZF_HR_04['Valideza'] = pd.to_datetime(
    Z_ZF_HR_04['Valideza'], format='%d%m%Y')
Z_ZF_HR_05['Valideza'] = pd.to_datetime(
    Z_ZF_HR_05['Valideza'], format='%d%m%Y')
Z_ZF_HR_06['Valideza'] = pd.to_datetime(
    Z_ZF_HR_06['Valideza'], format='%d%m%Y')
Z_ZF_EF_09['Valideza'] = pd.to_datetime(
    Z_ZF_EF_09['Valideza'], format='%d%m%Y')
Z_ZF_EF_10['Valideza'] = pd.to_datetime(
    Z_ZF_EF_10['Valideza'], format='%d%m%Y')
Z_ZF_EF_13['Valideza'] = pd.to_datetime(
    Z_ZF_EF_13['Valideza'], format='%d%m%Y')
Z_ZF_EF_14['Valideza'] = pd.to_datetime(
    Z_ZF_EF_14['Valideza'], format='%d%m%Y')
Z_ZF_EF_15['Valideza'] = pd.to_datetime(
    Z_ZF_EF_15['Valideza'], format='%d%m%Y')
Z_ZF_EF_16['Valideza'] = pd.to_datetime(
    Z_ZF_EF_16['Valideza'], format='%d%m%Y')

# ***Crear columna para verificar grupo***
Z_ZF_BC_12['Estado_calculado'] = Z_ZF_BC_12.apply(val, axis=1)
Z_ZF_BC_13['Estado_calculado'] = Z_ZF_BC_13.apply(val, axis=1)
Z_ZF_BC_14['Estado_calculado'] = Z_ZF_BC_14.apply(val, axis=1)
Z_ZF_HR_01['Estado_calculado'] = Z_ZF_HR_01.apply(val, axis=1)
Z_ZF_HR_02['Estado_calculado'] = Z_ZF_HR_02.apply(val, axis=1)
Z_ZF_HR_03['Estado_calculado'] = Z_ZF_HR_03.apply(val, axis=1)
Z_ZF_HR_04['Estado_calculado'] = Z_ZF_HR_04.apply(val, axis=1)
Z_ZF_HR_05['Estado_calculado'] = Z_ZF_HR_05.apply(val, axis=1)
Z_ZF_HR_06['Estado_calculado'] = Z_ZF_HR_06.apply(val, axis=1)
Z_ZF_EF_09['Estado_calculado'] = Z_ZF_EF_09.apply(val, axis=1)
Z_ZF_EF_10['Estado_calculado'] = Z_ZF_EF_10.apply(val, axis=1)
Z_ZF_EF_13['Estado_calculado'] = Z_ZF_EF_13.apply(val, axis=1)
Z_ZF_EF_14['Estado_calculado'] = Z_ZF_EF_14.apply(val, axis=1)
Z_ZF_EF_15['Estado_calculado'] = Z_ZF_EF_15.apply(val, axis=1)
Z_ZF_EF_16['Estado_calculado'] = Z_ZF_EF_16.apply(val, axis=1)

# Eliminar columnas que no son de interés y valores duplicados
Z_ZF_BC_12 = Z_ZF_BC_12.drop(columns_list_d, axis=1).drop_duplicates()
Z_ZF_BC_13 = Z_ZF_BC_13.drop(columns_list_d, axis=1).drop_duplicates()
Z_ZF_BC_14 = Z_ZF_BC_14.drop(columns_list_d, axis=1).drop_duplicates()
Z_ZF_HR_01 = Z_ZF_HR_01.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_HR_02 = Z_ZF_HR_02.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_HR_03 = Z_ZF_HR_03.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_HR_04 = Z_ZF_HR_04.drop(columns_list_d, axis=1).drop_duplicates()
Z_ZF_HR_05 = Z_ZF_HR_05.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_HR_06 = Z_ZF_HR_06.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_EF_09 = Z_ZF_EF_09.drop(columns_list_d, axis=1).drop_duplicates()
Z_ZF_EF_10 = Z_ZF_EF_10.drop(columns_list_d, axis=1).drop_duplicates()
Z_ZF_EF_13 = Z_ZF_EF_13.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_EF_14 = Z_ZF_EF_14.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_EF_15 = Z_ZF_EF_15.drop(columns_list_e, axis=1).drop_duplicates()
Z_ZF_EF_16 = Z_ZF_EF_16.drop(columns_list_d, axis=1).drop_duplicates()

# Ejecución de indicadores
Fecha = date.today().strftime("%d-%m-%y")

# Combinaciones críticas
# **ZBC_BC_BC_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_01 = ZBC_BC_BC_01.loc[
    (ZBC_BC_BC_01['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta1 = len(ZBC_BC_BC_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta1]
ZBC_BC_BC_01_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_02**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_02 = ZBC_BC_BC_02.loc[
    (ZBC_BC_BC_02['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_02['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta2 = len(ZBC_BC_BC_02.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta2]
ZBC_BC_BC_02_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_02', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_03**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_03 = ZBC_BC_BC_03.loc[
    (ZBC_BC_BC_03['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_03['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta3 = len(ZBC_BC_BC_03.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta3]
ZBC_BC_BC_03_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_03', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_04**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_04 = ZBC_BC_BC_04.loc[
    (ZBC_BC_BC_04['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_04['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta4 = len(ZBC_BC_BC_04.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta4]
ZBC_BC_BC_04_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_04', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_05**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_05 = ZBC_BC_BC_05.loc[
    (ZBC_BC_BC_05['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_05['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta5 = len(ZBC_BC_BC_05.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta5]
ZBC_BC_BC_05_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_05', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_06**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_06 = ZBC_BC_BC_06.loc[
    (ZBC_BC_BC_06['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_06['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta6 = len(ZBC_BC_BC_06.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta6]
ZBC_BC_BC_06_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_06', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_07**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_07 = ZBC_BC_BC_07.loc[
    (ZBC_BC_BC_07['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_07['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta7 = len(ZBC_BC_BC_07.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta7]
ZBC_BC_BC_07_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_07', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_08**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_08 = ZBC_BC_BC_08.loc[
    (ZBC_BC_BC_08['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_08['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta8 = len(ZBC_BC_BC_08.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta8]
ZBC_BC_BC_08_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_08', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZBC_BC_BC_09**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZBC_BC_BC_09 = ZBC_BC_BC_09.loc[
    (ZBC_BC_BC_09['Tipo'] != 'SServ.')
    & (ZBC_BC_BC_09['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta9 = len(ZBC_BC_BC_09.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta9]
ZBC_BC_BC_09_r = pd.DataFrame(
    {'Variante': 'ZBC_BC_BC_09', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_MA_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_MA_01 = ZFI_EF_MA_01.loc[
    (ZFI_EF_MA_01['Tipo'] != 'SServ.')
    & (ZFI_EF_MA_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta10 = len(ZFI_EF_MA_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta10]
ZFI_EF_MA_01_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_MA_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_PA_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_PA_01 = ZFI_EF_PA_01.loc[
    (ZFI_EF_PA_01['Tipo'] != 'SServ.')
    & (ZFI_EF_PA_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta11 = len(ZFI_EF_PA_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta11]
ZFI_EF_PA_01_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_PA_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_PA_02**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_PA_02 = ZFI_EF_PA_02.loc[
    (ZFI_EF_PA_02['Tipo'] != 'SServ.')
    & (ZFI_EF_PA_02['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta12 = len(ZFI_EF_PA_02.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta12]
ZFI_EF_PA_02_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_PA_02', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_PA_03**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_PA_03 = ZFI_EF_PA_03.loc[
    (ZFI_EF_PA_03['Tipo'] != 'SServ.')
    & (ZFI_EF_PA_03['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta13 = len(ZFI_EF_PA_03.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta13]
ZFI_EF_PA_03_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_PA_03', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_PA_04**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_PA_04 = ZFI_EF_PA_04.loc[
    (ZFI_EF_PA_04['Tipo'] != 'SServ.')
    & (ZFI_EF_PA_04['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta14 = len(ZFI_EF_PA_04.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta14]
ZFI_EF_PA_04_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_PA_04', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_PA_05**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_PA_05 = ZFI_EF_PA_05.loc[
    (ZFI_EF_PA_05['Tipo'] != 'SServ.')
    & (ZFI_EF_PA_05['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta15 = len(ZFI_EF_PA_05.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta15]
ZFI_EF_PA_05_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_PA_05', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_EF_RM_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_EF_RM_01 = ZFI_EF_RM_01.loc[
    (ZFI_EF_RM_01['Tipo'] != 'SServ.')
    & (ZFI_EF_RM_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta16 = len(ZFI_EF_RM_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta16]
ZFI_EF_RM_01_r = pd.DataFrame(
    {'Variante': 'ZFI_EF_RM_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZFI_MA_PA_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZFI_MA_PA_01 = ZFI_MA_PA_01.loc[
    (ZFI_MA_PA_01['Tipo'] != 'SServ.')
    & (ZFI_MA_PA_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta17 = len(ZFI_MA_PA_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta17]
ZFI_MA_PA_01_r = pd.DataFrame(
    {'Variante': 'ZFI_MA_PA_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZMM_OC_OL_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZMM_OC_OL_01 = ZMM_OC_OL_01.loc[
    (ZMM_OC_OL_01['Tipo'] != 'SServ.')
    & (ZMM_OC_OL_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta18 = len(ZMM_OC_OL_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta18]
ZMM_OC_OL_01_r = pd.DataFrame(
    {'Variante': 'ZMM_OC_OL_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZMM_OC_RS_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZMM_OC_RS_01 = ZMM_OC_RS_01.loc[
    (ZMM_OC_RS_01['Tipo'] != 'SServ.')
    & (ZMM_OC_RS_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta19 = len(ZMM_OC_RS_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta19]
ZMM_OC_RS_01_r = pd.DataFrame(
    {'Variante': 'ZMM_OC_RS_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **ZMM_SL_OL_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
ZMM_SL_OL_01 = ZMM_SL_OL_01.loc[
    (ZMM_SL_OL_01['Tipo'] != 'SServ.')
    & (ZMM_SL_OL_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta20 = len(ZMM_SL_OL_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta20]
ZMM_SL_OL_01_r = pd.DataFrame(
    {'Variante': 'ZMM_SL_OL_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Unir todos los resultados de combinaciones críticas**
comb_criticas = pd.concat(
    [
        ZBC_BC_BC_01_r, ZBC_BC_BC_02_r, ZBC_BC_BC_03_r, ZBC_BC_BC_04_r,
        ZBC_BC_BC_05_r, ZBC_BC_BC_06_r, ZBC_BC_BC_07_r, ZBC_BC_BC_08_r,
        ZBC_BC_BC_09_r, ZFI_EF_MA_01_r, ZFI_EF_PA_01_r, ZFI_EF_PA_02_r,
        ZFI_EF_PA_03_r, ZFI_EF_PA_04_r, ZFI_EF_PA_05_r, ZFI_EF_RM_01_r,
        ZFI_MA_PA_01_r, ZMM_OC_OL_01_r, ZMM_OC_RS_01_r, ZMM_SL_OL_01_r
        ], axis=0).reset_index(drop=True)

# Autorizaciones críticas
# **Z_ZF_BC_12**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_BC_12 = Z_ZF_BC_12.loc[
    (Z_ZF_BC_12['Tipo'] != 'SServ.')
    & (Z_ZF_BC_12['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta21 = len(Z_ZF_BC_12.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta21]
Z_ZF_BC_12_r = pd.DataFrame(
    {'Variante': 'Z_ZF_BC_12', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_BC_13**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_BC_13 = Z_ZF_BC_13.loc[
    (Z_ZF_BC_13['Tipo'] != 'SServ.')
    & (Z_ZF_BC_13['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta22 = len(Z_ZF_BC_13.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta22]
Z_ZF_BC_13_r = pd.DataFrame(
    {'Variante': 'Z_ZF_BC_13', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_BC_14**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_BC_14 = Z_ZF_BC_14.loc[
    (Z_ZF_BC_14['Tipo'] != 'SServ.')
    & (Z_ZF_BC_14['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta23 = len(Z_ZF_BC_14.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta23]
Z_ZF_BC_14_r = pd.DataFrame(
    {'Variante': 'Z_ZF_BC_14', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_HR_01**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_HR_01 = Z_ZF_HR_01.loc[
    (Z_ZF_HR_01['Tipo'] != 'SServ.')
    & (Z_ZF_HR_01['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta24 = len(Z_ZF_HR_01.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta24]
Z_ZF_HR_01_r = pd.DataFrame(
    {'Variante': 'Z_ZF_HR_01', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_HR_02**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_HR_02 = Z_ZF_HR_02.loc[
    (Z_ZF_HR_02['Tipo'] != 'SServ.')
    & (Z_ZF_HR_02['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta25 = len(Z_ZF_HR_02.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta25]
Z_ZF_HR_02_r = pd.DataFrame(
    {'Variante': 'Z_ZF_HR_02', 'Fecha': Fecha, 'Desviaciones': cuenta})

# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_HR_03 = Z_ZF_HR_03.loc[
    (Z_ZF_HR_03['Tipo'] != 'SServ.')
    & (Z_ZF_HR_03['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta26 = len(Z_ZF_HR_03.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta26]
Z_ZF_HR_03_r = pd.DataFrame(
    {'Variante': 'Z_ZF_HR_03', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_HR_04**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_HR_04 = Z_ZF_HR_04.loc[
    (Z_ZF_HR_04['Tipo'] != 'SServ.')
    & (Z_ZF_HR_04['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta27 = len(Z_ZF_HR_04.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta27]
Z_ZF_HR_04_r = pd.DataFrame(
    {'Variante': 'Z_ZF_HR_04', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_HR_05**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_HR_05 = Z_ZF_HR_05.loc[
    (Z_ZF_HR_05['Tipo'] != 'SServ.')
    & (Z_ZF_HR_05['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta28 = len(Z_ZF_HR_05.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta28]
Z_ZF_HR_05_r = pd.DataFrame(
    {'Variante': 'Z_ZF_HR_05', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_HR_06**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_HR_06 = Z_ZF_HR_06.loc[
    (Z_ZF_HR_06['Tipo'] != 'SServ.')
    & (Z_ZF_HR_06['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta29 = len(Z_ZF_HR_06.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta29]
Z_ZF_HR_06_r = pd.DataFrame(
    {'Variante': 'Z_ZF_HR_06', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_EF_09**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_EF_09 = Z_ZF_EF_09.loc[
    (Z_ZF_EF_09['Tipo'] != 'SServ.')
    & (Z_ZF_EF_09['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta30 = len(Z_ZF_EF_09.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta30]
Z_ZF_EF_09_r = pd.DataFrame(
    {'Variante': 'Z_ZF_EF_09', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_EF_10**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_EF_10 = Z_ZF_EF_10.loc[
    (Z_ZF_EF_10['Tipo'] != 'SServ.')
    & (Z_ZF_EF_10['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta31 = len(Z_ZF_EF_10.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta31]
Z_ZF_EF_10_r = pd.DataFrame(
    {'Variante': 'Z_ZF_EF_10', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_EF_13**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_EF_13 = Z_ZF_EF_13.loc[
    (Z_ZF_EF_13['Tipo'] != 'SServ.')
    & (Z_ZF_EF_13['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta32 = len(Z_ZF_EF_13.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta32]
Z_ZF_EF_13_r = pd.DataFrame(
    {'Variante': 'Z_ZF_EF_13', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_EF_14**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_EF_14 = Z_ZF_EF_14.loc[
    (Z_ZF_EF_14['Tipo'] != 'SServ.')
    & (Z_ZF_EF_14['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta33 = len(Z_ZF_EF_14.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta33]
Z_ZF_EF_14_r = pd.DataFrame(
    {'Variante': 'Z_ZF_EF_14', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_EF_15**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_EF_15 = Z_ZF_EF_15.loc[
    (Z_ZF_EF_15['Tipo'] != 'SServ.')
    & (Z_ZF_EF_15['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta34 = len(Z_ZF_EF_15.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta34]
Z_ZF_EF_15_r = pd.DataFrame(
    {'Variante': 'Z_ZF_EF_15', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Z_ZF_EF_16**
# Filtro para dejar los que en Tipo son distintos a "SServ."
# y en Grupo son distintos a "Caducados"
Z_ZF_EF_16 = Z_ZF_EF_16.loc[
    (Z_ZF_EF_16['Tipo'] != 'SServ.')
    & (Z_ZF_EF_16['Estado_calculado'] != 'CADUCADOS')]

# Cuento los usuarios que quedan
cuenta35 = len(Z_ZF_EF_16.index)

# Creo nuevo DF con el nombre de la variante y cantidad de usuarios
cuenta = [cuenta35]
Z_ZF_EF_16_r = pd.DataFrame(
    {'Variante': 'Z_ZF_EF_16', 'Fecha': Fecha, 'Desviaciones': cuenta})

# **Unir todos los resultados de autorizaciones críticas**
autoriz_criticas = pd.concat(
    [
        Z_ZF_BC_12_r, Z_ZF_BC_13_r, Z_ZF_BC_14_r, Z_ZF_HR_01_r, Z_ZF_HR_02_r,
        Z_ZF_HR_03_r, Z_ZF_HR_04_r, Z_ZF_HR_05_r, Z_ZF_HR_06_r, Z_ZF_EF_09_r,
        Z_ZF_EF_10_r, Z_ZF_EF_13_r, Z_ZF_EF_14_r, Z_ZF_EF_15_r, Z_ZF_EF_16_r],
    axis=0).reset_index(drop=True)

# Agregar las cantidades de combinaciones y autorizaciones críticas
# por variante a la base histórica
# Limpiamos las tablas bases para eliminar priemra columna
base_cc = base_cc.drop(['Unnamed: 0'], axis=1)
base_ac = base_ac.drop(['Unnamed: 0'], axis=1)
detalle_des = detalle_des.drop(['Unnamed: 0'], axis=1)

# Creamos DataFrame con las columnas de datos de los indicadores
base_cc_info = base_cc[['Indicador', 'Variante', 'Texto']].drop_duplicates()
base_ac_info = base_ac[['Indicador', 'Variante', 'Texto']].drop_duplicates()

# Agregamos las combinaciones críticas a su base histórica
resultados_cc = pd.merge(
    base_cc_info, comb_criticas, on='Variante', how='inner')
base_cc = pd.concat([base_cc, resultados_cc], axis=0).reset_index(drop=True)

# Agregamos las autorizaciones críticas a su base histórica
resultados_ac = pd.merge(
    base_ac_info, autoriz_criticas, on='Variante', how='inner')
base_ac = pd.concat([base_ac, resultados_ac], axis=0).reset_index(drop=True)

# Agregamos detalle de las desviaciones encontradas
# Agregamos columnas con nombre del indicador, variante
# y fecha a cada dataframe con el detalle de las desviaciones
# Combinaciones críticas
ZBC_BC_BC_01[columns_dev_details] = 'IS11', 'ZBC_BC_BC_01', Fecha
ZBC_BC_BC_02[columns_dev_details] = 'IS12', 'ZBC_BC_BC_02', Fecha
ZBC_BC_BC_03[columns_dev_details] = 'IS13', 'ZBC_BC_BC_03', Fecha
ZBC_BC_BC_04[columns_dev_details] = 'IS14', 'ZBC_BC_BC_04', Fecha
ZBC_BC_BC_05[columns_dev_details] = 'IS15', 'ZBC_BC_BC_05', Fecha
ZBC_BC_BC_06[columns_dev_details] = 'IS16', 'ZBC_BC_BC_06', Fecha
ZBC_BC_BC_07[columns_dev_details] = 'IS17', 'ZBC_BC_BC_07', Fecha
ZBC_BC_BC_08[columns_dev_details] = 'IS18', 'ZBC_BC_BC_08', Fecha
ZBC_BC_BC_09[columns_dev_details] = 'IS19', 'ZBC_BC_BC_09', Fecha
ZFI_EF_MA_01[columns_dev_details] = 'IS02', 'ZFI_EF_MA_01', Fecha
ZFI_EF_PA_01[columns_dev_details] = 'IS04', 'ZFI_EF_PA_01', Fecha
ZFI_EF_PA_02[columns_dev_details] = 'IS07', 'ZFI_EF_PA_02', Fecha
ZFI_EF_PA_03[columns_dev_details] = 'IS08', 'ZFI_EF_PA_03', Fecha
ZFI_EF_PA_04[columns_dev_details] = 'IS09', 'ZFI_EF_PA_04', Fecha
ZFI_EF_PA_05[columns_dev_details] = 'IS22', 'ZFI_EF_PA_05', Fecha
ZFI_EF_RM_01[columns_dev_details] = 'IS06', 'ZFI_EF_RM_01', Fecha
ZFI_MA_PA_01[columns_dev_details] = 'IS03', 'ZFI_MA_PA_01', Fecha
ZMM_OC_OL_01[columns_dev_details] = 'IS01', 'ZMM_OC_OL_01', Fecha
ZMM_OC_RS_01[columns_dev_details] = 'IS05', 'ZMM_OC_RS_01', Fecha
ZMM_SL_OL_01[columns_dev_details] = 'IS10', 'ZMM_SL_OL_01', Fecha

# Autorizaciones críticas
Z_ZF_BC_12[columns_dev_details] = 'IS24', 'Z_ZF_BC_12', Fecha
Z_ZF_BC_13[columns_dev_details] = 'IS25', 'Z_ZF_BC_13', Fecha
Z_ZF_BC_14[columns_dev_details] = 'IS23', 'Z_ZF_BC_14', Fecha
Z_ZF_HR_01[columns_dev_details] = 'IS27', 'Z_ZF_HR_01', Fecha
Z_ZF_HR_02[columns_dev_details] = 'IS28', 'Z_ZF_HR_02', Fecha
Z_ZF_HR_03[columns_dev_details] = 'IS29', 'Z_ZF_HR_03', Fecha
Z_ZF_HR_04[columns_dev_details] = 'IS30', 'Z_ZF_HR_04', Fecha
Z_ZF_HR_05[columns_dev_details] = 'IS31', 'Z_ZF_HR_05', Fecha
Z_ZF_HR_06[columns_dev_details] = 'IS32', 'Z_ZF_HR_06', Fecha
Z_ZF_EF_09[columns_dev_details] = 'IS33', 'Z_ZF_EF_09', Fecha
Z_ZF_EF_10[columns_dev_details] = 'IS34', 'Z_ZF_EF_10', Fecha
Z_ZF_EF_13[columns_dev_details] = 'IS35', 'Z_ZF_EF_13', Fecha
Z_ZF_EF_14[columns_dev_details] = 'IS36', 'Z_ZF_EF_14', Fecha
Z_ZF_EF_15[columns_dev_details] = 'IS37', 'Z_ZF_EF_15', Fecha
Z_ZF_EF_16[columns_dev_details] = 'IS38', 'Z_ZF_EF_16', Fecha

# Juntamos todos los DataFrames y luego reordenamos las columnas
detalle = pd.concat(
    [
        ZBC_BC_BC_01, ZBC_BC_BC_02, ZBC_BC_BC_03, ZBC_BC_BC_04, ZBC_BC_BC_05,
        ZBC_BC_BC_06, ZBC_BC_BC_07, ZBC_BC_BC_08, ZBC_BC_BC_09, ZFI_EF_MA_01,
        ZFI_EF_PA_01, ZFI_EF_PA_02, ZFI_EF_PA_03, ZFI_EF_PA_04, ZFI_EF_PA_05,
        ZFI_EF_RM_01, ZFI_MA_PA_01, ZMM_OC_OL_01, ZMM_OC_RS_01, ZMM_SL_OL_01,
        Z_ZF_BC_12, Z_ZF_BC_13, Z_ZF_BC_14, Z_ZF_HR_01, Z_ZF_HR_02, Z_ZF_HR_03,
        Z_ZF_HR_04, Z_ZF_HR_05, Z_ZF_HR_06, Z_ZF_EF_09, Z_ZF_EF_10, Z_ZF_EF_13, 
        Z_ZF_EF_14, Z_ZF_EF_15, Z_ZF_EF_16],
    axis=0).reset_index(drop=True).fillna('')
detalle['Nombre_completo'] = detalle['Nom.largo'] + detalle['Nombrecompleto']
detalle = detalle[
    [
        'Indicador', 'Variante', 'Fecha', 'Usuario',
        'Nombre_completo', 'Estado_calculado', 'Tipo']]

# Agregamos desviaciones a la base histórica
detalle_des = pd.concat([detalle_des, detalle], axis=0).\
    reset_index(drop=True).sort_values(['Indicador', 'Fecha'])

# Guardar en Excel
nombre_archivo = history + 'Monitoreo IS.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')

base_cc.to_excel(writer, sheet_name='Combinaciones críticas')
base_ac.to_excel(writer, sheet_name='Autorizaciones críticas')
detalle_des.to_excel(writer, sheet_name='Detalle Desviaciones')

writer.save()
