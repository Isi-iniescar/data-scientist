# Importar Librerías
import camelot
import pandas as pd
from datetime import datetime as dt
import glob
from datetime import date
import string
import unicodedata

# Utilidades

def rreplace(s, old, new, n_occurrence):
    # Replace the last n_occurrence (n_occurrence = 1 is the last occurrence) of an expression in a string s
    li = s.rsplit(old, n_occurrence)
    return new.join(li)

def read_pdf(input_path):
    # Lee los archivos pdf del path y crea diccionario con respectivos DF
    all_files = pd.Series(glob.glob(input_path + "/*.pdf"))
    d = {}
    for i in range(0, len(all_files)):
        file = all_files[i]
        index_from = file.rfind("\\") + 1
        if 'Diciembre' in file:
            d[str(file[index_from:-4])] = camelot.read_pdf(
                file,
                pages='5,6,7,8,9,10,11')
        else:
            d[str(file[index_from:-4])] = camelot.read_pdf(
                file,
                pages='2,3,4,5,6,7,8')
    return d


def num_format(df, col):
    df[col] = df[col].replace('', '0')\
        .str.replace('-', '0')\
        .str.replace('(', '-')\
        .str.replace(')', '')\
        .str.replace('.', '')\
        .str.replace(' ', '0')\
        .astype(float)
    return df


def cols_in_df(df):
    # Guarda columnas de dataframe como dataframes separados en diccionario
    cols = df.columns
    lista = []
    for i in range(0, len(cols)):
        lista.append(df[[cols[i]]])
    return lista


def split_row(df_col):
    for i in range(0, len(df_col)):
        df_col[i] = df_col[i][cabeceras[i][0]]\
            .str.split('\n', expand=True)\
            .stack().reset_index()\
            .rename(columns={0: cabeceras[i][0]})\
            .loc[:, df_col[i].columns]\
            .reset_index(drop=True)
    return df_col


def save_as_csv(df, output_path, tipo, nombre, sep=';'):
    df.to_csv(output_path + '\\' + dt.now().strftime('%Y%m%d') + '_' + tipo + nombre + '.csv', sep=sep)


def clean_assets(df):
    '''Limpiar encabezados'''
    encabezado = df.iloc[0].str.replace('\n', '')
    df = df.iloc[2:]
    df.columns = encabezado
    df = df.reset_index(drop=True)
    '''Separar activos corrientes y no corrientes'''
    global activos_corr
    global activos_no_corr
    index_ac = df[df['ACTIVOS'] == 'ACTIVOS NO CORRIENTES'].index.tolist()
    activos_corr = df.iloc[0:index_ac[0]]
    activos_corr = activos_corr.rename(
        columns={'ACTIVOS': 'ACTIVOS CORRIENTES'})
    activos_no_corr = df.iloc[index_ac[0]+1:]
    activos_no_corr = activos_no_corr.rename(
        columns={'ACTIVOS': 'ACTIVOS NO CORRIENTES'})
    activos_corr = activos_corr.drop(
        activos_corr[activos_corr['Nota N°'] == ''].index)\
        .reset_index(drop=True)
    activos_no_corr = activos_no_corr.drop(
        activos_no_corr[activos_no_corr['Nota N°'] == ''].index)\
        .reset_index(drop=True)
    '''Dejar numeros en formato'''
    global fecha1
    global fecha2
    fecha1 = encabezado[3]
    fecha2 = encabezado[4]
    activos_corr = num_format(activos_corr, fecha1)
    activos_corr = num_format(activos_corr, fecha2)
    activos_no_corr = num_format(activos_no_corr, fecha1)
    activos_no_corr = num_format(activos_no_corr, fecha2)
    '''Guardar DF como CSV'''
    tipo = 'ac'
    save_as_csv(activos_corr, output_path, tipo, nombre)
    tipo = 'anc'
    save_as_csv(activos_no_corr, output_path, tipo, nombre)
    return df


def clean_liability(df):
    '''Limpiar encabezados'''
    encabezado = df.iloc[0].str.replace('\n', '')
    df = df.iloc[2:]
    df.columns = encabezado
    df = df.reset_index(drop=True)
    '''Separar pasivos corrientes, no corrientes y patrimonio'''
    global pasivos_corr
    global pasivos_no_corr
    global patrimonio
    index_pc = df[df['PASIVOS Y PATRIMONIO NETO'] == 'PASIVOS NO CORRIENTES']\
        .index.tolist()
    index_pat = df[df['PASIVOS Y PATRIMONIO NETO'] == 'PATRIMONIO NETO']\
        .index.tolist()
    pasivos_corr = df.iloc[0:index_pc[0]]
    pasivos_corr = pasivos_corr.rename(
        columns={'PASIVOS Y PATRIMONIO NETO': 'PASIVOS CORRIENTES'})
    pasivos_corr = pasivos_corr.drop(
        pasivos_corr[pasivos_corr['Nota N°'] == ''].index)\
        .reset_index(drop=True)
    pasivos_no_corr = df.iloc[index_pc[0]+1:index_pat[0]]
    pasivos_no_corr = pasivos_no_corr.rename(
        columns={'PASIVOS Y PATRIMONIO NETO': 'PASIVOS NO CORRIENTES'})
    pasivos_no_corr = pasivos_no_corr.drop(
        pasivos_no_corr[pasivos_no_corr['Nota N°'] == ''].index)\
        .reset_index(drop=True)
    patrimonio = df.iloc[index_pat[0]+1:]
    patrimonio = patrimonio.rename(
        columns={'PASIVOS Y PATRIMONIO NETO': 'PATRIMONIO NETO'})
    patrimonio = patrimonio.drop(
        patrimonio[
            patrimonio['PATRIMONIO NETO'] == 'SUBTOTAL PATRIMONIO'].index)
    patrimonio = patrimonio.drop(
        patrimonio[
            patrimonio['PATRIMONIO NETO'] == 'TOTAL PATRIMONIO NETO'].index)
    patrimonio = patrimonio.drop(
        patrimonio[
            patrimonio['PATRIMONIO NETO'] == 'TOTAL PASIVOS Y PATRIMONIO NETO'].index)
    '''Dejar numeros en formato'''
    global fecha1
    global fecha2
    fecha1 = encabezado[3]
    fecha2 = encabezado[4]
    pasivos_corr = num_format(pasivos_corr, fecha1)
    pasivos_corr = num_format(pasivos_corr, fecha2)
    pasivos_no_corr = num_format(pasivos_no_corr, fecha1)
    pasivos_no_corr = num_format(pasivos_no_corr, fecha2)
    patrimonio = num_format(patrimonio, fecha1)
    patrimonio = num_format(patrimonio, fecha2)
    '''Guardar DF como CSV'''
    tipo = 'pc'
    save_as_csv(pasivos_corr, output_path, tipo, nombre)
    tipo = 'pnc'
    save_as_csv(pasivos_no_corr, output_path, tipo, nombre)
    tipo = 'pat'
    save_as_csv(patrimonio, output_path, tipo, nombre)
    return df


def clean_eerr_ori_efe(df):
    global eerr    
    '''Limpiar encabezados'''
    encabezado = df.iloc[0].str.replace('\n','')
    df = df.iloc[1:]
    df.columns = encabezado
    df = df.reset_index(drop=True)    
    '''Dejar numeros en formato'''
    global fecha_1
    global fecha_2    
    fecha_1 = encabezado[3]
    fecha_2 = encabezado[4]    
    df = num_format(df, fecha_1)
    df = num_format(df, fecha_2)    
    eerr = df    
    '''Guardar DF como CSV'''
    save_as_csv(df, output_path, tipo , nombre)    
    return df


def clean_ecpn(df):
    '''Limpiar encabezados'''
    encabezado = df.iloc[1].str.replace('\n', '').tolist()
    encabezado[0] = encabezado[0].replace(
        '',
        'Concepto')
    encabezado[-1] = encabezado[-1].replace(
        '',
        'Total')
    encabezado[-2] = encabezado[-2].replace(
        '',
        'Participaciones no controladoras')
    encabezado[-3] = encabezado[-3].replace(
        '',
        'Otros resultados integrales')
    df = df.iloc[2:]
    df.columns = encabezado
    df = df.reset_index(drop=True)
    '''Separar registros en filas'''
    # Separar cada columna en DF
    df_col = cols_in_df(df)
    df_col.pop()
    # Aplicar función para separar filas
    global cabeceras
    cabeceras = []
    for i in range(0, len(df_col)):
        cabeceras.append(list(df_col[i]))
    aux = split_row(df_col)
    # Eliminar registro que no sirve
    aux[0] = aux[0].drop(
        aux[0][aux[0][cabeceras[0][0]] == 'Otras variaciones patrimoniales '].index)\
        .reset_index(drop=True)
    '''Dejar numeros en formato'''
    for i in range(1, len(aux)):
        aux[i] = num_format(aux[i], cabeceras[i][0])
    '''Reconstituir el DF'''
    df = pd.concat(aux, axis=1)
    df['Total'] = df.sum(axis=1)
    '''Guardar DF como CSV'''
    save_as_csv(df, output_path, tipo, nombre)
    return df

def read_file(path, header_int, file_extension, sep = '|', encoding = 'latin1'):
# Lee los archivos del path (correspondientes a la fecha de descarga), y crea diccionario con respectivos dataframes
    all_files = pd.Series(glob.glob(path + "/*." + file_extension))
    fecha_str = date.today().strftime("%Y%m")
    files = all_files[all_files.astype(str).str.contains(fecha_str)].tolist()
    d = {}
    
    for i in range(0,len(files)):
        file_path = files[i]
        index_from = file_path.rfind("\\") + 1
        file_name = str_format(file_path[index_from:-4])
        
        if file_name[:8].isdigit():
            file_name = file_name[9:]
        
        d[str(file_name)] = pd.read_csv(file_path,header=header_int, sep=sep, encoding=encoding, dtype=str)
    return d

def str_format(stringg):
    
    #Elimina simbolos de puntuación, u otros simbolos de un string
    
    punctuation = list(string.punctuation)          
    symbols = ['^','°']                     
    new_string = unicodedata.normalize('NFKD', stringg).encode('ascii', errors='ignore').decode('utf-8') # Elimina tildes 

    for i in range(len(punctuation)):
        new_string = new_string.replace(punctuation[i],'_')   # Elimina simbolos de puntuación
    
    for i in range(len(symbols)):
        new_string = new_string.replace(symbols[i],'')       # Elimina otros simbolos
    
    new_string = new_string.replace(' ','_').lower()         # Elimina espacios 
    
    while new_string[-1] == '_':
        new_string = rreplace(new_string,'_','',1)           # Elimina los "espacios" (_) al final del string
    
    return new_string

def save_as_csv_d(dict, path, sep=';', encoding = 'latin1'):
    
    for keys in dict:
        dict[keys].to_csv(path + '\\' + dt.now().strftime('%Y%m%d') + str(keys) + '.csv', sep=sep)

def schema_cols(dict, col_names):
    for keys in dict:
        for col in col_names:
            if col not in dict[keys].columns:
                dict[keys][col] = '0'
        dict[keys] = dict[keys][col_names]
    return dict

# Cargar PDF
input_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/'
pdfs = read_pdf(input_path)
global keys
keys = list(pdfs.keys())


# ESF - Activos
i = 0
for pdf in pdfs.keys():
    global output_path
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/activos/'
    global nombre
    nombre = keys[i]
    pdfs[pdf][0].df = clean_assets(pdfs[pdf][0].df)
    i = i + 1
    # Agregar valores del periodo a la base histórica del IG18_19 de Auditoría Continua
    if 'Informeindividual' in pdf:   
        act_c = activos_corr.drop(['ACTIVOS CORRIENTES', 'Nota N°', fecha2], axis=1)
        act_c['Código'] = act_c['Código'].astype(int)
        act_nc = activos_no_corr.drop(['ACTIVOS NO CORRIENTES', 'Nota N°', fecha2], axis=1)
        act_nc['Código'] = act_nc['Código'].astype(int)

        base_ac = pd.merge(base_ac, act_c, how='left', on='Código')
        base_anc = pd.merge(base_anc, act_nc, how='left', on='Código')

# ESF - Pasivos
i = 0
for pdf in pdfs.keys():
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/pasivos/'
    nombre = keys[i]
    pdfs[pdf][1].df = clean_liability(pdfs[pdf][1].df)
    i = i + 1
    if 'Informeindividual' in pdf:
    # Agregar valores del periodo a la base histórica del IG18_19 de Auditoría Continua
        pas_c = pasivos_corr.drop(['PASIVOS CORRIENTES', 'Nota N°', fecha2], axis=1)
        pas_c['Código'] = pas_c['Código'].astype(int)
        pas_nc = pasivos_no_corr.drop(['PASIVOS NO CORRIENTES', 'Nota N°', fecha2], axis=1)
        pas_nc['Código'] = pas_nc['Código'].astype(int)
        pat = patrimonio.drop(['PATRIMONIO NETO', 'Nota N°', fecha2], axis=1)
        pat['Código'] = pat['Código'].astype(int)

        base_pc = pd.merge(base_pc, pas_c, how='left', on='Código')
        base_pnc = pd.merge(base_pnc, pas_nc, how='left', on='Código')
        base_pat = pd.merge(base_pat, pat, how='left', on='Código')


# Estado de Resultados (EERR)
i = 0
for pdf in pdfs.keys():
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/eerr/'
    global tipo
    tipo = 'eerr'
    nombre = keys[i]
    pdfs[pdf][2].df = clean_eerr_ori_efe(pdfs[pdf][2].df)
    # Agregar valores del periodo a la base histórica del IG18_19 de Auditoría Continua
    if 'Informeindividual' in pdf: 
        eerr.columns = ['Código', 'Cuenta', 'Nota N°', fecha_1, fecha_2]
        eerr = eerr.drop(['Cuenta', 'Nota N°', fecha_2], axis=1)
        eerr['Código'] = eerr['Código'].astype(int)

        base_eerr = pd.merge(base_eerr, eerr, how='left', on='Código')
    i = i + 1

# Otros Resultados Integrales (ORI)
i = 0
for pdf in pdfs.keys():
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/ori/'
    tipo = 'ori'
    nombre = keys[i]
    pdfs[pdf][3].df = clean_eerr_ori_efe(pdfs[pdf][3].df)
    i = i + 1

# ECPN periodo anterior
i = 0
for pdf in pdfs.keys():
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/ecpn/'
    tipo = 'per_anterior'
    nombre = keys[i]
    pdfs[pdf][4].df = clean_ecpn(pdfs[pdf][4].df)
    i = i + 1

# ECPN periodo actual
i = 0
for pdf in pdfs.keys():
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/ecpn/'
    tipo = 'per_actual'
    nombre = keys[i]
    pdfs[pdf][5].df = clean_ecpn(pdfs[pdf][5].df)
    i = i + 1

# Estado de Flujo de Efectivo (EFE)
i = 0
for pdf in pdfs.keys():
    output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/efe/'
    tipo = 'efe'
    nombre = keys[i]
    pdfs[pdf][6].df = clean_eerr_ori_efe(pdfs[pdf][6].df)
    i = i + 1

#Guardar base histórica del IG18_19
input_ig18_19 = 'C:/projects/auditoria_continua/data/output/'

writer = pd.ExcelWriter(input_ig18_19 + 'Base_Historica.xlsx', engine='xlsxwriter')
base_ac.to_excel(writer, sheet_name='Activos_corrientes')
base_anc.to_excel(writer, sheet_name='Activos_no_corrientes')
base_pc.to_excel(writer, sheet_name='Pasivos_corrientes')
base_pnc.to_excel(writer, sheet_name='Pasivos_no_corrientes')
base_pat.to_excel(writer, sheet_name='Patrimonio')
base_eerr.to_excel(writer, sheet_name='EERR')
writer.save()

# Cargar archivos CSV en diccionario
## ACTIVOS

dict_activos = read_file(r'C:/projects/data_lake/data/input/current/eeff_emitidos/activos/',0,file_extension='csv', sep=';')

### Eliminar primera columna en df

for keys in dict_activos:
    dict_activos[keys] = dict_activos[keys].drop(columns = dict_activos[keys].columns[0])

### Cambiar nombre de columnas y reordenar

for keys in dict_activos:
    
    if keys[:3]=='anc':
        dict_activos[keys]['TIPO'] = 'Activos no corrientes'
        dict_activos[keys].rename(columns = {'ACTIVOS NO CORRIENTES':'CUENTA'}, inplace = True)
    else:
        dict_activos[keys]['TIPO'] = 'Activos corrientes'
        dict_activos[keys].rename(columns = {'ACTIVOS CORRIENTES':'CUENTA'}, inplace = True)
        
    cols = dict_activos[keys].columns.tolist()
    dict_activos[keys] = dict_activos[keys][[cols[0], cols[5], cols[1], cols[2], cols[3], cols[4]]]

### Cambiar nombre de columnas de fechas

lista = list(dict_activos.keys())

for keys in lista:
    year = dict_activos[keys].columns[-2][6:]
    month = dict_activos[keys].columns[-2][3:5]
    day = dict_activos[keys].columns[-2][:2]
    dict_activos[str(keys) + '_' + str(year) + str(month) + str(day)] = dict_activos.pop(keys)

### Cambiar nombre de últimas columnas

for keys in dict_activos:
    cols = dict_activos[keys].columns
    dict_activos[keys] = dict_activos[keys].rename(columns={cols[-2]:'monto_fecha1', cols[-1]:'monto_fecha1_anterior'})

### Guardar en archivos CSV

save_as_csv_d(dict_activos, r'C:/projects/data_lake/data/output/current/eeff_emitidos/activos/')

## PASIVOS

dict_pasivos = read_file(r'C:/projects/data_lake/data/input/current/eeff_emitidos/pasivos/',0,file_extension='csv', sep=';')

### Eliminar primera columna en df

for keys in dict_pasivos:
    dict_pasivos[keys] = dict_pasivos[keys].drop(columns = dict_pasivos[keys].columns[0])

### Cambiar nombre de columnas y reordenar

for keys in dict_pasivos:
    
    if keys[:3]=='pnc':
        dict_pasivos[keys]['TIPO'] = 'Pasivos no corrientes'
        dict_pasivos[keys].rename(columns = {'PASIVOS NO CORRIENTES':'CUENTA'}, inplace = True)
    elif keys[:3]=='pat':
        dict_pasivos[keys]['TIPO'] = 'Patrimonio neto'
        dict_pasivos[keys].rename(columns = {'PATRIMONIO NETO':'CUENTA'}, inplace = True)
    else:
        dict_pasivos[keys]['TIPO'] = 'Pasivos corrientes'
        dict_pasivos[keys].rename(columns = {'PASIVOS CORRIENTES':'CUENTA'}, inplace = True)
        
    cols = dict_pasivos[keys].columns.tolist()
    dict_pasivos[keys] = dict_pasivos[keys][[cols[0], cols[5], cols[1], cols[2], cols[3], cols[4]]]

### Cambiar nombre de columnas de fechas

lista_pasivos = list(dict_pasivos.keys())

for keys in lista_pasivos:
    year = dict_pasivos[keys].columns[-2][6:]
    month = dict_pasivos[keys].columns[-2][3:5]
    day = dict_pasivos[keys].columns[-2][:2]
    dict_pasivos[str(keys) + '_' + str(year) + str(month) + str(day)] = dict_pasivos.pop(keys)

### Cambiar nombre de últimas columnas

for keys in dict_pasivos:
    cols = dict_pasivos[keys].columns
    dict_pasivos[keys] = dict_pasivos[keys].rename(columns={cols[-2]:'monto_fecha1', cols[-1]:'monto_fecha1_anterior'})

### Guardar en archivos CSV

save_as_csv_d(dict_activos, r'C:/projects/data_lake/data/output/current/eeff_emitidos/activos/')

## ECPN

dict_ecpn = read_file(r'C:/projects/data_lake/data/input/current/eeff_emitidos/ecpn/',0,file_extension='csv', sep=';')

### Eliminar primera columna en df

for keys in dict_ecpn:
    dict_ecpn[keys] = dict_ecpn[keys].drop(columns = dict_ecpn[keys].columns[0])

### Definir esquema

cols = ['Concepto', 'Fondo de reservas de eventualidades',
       'Fondo de contingencia', 'Fondo de reservas de pensiones adicional',
       'Otras reservas', 'Ajuste de inversiones a valor razonable',
       'Ajuste acumulado por diferencias de conversiÃ³n',
       'Excedente (dÃ©ficit) de ejercicios anteriores',
       'Excedente (dÃ©ficit) del ejercicio',
       'Resultados en valuaciÃ³n de propiedades',
       'Resultados en cobertura de flujos de caja',
       'Otros resultados integrales', 'Participaciones no controladoras',
       'Total']

dict_ecpn = schema_cols(dict_ecpn, cols)

### Guardar en archivos CSV

save_as_csv_d(dict_ecpn, r'C:/projects/data_lake/data/output/current/eeff_emitidos/ecpn/')

## EERR

dict_eerr = read_file(r'C:/projects/data_lake/data/input/current/eeff_emitidos/eerr/',0,file_extension='csv', sep=';')

### Eliminar primera columna en df

for keys in dict_eerr:
    dict_eerr[keys] = dict_eerr[keys].drop(columns = dict_eerr[keys].columns[0])

### Cambiar nombres de df

lista_eerr = list(dict_eerr.keys())

for keys in lista_eerr:
    date1_str = dict_eerr[keys].columns[-2][:10]
    date2_str = dict_eerr[keys].columns[-2][13:-2]
    date1 = dt.strptime(date1_str, '%d/%m/%Y')
    date2 = dt.strptime(date2_str, '%d/%m/%Y')
    month_delta = round(((date2 - date1).days)/30)
    dict_eerr[str(keys) + '_rango' + str(month_delta) + 'meses'] = dict_eerr.pop(keys)

### Cambiar nombre de últimas columnas

for keys in dict_eerr:
    cols = dict_eerr[keys].columns
    dict_eerr[keys] = dict_eerr[keys].rename(columns={cols[-2]:'monto_rango', cols[-1]:'monto_rango_anterior'})

### Guardar en archivos CSV

save_as_csv_d(dict_eerr, r'C:/projects/data_lake/data/output/current/eeff_emitidos/eerr/')

## EFE

dict_efe = read_file(r'C:/projects/data_lake/data/input/current/eeff_emitidos/efe/',0,file_extension='csv', sep=';')

### Eliminar primera columna en df

for keys in dict_efe:
    dict_efe[keys] = dict_efe[keys].drop(columns = dict_efe[keys].columns[0])

### Cambiar nombres de df

lista_efe = list(dict_efe.keys())

for keys in lista_efe:
    date1_str = dict_efe[keys].columns[-2][:10]
    date2_str = dict_efe[keys].columns[-2][13:-2]
    date1 = dt.strptime(date1_str, '%d/%m/%Y')
    date2 = dt.strptime(date2_str, '%d/%m/%Y')
    month_delta = round(((date2 - date1).days)/30)
    dict_efe[str(keys) + '_rango' + str(month_delta) + 'meses'] = dict_efe.pop(keys)

### Cambiar nombre de últimas columnas

for keys in dict_efe:
    cols = dict_efe[keys].columns
    dict_efe[keys] = dict_efe[keys].rename(columns={cols[-2]:'monto_rango', cols[-1]:'monto_rango_anterior'})

### Guardar en archivos CSV

save_as_csv_d(dict_efe, r'C:/projects/data_lake/data/output/current/eeff_emitidos/efe/')

## ORI

dict_ori = read_file(r'C:/projects/data_lake/data/input/current/eeff_emitidos/ori/',0,file_extension='csv', sep=';')

### Eliminar primera columna en df

for keys in dict_ori:
    dict_ori[keys] = dict_ori[keys].drop(columns = dict_ori[keys].columns[0])

### Cambiar nombres de df

lista_ori = list(dict_ori.keys())

for keys in lista_ori:
    date1_str = dict_ori[keys].columns[-2][:10]
    date2_str = dict_ori[keys].columns[-2][13:-2]
    date1 = dt.strptime(date1_str, '%d/%m/%Y')
    date2 = dt.strptime(date2_str, '%d/%m/%Y')
    month_delta = round(((date2 - date1).days)/30)
    dict_ori[str(keys) + '_rango' + str(month_delta) + 'meses'] = dict_ori.pop(keys)

### Cambiar nombre de últimas columnas

for keys in dict_ori:
    cols = dict_ori[keys].columns
    dict_ori[keys] = dict_ori[keys].rename(columns={cols[-2]:'monto_rango', cols[-1]:'monto_rango_anterior'})

### Guardar en archivos CSV

save_as_csv_d(dict_ori, r'C:/projects/data_lake/data/output/current/eeff_emitidos/ori/')

