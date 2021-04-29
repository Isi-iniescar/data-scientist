# ## Importar librerías
import pandas as pd
import numpy as np
import glob

# ## Utilidades


def read_xlsx(path, header_int, sheet_name_str):
    # Lee los archivos xlsx del path y crea diccionario con dataframes
    all_files = pd.Series(glob.glob(path + "/*.xlsx"))
    d = {}

    for i in range(0, len(all_files)):
        file_path = all_files[i]
        index_from = file_path.rfind("\\") + 1
        file_name = file_path[index_from:-4]

        d[str(file_name)] = pd.read_excel(file_path,
                                          header=header_int,
                                          sheet_name=sheet_name_str,
                                          dtype=str)
    return d


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


input_path = 'A:/Base de denuncias ACHS/Encuesta de satisfacción/'
output_path = 'C:/projects/pamc/data/bases/current_data/'


# ## Cargar datos
d_encuestas = read_xlsx(input_path, header_int=0, sheet_name_str='Sheet0')
encuestas = list(d_encuestas.values())
keys = list(d_encuestas.keys())


# ## Limpieza
auditorias = []
codigo = []

for i in keys:
    aux = i[13:].replace('.', '').replace('_', '')
    auditorias.append(aux)
    cod = i[0:3]
    codigo.append(cod)

encuestas_clean = []

for df in encuestas:
    aux = df.iloc[1:].fillna('0')
    aux = aux[aux[aux.columns[0]] != '0']
    aux = aux[aux.columns[-9:]]
    aux[aux.columns[:-1]] = clean_blankspace(aux[aux.columns[:-1]])
    aux.columns = ['Participación',
                   'Claridad Información',
                   'Conocimiento Técnico',
                   'Actitud Profesional',
                   'Comunicación',
                   'Claridad de resultados',
                   'Objetividad e independencia',
                   'Satisfacción General',
                   'Comentarios']
    aux = aux.drop(aux[
        (aux['Participación'] == '0') &
        (aux['Claridad Información'] == '0') &
        (aux['Conocimiento Técnico'] == '0') &
        (aux['Actitud Profesional'] == '0') &
        (aux['Comunicación'] == '0') &
        (aux['Claridad de resultados'] == '0') &
        (aux['Objetividad e independencia'] == '0') &
        (aux['Satisfacción General'] == '0')].index)

    for col in aux.columns[:-1]:
        for i in aux[col]:
            if i.isdigit():
                pass
            else:
                a = list(aux[col]).index(i)
                aux[col].iloc[a] = '0'

    squema = {'Participación': str,
              'Claridad Información': int,
              'Conocimiento Técnico': int,
              'Actitud Profesional': int,
              'Comunicación': int,
              'Claridad de resultados': int,
              'Objetividad e independencia': int,
              'Satisfacción General': int,
              'Comentarios': str}

    aux = aux.astype(squema)

    encuestas_clean.append(aux)


# ## Ejecución indicador
n_respuestas = []
claridad_info = []
conocimiento = []
actitud = []
comunicacion = []
claridad_r = []
obj = []
satisfaccion = []
comentarios = []

for i in encuestas_clean:
    # Claridad de la información
    total = list(i['Claridad Información'])

    while 0 in total:
        total.remove(0)
    largo = len(total)

    aux_sat = list(i['Claridad Información']).count(7) + \
        list(i['Claridad Información']).count(6)
    aux_insat = list(i['Claridad Información']).count(1) + \
        list(i['Claridad Información']).count(2) + \
        list(i['Claridad Información']).count(3) + \
        list(i['Claridad Información']).count(4)
    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat
    except:
        sat_neta = 0

    claridad_info.append(sat_neta)

for i in encuestas_clean:
    # Conocimiento técnico
    total = list(i['Conocimiento Técnico'])

    while 0 in total:
        total.remove(0)

    largo = len(total)

    aux_sat = list(i['Conocimiento Técnico']).count(7) + \
        list(i['Conocimiento Técnico']).count(6)
    aux_insat = list(i['Conocimiento Técnico']).count(1) + \
        list(i['Conocimiento Técnico']).count(2) + \
        list(i['Conocimiento Técnico']).count(3) + \
        list(i['Conocimiento Técnico']).count(4)

    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat

    except:
        sat_neta = 0

    conocimiento.append(sat_neta)

    # Actitud profesional
    total = list(i['Actitud Profesional'])

    while 0 in total:
        total.remove(0)
    largo = len(total)

    aux_sat = list(i['Actitud Profesional']).count(7) + \
        list(i['Actitud Profesional']).count(6)
    aux_insat = list(i['Actitud Profesional']).count(1) + \
        list(i['Actitud Profesional']).count(2) + \
        list(i['Actitud Profesional']).count(3) + \
        list(i['Actitud Profesional']).count(4)
    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat
    except:
        sat_neta = 0

    actitud.append(sat_neta)

    # Comunicación
    total = list(i['Comunicación'])

    while 0 in total:
        total.remove(0)
    largo = len(total)

    aux_sat = list(i['Comunicación']).count(7) + \
        list(i['Comunicación']).count(6)
    aux_insat = list(i['Comunicación']).count(1) + \
        list(i['Comunicación']).count(2) + \
        list(i['Comunicación']).count(3) + \
        list(i['Comunicación']).count(4)
    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat
    except:
        sat_neta = 0

    comunicacion.append(sat_neta)

    # Claridad de resultados
    total = list(i['Claridad de resultados'])

    while 0 in total:
        total.remove(0)
    largo = len(total)

    aux_sat = list(i['Claridad de resultados']).count(7) + \
        list(i['Claridad de resultados']).count(6)
    aux_insat = list(i['Claridad de resultados']).count(1) + \
        list(i['Claridad de resultados']).count(2) + \
        list(i['Claridad de resultados']).count(3) + \
        list(i['Claridad de resultados']).count(4)
    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat
    except:
        sat_neta = 0

    claridad_r.append(sat_neta)

    # Objetividad e independencia
    total = list(i['Objetividad e independencia'])

    while 0 in total:
        total.remove(0)
    largo = len(total)

    aux_sat = list(i['Objetividad e independencia']).count(7) + \
        list(i['Objetividad e independencia']).count(6)
    aux_insat = list(i['Objetividad e independencia']).count(1) + \
        list(i['Objetividad e independencia']).count(2) + \
        list(i['Objetividad e independencia']).count(3) + \
        list(i['Objetividad e independencia']).count(4)
    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat
    except:
        sat_neta = 0

    obj.append(sat_neta)

    # Satisfacción General
    total = list(i['Satisfacción General'])

    while 0 in total:
        total.remove(0)
    largo = len(total)

    aux_sat = list(i['Satisfacción General']).count(7) + \
        list(i['Satisfacción General']).count(6)
    aux_insat = list(i['Satisfacción General']).count(1) + \
        list(i['Satisfacción General']).count(2) + \
        list(i['Satisfacción General']).count(3) + \
        list(i['Satisfacción General']).count(4)
    try:
        porc_sat = aux_sat/largo
        porc_insat = aux_insat/largo
        sat_neta = porc_sat - porc_insat
    except:
        sat_neta = 0

    satisfaccion.append(sat_neta)

    # Comentarios
    aux = ' - '.join(list(i['Comentarios']))
    comentarios.append(aux)

    # Cantidad de respuestas
    n_respuestas.append(len(list(i['Satisfacción General'])))

df = pd.DataFrame({'Código encuesta': codigo,
                   'Nombre Auditoría': auditorias,
                   'Satisfacción General': satisfaccion,
                   'Claridad Información': claridad_info,
                   'Conocimiento Técnico': conocimiento,
                   'Actitud Profesional': actitud,
                   'Comunicación': comunicacion,
                   'Claridad de resultados': claridad_r,
                   'Objetividad e independencia': obj,
                   'Número de respuestas': n_respuestas,
                   'Comentarios': comentarios})


# ## Guardar en excel
nombre_archivo = 'pamc4_encuesta.xlsx'
writer = pd.ExcelWriter(output_path + nombre_archivo, engine='xlsxwriter')

df.to_excel(writer, sheet_name='Encuesta_clientes')

writer.save()
