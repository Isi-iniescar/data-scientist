# ## Importar librerías
import pandas as pd
import numpy as np
import glob

# ## Utilidades


def read_xlsx(path, header_int, sheet_name_str):
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


input_path = 'A:/Base de denuncias ACHS/Evaluaciones jefes/'
output_path = 'C:/projects/pamc/data/bases/current_data/'


# ## Cargar datos
nombres = read_xlsx(input_path,
                    header_int=0,
                    sheet_name_str='Menu')
d_jefatura = read_xlsx(input_path,
                       header_int=6,
                       sheet_name_str='Evaluación')
d_especialista = read_xlsx(input_path,
                           header_int=6,
                           sheet_name_str='Evaluación Especialista')
d_auditor = read_xlsx(input_path,
                      header_int=6,
                      sheet_name_str='Evaluación Auditores')

# ## Tratamiento de datos

# **Extraer nombre y cargo del auditor**

lista = list(nombres.values())

df_nom_cargo = pd.DataFrame()
columns = ['Nombre Auditor', 'Cargo']

for i in lista:
    aux = i[['Unnamed: 1', 'Unnamed: 2']].dropna()
    aux = aux.T.reset_index(drop=True)
    aux.columns = columns
    aux = aux.drop([0], axis='index').reset_index(drop=True)
    df_nom_cargo = pd.concat(
                             [df_nom_cargo,
                              aux],
                             axis='index').dropna().reset_index(drop=True)

df_nombre_aud = df_nom_cargo[['Nombre Auditor']]

lista_j = list(d_jefatura.values())
lista_e = list(d_especialista.values())
lista_a = list(d_auditor.values())

fecha_ev = []
fecha_emit = []
codigo = []
nombre = []
exp_1 = []
exp_2 = []
exp_3 = []
exp_4 = []
hab_1 = []
hab_2 = []
hab_3 = []
hab_4 = []
lider_1 = []
lider_2 = []
lider_3 = []
nota = []
f1 = []
f2 = []
f3 = []
m1 = []
m2 = []
m3 = []
aux = []


# **Extraer información de hoja jefatura**

for i in lista_j:
    # Auxiliar nombre auditor
    aux_auditor = i['Unnamed: 3'].iloc[4]
    aux.append(aux_auditor)

    # Extraer fecha evaluación
    aux_fecha = i[['Unnamed: 18']].iloc[[2]]
    aux_fecha.columns = ['Fecha Evaluacion']
    fecha_ev.append(aux_fecha)

    # Extraer fecha emision
    aux_fecha_emit = i[['Unnamed: 19']].iloc[[6]]
    aux_fecha_emit.columns = ['Fecha Emisión Informe']
    fecha_emit.append(aux_fecha_emit)

    # Extraer Código de auditoria
    aux_cod = i[['Unnamed: 11']].iloc[[2]]
    aux_cod.columns = ['N Informe']
    codigo.append(aux_cod)

    # Extraer nombre de auditoria
    aux_nombre = i[['Unnamed: 11']].iloc[[4]]
    aux_nombre.columns = ['Nombre del informe']
    nombre.append(aux_nombre)

    # Extraer nota supervisor de la expertiz técnica
    aux_exp_1 = i[['Unnamed: 17']].iloc[[23]]
    aux_exp_1.columns = ['Expertiz - Gestion AI']
    exp_1.append(aux_exp_1)

    aux_exp_2 = i[['Unnamed: 17']].iloc[[24]]
    aux_exp_2.columns = ['Expertiz - Entrega AI']
    exp_2.append(aux_exp_2)

    aux_exp_3 = i[['Unnamed: 17']].iloc[[25]]
    aux_exp_3.columns = ['Expertiz - Perspicacia del Negocio']
    exp_3.append(aux_exp_3)

    aux_exp_4 = i[['Unnamed: 17']].iloc[[26]]
    aux_exp_4.columns = ['Expertiz - Conclusión']
    exp_4.append(aux_exp_4)

    # Extraer nota supervisor de habilidades personales
    aux_hab_1 = i[['Unnamed: 17']].iloc[[28]]
    aux_hab_1.columns = ['Habilidades - Responsabilidad']
    hab_1.append(aux_hab_1)

    aux_hab_2 = i[['Unnamed: 17']].iloc[[29]]
    aux_hab_2.columns = ['Habilidades - Comunicación']
    hab_2.append(aux_hab_2)

    aux_hab_3 = i[['Unnamed: 17']].iloc[[30]]
    aux_hab_3.columns = ['Habilidades - Persuasión y colaboración']
    hab_3.append(aux_hab_3)

    aux_hab_4 = i[['Unnamed: 17']].iloc[[31]]
    aux_hab_4.columns = ['Habilidades - Pensamiento crítico']
    hab_4.append(aux_hab_4)

    # Extraer nota supervisor de liderazgo
    aux_lider_1 = i[['Unnamed: 17']].iloc[[33]]
    aux_lider_1.columns = ['Liderazgo - Gestión del cambio']
    lider_1.append(aux_lider_1)

    aux_lider_2 = i[['Unnamed: 17']].iloc[[34]]
    aux_lider_2.columns = ['Liderazgo - Gestión del desempeño']
    lider_2.append(aux_lider_2)

    aux_lider_3 = i[['Unnamed: 17']].iloc[[35]]
    aux_lider_3.columns = ['Liderazgo - Gestión de equipo']
    lider_3.append(aux_lider_3)

    # Extraer nota final
    aux_nota = i[['Unnamed: 23']].iloc[[38]]
    aux_nota.columns = ['Nota final']
    nota.append(aux_nota)

    # Extraer fortalezas
    aux_f1 = i[['Unnamed: 3']].iloc[[39]]
    aux_f1.columns = ['f1']
    f1.append(aux_f1)

    aux_f2 = i[['Unnamed: 3']].iloc[[41]]
    aux_f2.columns = ['f2']
    f2.append(aux_f2)

    aux_f3 = i[['Unnamed: 3']].iloc[[43]]
    aux_f3.columns = ['f3']
    f3.append(aux_f3)

    # Extraer mejoras
    aux_m1 = i[['Unnamed: 3']].iloc[[50]]
    aux_m1.columns = ['m1']
    m1.append(aux_m1)

    aux_m2 = i[['Unnamed: 3']].iloc[[52]]
    aux_m2.columns = ['m2']
    m2.append(aux_m2)

    aux_m3 = i[['Unnamed: 3']].iloc[[54]]
    aux_m3.columns = ['m3']
    m3.append(aux_m3)


# **Extraer información de hoja Auditor especialista**

for i in lista_e:
    # Auxiliar nombre auditor
    aux_auditor = i['Unnamed: 3'].iloc[4]
    aux.append(aux_auditor)

    # Extraer fecha emision
    aux_fecha_emit = i[['Unnamed: 19']].iloc[[6]]
    aux_fecha_emit.columns = ['Fecha Emisión Informe']
    fecha_emit.append(aux_fecha_emit)

    # Extraer Código de auditoria
    aux_cod = i[['Unnamed: 11']].iloc[[2]]
    aux_cod.columns = ['N Informe']
    codigo.append(aux_cod)

    # Extraer nombre de auditoria
    aux_nombre = i[['Unnamed: 11']].iloc[[4]]
    aux_nombre.columns = ['Nombre del informe']
    nombre.append(aux_nombre)

    # Extraer nota supervisor de la expertiz técnica
    aux_exp_1 = i[['Unnamed: 17']].iloc[[23]]
    aux_exp_1.columns = ['Expertiz - Gestion AI']
    exp_1.append(aux_exp_1)

    aux_exp_2 = i[['Unnamed: 17']].iloc[[24]]
    aux_exp_2.columns = ['Expertiz - Entrega AI']
    exp_2.append(aux_exp_2)

    aux_exp_3 = i[['Unnamed: 17']].iloc[[25]]
    aux_exp_3.columns = ['Expertiz - Perspicacia del Negocio']
    exp_3.append(aux_exp_3)

    aux_exp_4 = i[['Unnamed: 17']].iloc[[26]]
    aux_exp_4.columns = ['Expertiz - Conclusión']
    exp_4.append(aux_exp_4)

    # Extraer nota supervisor de habilidades personales
    aux_hab_1 = i[['Unnamed: 17']].iloc[[28]]
    aux_hab_1.columns = ['Habilidades - Responsabilidad']
    hab_1.append(aux_hab_1)

    aux_hab_2 = i[['Unnamed: 17']].iloc[[29]]
    aux_hab_2.columns = ['Habilidades - Comunicación']
    hab_2.append(aux_hab_2)

    aux_hab_3 = i[['Unnamed: 17']].iloc[[30]]
    aux_hab_3.columns = ['Habilidades - Persuasión y colaboración']
    hab_3.append(aux_hab_3)

    aux_hab_4 = i[['Unnamed: 17']].iloc[[31]]
    aux_hab_4.columns = ['Habilidades - Pensamiento crítico']
    hab_4.append(aux_hab_4)

    # Extraer nota supervisor de liderazgo
    aux_lider_1 = i[['Unnamed: 17']].iloc[[33]]
    aux_lider_1.columns = ['Liderazgo - Gestión del cambio']
    lider_1.append(aux_lider_1)

    aux_lider_2 = i[['Unnamed: 17']].iloc[[34]]
    aux_lider_2.columns = ['Liderazgo - Gestión del desempeño']
    lider_2.append(aux_lider_2)

    # Extraer nota final
    aux_nota = i[['Unnamed: 23']].iloc[[37]]
    aux_nota.columns = ['Nota final']
    nota.append(aux_nota)

    # Extraer fortalezas
    aux_f1 = i[['Unnamed: 3']].iloc[[38]]
    aux_f1.columns = ['f1']
    f1.append(aux_f1)

    aux_f2 = i[['Unnamed: 3']].iloc[[40]]
    aux_f2.columns = ['f2']
    f2.append(aux_f2)

    aux_f3 = i[['Unnamed: 3']].iloc[[42]]
    aux_f3.columns = ['f3']
    f3.append(aux_f3)

    # Extraer mejoras
    aux_m1 = i[['Unnamed: 3']].iloc[[49]]
    aux_m1.columns = ['m1']
    m1.append(aux_m1)

    aux_m2 = i[['Unnamed: 3']].iloc[[51]]
    aux_m2.columns = ['m2']
    m2.append(aux_m2)

    aux_m3 = i[['Unnamed: 3']].iloc[[53]]
    aux_m3.columns = ['m3']
    m3.append(aux_m3)


# **Extraer información de hoja Auditores**

lider = []

for i in lista_a:
    # Auxiliar nombre auditor
    aux_auditor = i['Unnamed: 3'].iloc[4]
    aux.append(aux_auditor)

    # Extraer fecha emision
    aux_fecha_emit = i[['Unnamed: 19']].iloc[[6]]
    aux_fecha_emit.columns = ['Fecha Emisión Informe']
    fecha_emit.append(aux_fecha_emit)

    # Extraer Código de auditoria
    aux_cod = i[['Unnamed: 11']].iloc[[2]]
    aux_cod.columns = ['N Informe']
    codigo.append(aux_cod)

    # Extraer nombre de auditoria
    aux_nombre = i[['Unnamed: 11']].iloc[[4]]
    aux_nombre.columns = ['Nombre del informe']
    nombre.append(aux_nombre)

    # Extraer nota supervisor de la expertiz técnica
    aux_exp_1 = i[['Unnamed: 17']].iloc[[26]]
    aux_exp_1.columns = ['Expertiz - Gestion AI']
    exp_1.append(aux_exp_1)

    aux_exp_2 = i[['Unnamed: 17']].iloc[[27]]
    aux_exp_2.columns = ['Expertiz - Entrega AI']
    exp_2.append(aux_exp_2)

    aux_exp_3 = i[['Unnamed: 17']].iloc[[28]]
    aux_exp_3.columns = ['Expertiz - Perspicacia del Negocio']
    exp_3.append(aux_exp_3)

    aux_exp_4 = i[['Unnamed: 17']].iloc[[29]]
    aux_exp_4.columns = ['Expertiz - Conclusión']
    exp_4.append(aux_exp_4)

    # Extraer nota supervisor de habilidades personales
    aux_hab_1 = i[['Unnamed: 17']].iloc[[31]]
    aux_hab_1.columns = ['Habilidades - Responsabilidad']
    hab_1.append(aux_hab_1)

    aux_hab_2 = i[['Unnamed: 17']].iloc[[32]]
    aux_hab_2.columns = ['Habilidades - Comunicación']
    hab_2.append(aux_hab_2)

    aux_hab_3 = i[['Unnamed: 17']].iloc[[33]]
    aux_hab_3.columns = ['Habilidades - Persuasión y colaboración']
    hab_3.append(aux_hab_3)

    aux_hab_4 = i[['Unnamed: 17']].iloc[[34]]
    aux_hab_4.columns = ['Habilidades - Pensamiento crítico']
    hab_4.append(aux_hab_4)

    # Crear lista para liderazgo
    lider.append('No aplica')

    # Extraer nota final
    aux_nota = i[['Unnamed: 23']].iloc[[37]]
    aux_nota.columns = ['Nota final']
    nota.append(aux_nota)

    # Extraer fortalezas
    aux_f1 = i[['Unnamed: 3']].iloc[[38]]
    aux_f1.columns = ['f1']
    f1.append(aux_f1)

    aux_f2 = i[['Unnamed: 3']].iloc[[40]]
    aux_f2.columns = ['f2']
    f2.append(aux_f2)

    aux_f3 = i[['Unnamed: 3']].iloc[[42]]
    aux_f3.columns = ['f3']
    f3.append(aux_f3)

    # Extraer mejoras
    aux_m1 = i[['Unnamed: 3']].iloc[[49]]
    aux_m1.columns = ['m1']
    m1.append(aux_m1)

    aux_m2 = i[['Unnamed: 3']].iloc[[51]]
    aux_m2.columns = ['m2']
    m2.append(aux_m2)

    aux_m3 = i[['Unnamed: 3']].iloc[[53]]
    aux_m3.columns = ['m3']
    m3.append(aux_m3)


# **Juntar las fechas de evaluaciones y eliminar los valores nulos**

df_fechas_ev = pd.DataFrame()
for i in fecha_ev:
    df_fechas_ev = pd.concat([df_fechas_ev,
                              i],
                             axis='index').dropna().reset_index(drop=True)


# **Juntar las fechas de emisión y eliminar los valores nulos**

df_fechas_em = pd.DataFrame()
for i in fecha_emit:
    df_fechas_em = pd.concat([df_fechas_em,
                              i],
                             axis='index').dropna().reset_index(drop=True)


# **Juntar todos los códigos y eliminar los valores nulos**

df_codigo = pd.DataFrame()
for i in codigo:
    df_codigo = pd.concat([df_codigo,
                           i],
                          axis='index').dropna().reset_index(drop=True)


# **Juntar todos los nombres de trabajos y eliminar los valores nulos**

df_nombres = pd.DataFrame()
for i in nombre:
    df_nombres = pd.concat([df_nombres,
                            i],
                           axis='index').dropna().reset_index(drop=True)


# **Juntar todas las notas de expertiz y eliminar los valores nulos**

df_exp_1 = pd.DataFrame()
for i in exp_1:
    df_exp_1 = pd.concat([df_exp_1,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_exp_2 = pd.DataFrame()
for i in exp_2:
    df_exp_2 = pd.concat([df_exp_2,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_exp_3 = pd.DataFrame()
for i in exp_3:
    df_exp_3 = pd.concat([df_exp_3,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_exp_4 = pd.DataFrame()
for i in exp_4:
    df_exp_4 = pd.concat([df_exp_4,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_exp = pd.concat([df_exp_1, df_exp_2, df_exp_3, df_exp_4], axis=1).\
    drop(df_exp_1[
        df_exp_1['Expertiz - Gestion AI'] == 'Evaluar'].index).\
    reset_index(drop=True).astype(float)
df_exp = np.round(df_exp, 2)


# **Juntar todas las notas de habilidades y eliminar los valores nulos**

df_hab_1 = pd.DataFrame()
for i in hab_1:
    df_hab_1 = pd.concat([df_hab_1,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_hab_2 = pd.DataFrame()
for i in hab_2:
    df_hab_2 = pd.concat([df_hab_2,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_hab_3 = pd.DataFrame()
for i in hab_3:
    df_hab_3 = pd.concat([df_hab_3,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_hab_4 = pd.DataFrame()
for i in hab_4:
    df_hab_4 = pd.concat([df_hab_4,
                          i],
                         axis='index').dropna().reset_index(drop=True)

df_hab = pd.concat([df_hab_1, df_hab_2, df_hab_3, df_hab_4], axis=1).\
    drop(df_hab_1[
        df_hab_1['Habilidades - Responsabilidad'] == 'Evaluar'].index).\
    reset_index(drop=True).astype(float)
df_hab = np.round(df_hab, 2)


# **Juntar todas las notas de liderazgo y eliminar los valores nulos**

aux_1 = pd.DataFrame()
aux_2 = pd.DataFrame()
aux_3 = pd.DataFrame()

for i in lider_1:
    aux_1 = pd.concat([aux_1, i], axis='index').reset_index(drop=True)

for i in lider_2:
    aux_2 = pd.concat([aux_2, i], axis='index').reset_index(drop=True)

for i in lider_3:
    aux_3 = pd.concat([aux_3, i], axis='index').reset_index(drop=True)

aux = aux[0:len(lider_1)]

df_aux1 = pd.DataFrame({'Nombre Auditor': aux})

df_aux2 = pd.concat([aux_1, aux_2, aux_3], axis='columns')

df_lider = pd.concat([df_aux1, df_aux2], axis='columns')
df_lider = df_lider.drop(
    df_lider[df_lider['Liderazgo - Gestión del cambio'] == 'Evaluar'].index).\
        dropna(thresh=2).reset_index(drop=True)


# **Juntar todas las notas finales y eliminar los valores nulos**

df_notas = pd.DataFrame()
for i in nota:
    df_notas = pd.concat([df_notas, i], axis='index').dropna()
    df_notas = df_notas.drop(df_notas[df_notas['Nota final'] == '0'].index).\
        reset_index(drop=True).astype(float)
df_notas = np.round(df_notas, 2)


# **Juntar todas fortalezas y eliminar los valores nulos**

df_f1 = pd.DataFrame()
for i in f1:
    df_f1 = pd.concat([df_f1, i], axis='index').\
        reset_index(drop=True).fillna('')

df_f2 = pd.DataFrame()
for i in f2:
    df_f2 = pd.concat([df_f2, i], axis='index').\
        reset_index(drop=True).fillna('')

df_f3 = pd.DataFrame()
for i in f3:
    df_f3 = pd.concat([df_f3, i], axis='index').\
        reset_index(drop=True).fillna('')


df_fort = pd.concat([df_f1, df_f2, df_f3], axis=1).reset_index(drop=True)
df_fort['Fortalezas'] = df_f1['f1'] + ' - ' + df_f2['f2'] + ' - ' + df_f3['f3']
df_fort = df_fort.drop(df_fort[df_fort['Fortalezas'] == ' -  - '].index).\
    reset_index(drop=True)
df_fort = df_fort[['Fortalezas']]


# **Juntar todas mejoras y eliminar los valores nulos**

df_m1 = pd.DataFrame()
for i in m1:
    df_m1 = pd.concat([df_m1, i], axis='index').reset_index(drop=True).\
        fillna('')

df_m2 = pd.DataFrame()
for i in m2:
    df_m2 = pd.concat([df_m2, i], axis='index').reset_index(drop=True).\
        fillna('')

df_m3 = pd.DataFrame()
for i in m3:
    df_m3 = pd.concat([df_m3, i], axis='index').reset_index(drop=True).\
        fillna('')

df_mejora = pd.concat([df_m1, df_m2, df_m3], axis=1).reset_index(drop=True)
df_mejora['Mejoras'] = df_m1['m1'] + ' - ' + df_m2['m2'] + ' - ' + df_m3['m3']
df_mejora = df_mejora.drop(df_mejora[df_mejora['Mejoras'] == ' -  - '].index).\
    reset_index(drop=True)
df_mejora = df_mejora[['Mejoras']]


# **Juntar todo**

df = pd.concat([df_codigo,
                df_nombres,
                df_fechas_em,
                df_nombre_aud,
                df_fechas_ev,
                df_exp,
                df_hab,
                df_notas,
                df_fort,
                df_mejora], axis=1)
df = pd.merge(df, df_lider, on='Nombre Auditor', how='left')


# **Hacer cálculo de días de evaluación**

df['Fecha Evaluacion'] = pd.to_datetime(df['Fecha Evaluacion'])
df['Fecha Emisión Informe'] = pd.to_datetime(df['Fecha Emisión Informe'])

df['Días de Evaluación'] = df.apply(lambda row: np.busday_count(
    row['Fecha Emisión Informe'].date(),
    row['Fecha Evaluacion'].date()), axis=1)


# **Hacer cálculo de cuatrimestre**

mes = []
for i in df['Fecha Emisión Informe']:
    aux = i.month
    mes.append(aux)

df['Mes'] = mes

cuatrimestre = []

for i in df['Mes']:
    if (i == 1) | (i == 2) | (i == 3) | (i == 4):
        cuatrimestre.append('1°')
    elif (i == 5) | (i == 6) | (i == 7) | (i == 8):
        cuatrimestre.append('2°')
    else:
        cuatrimestre.append('3°')

df['Cuatrimestre'] = cuatrimestre


# **Reordenar columnas**

df = df[['Cuatrimestre',
         'N Informe',
         'Nombre del informe',
         'Fecha Emisión Informe',
         'Nombre Auditor',
         'Fecha Evaluacion',
         'Días de Evaluación',
         'Expertiz - Gestion AI',
         'Expertiz - Entrega AI',
         'Expertiz - Perspicacia del Negocio',
         'Expertiz - Conclusión',
         'Habilidades - Responsabilidad',
         'Habilidades - Comunicación',
         'Habilidades - Persuasión y colaboración',
         'Habilidades - Pensamiento crítico',
         'Liderazgo - Gestión del cambio',
         'Liderazgo - Gestión del desempeño',
         'Liderazgo - Gestión de equipo',
         'Nota final',
         'Fortalezas',
         'Mejoras']].fillna('No aplica')


# ## Guardar en Excel

nombre_archivo = 'pamc3_ev_jefaturas' + '.xlsx'
writer = pd.ExcelWriter(output_path + nombre_archivo, engine='xlsxwriter')

df.to_excel(writer, sheet_name='Evaluacion_jefaturas')

writer.save()
