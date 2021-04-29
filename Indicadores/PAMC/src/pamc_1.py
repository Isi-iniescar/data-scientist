# Importar librerías
import pandas as pd

# Utilidades


def compliance(x, position):
    status_list = x.values.tolist()
    if position == 'auditor':
        aux_list = [i for i in status_list if i == 'C']
        percentage = len(aux_list)/len(status_list)
    if position == 'boss':
        aux_list = [i for i in status_list if i == 'R']
        percentage = len(aux_list)/len(status_list)
    if position == 'subg':
        counter = status_list.count('A')
        percentage = counter/len(status_list)
    return percentage


def status(x, position):

    for i in x:
        if position == 'auditor':
            if (i == 'Aprobada') or (i == 'Completada'):
                result = 'C'
            else:
                result = 'P'
        if position == 'boss':

            if (i == 'Aprobada'):
                result = 'R'
            else:
                result = 'P'
        if position == 'subg':
            result = status_subg(x)
    return result


def status_subg(x):
    values_list = x.unique()
    if (len(values_list) == 1) and (values_list[0] == 'Aprobada'):
        result = 'A'
    else:
        result = 'P'
    return result


input_path = 'C:/projects/pamc/data/supervision/'
input_path_af = 'C:/projects/pamc/data/supervision/archivos_fijos/'
output_path = 'C:/projects/pamc/data/bases/current_data/'


# Carga de archivos

xls0 = pd.ExcelFile(output_path + 'PAMC_Hitos.xlsx')
xls1 = pd.ExcelFile(input_path + 'PAMC_Auditorías.xlsx')
xls2 = pd.ExcelFile(input_path + 'PAMC_Plan_de_Trabajo.xlsx')
xls3 = pd.ExcelFile(input_path + 'PAMC_Supervisión_Etapas.xlsx')
xls4 = pd.ExcelFile(input_path_af + 'pdt_map.xlsx')
xls5 = pd.ExcelFile(input_path_af + 'etapas_map.xlsx')

fechas = pd.read_excel(xls0, 'Sheet1')
auditorias = pd.read_excel(xls1, 'Sheet1')
pdt = pd.read_excel(xls2, 'Sheet1')
etapas = pd.read_excel(xls3, 'Sheet1')

pdt_map = pd.read_excel(xls4, 'Hoja1')
etapas_map = pd.read_excel(xls5, 'Hoja1')


# # Limpieza

# #### Obtener fechas para filtrar el periodo correspondiente
start_date, end_date = '2021-01-01', '2021-03-31'
fechas = fechas[['Auditoría',
                 'Auditoría Periodo Actual',
                 'Hito',
                 'Fecha Real']]
fechas = fechas[fechas['Hito'] == 'Emisión Informe']
fechas['Fecha Real'] = fechas['Fecha Real'].astype(str).str[:10]
fechas = fechas[
    (fechas['Fecha Real'] >= start_date) & (fechas['Fecha Real'] <= end_date)]


# #### Limpieza de Planes de trabajo y etapas: Normalización de nombres

# Eliminar columna sin nombre
pdt_map = pdt_map.dropna(subset=['Unnamed: 1'])
etapas_map = etapas_map.dropna(subset=['Unnamed: 1'])

# Renombrar columna con valores normalizados
pdt_map.rename(columns={'Unnamed: 1': 'plan_norm'}, inplace=True)
etapas_map.rename(columns={'Unnamed: 1': 'etapa_norm'}, inplace=True)


# #### Limpieza de Planes de trabajo y etapas: Crear DataFrame normalizado

pdt_norm = pdt.merge(
    pdt_map,
    left_on='Plan de Trabajo',
    right_on='Plan de Trabajo',
    how='inner')[['Auditoría',
                  'Auditoría Periodo Actual',
                  'plan_norm',
                  'Estado de Finalización']].\
                      rename(columns={'Estado de Finalización': 'estado_pdt'})
etapas_norm = etapas.merge(
    etapas_map,
    left_on='Etapa',
    right_on='Etapa',
    how='inner')[['Auditoría',
                  'Auditoría Periodo Actual',
                  'Plan de Trabajo',
                  'etapa_norm',
                  'Estado de Finalización']].\
                merge(pdt_map,
                      on='Plan de Trabajo',
                      how='inner').\
                rename(columns={'Estado de Finalización': 'estado_etapa'})


# # Ejecución

# #### Juntar planes de trabajo, etapas y estado de auditorías en un DataFrame

key_columns = ['Auditoría', 'Auditoría Periodo Actual', 'plan_norm']
data = pdt_norm.merge(
    etapas_norm,
    on=key_columns,
    how='left').reset_index(drop=True).\
        merge(auditorias, on='Auditoría', how='left')


# #### Ordenar columnas

columns = ['Auditoría',
           'Auditoría Periodo Actual',
           'plan_norm',
           'estado_pdt',
           'etapa_norm',
           'estado_etapa',
           'Estado de Auditoría / Aprobación de Auditoría']
data = data[columns]


# #### Filtrar periodo correspondiente

columns.append('Fecha Real')
data = data.merge(
    fechas,
    on=['Auditoría', 'Auditoría Periodo Actual'],
    how='inner')[columns]
data = data.rename(
    columns={'Estado de Auditoría / Aprobación de Auditoría': 'estado_aud'})


# #### Llenar valores nulos
# Dado que existen auditorías sin etapas, la llenamos el plan de trabajo

data['etapa_norm'] = data['etapa_norm'].fillna(data['plan_norm'])
data['estado_etapa'] = data['estado_etapa'].fillna(data['estado_pdt'])


# #### Agregar analizar estado de acuerdo a parámetros PAMC (C, R, A, P)

#  - C: Completado, aplica para la revisión a nivel de auditor
#  - R: Revisado, aplica para la revisión a nivel de jefe
#  - A: Aprobado, aplica para la revisión a nivel de subgerente
#  - P: Pendiente, aplica para todos los niveles

# El auditor hizo su trabajo cuando la ETAPA está aprobada o completada
status_auditor = data.groupby(['Auditoría',
                               'Auditoría Periodo Actual',
                               'plan_norm',
                               'etapa_norm']).\
    apply(lambda x: status(x['estado_etapa'], position='auditor')).\
    reset_index().rename(columns={0: 'status_auditor'})

# El jefe hizo su trabajo cuando la ETAPA está aprobada
status_boss = data.groupby(['Auditoría',
                            'Auditoría Periodo Actual',
                            'plan_norm',
                            'etapa_norm']).\
    apply(lambda x: status(x['estado_etapa'], position='boss')).\
    reset_index().rename(columns={0: 'status_boss'})

# El subgerente hizo su trabajo cuando el PLAN DE TRABAJO está aprobad0
status_subg = data.groupby(['Auditoría',
                            'Auditoría Periodo Actual',
                            'plan_norm']).\
    apply(lambda x: status(x['estado_pdt'], position='subg')).\
    reset_index().rename(columns={0: 'status_subg'})


# Se une horizontalmente los resultados anteriores (auditor, jefe y subgerente)
full_data = status_auditor.merge(status_boss,
                                 on=['Auditoría',
                                     'Auditoría Periodo Actual',
                                     'plan_norm',
                                     'etapa_norm'],
                                 how='inner').\
                                merge(status_subg,
                                      on=['Auditoría',
                                          'Auditoría Periodo Actual',
                                          'plan_norm'],
                                      how='left')

# #### Cálculo del porcentaje de cumplimiento

# Se calcula el porcentaje de Completado, Revisado o Aprobado (según el cargo)
result_auditor = status_auditor.\
    groupby(['Auditoría',
             'Auditoría Periodo Actual']).\
    apply(lambda x: compliance(x['status_auditor'], position='auditor')).\
    reset_index().rename(columns={0: 'percentage_auditor'})

result_boss = status_boss.\
    groupby(['Auditoría',
             'Auditoría Periodo Actual']).\
    apply(lambda x: compliance(x['status_boss'], position='boss')).\
    reset_index().rename(columns={0: 'percentage_boss'})

result_subg = status_subg.\
    groupby(['Auditoría',
             'Auditoría Periodo Actual']).\
    apply(lambda x: compliance(x['status_subg'], position='subg')).\
    reset_index().rename(columns={0: 'percentage_subg'})

# Se consolida la información en un DataFrame
result_per_audit = result_auditor.merge(result_boss,
                                        on=['Auditoría',
                                            'Auditoría Periodo Actual'],
                                        how='inner').\
                                  merge(result_subg,
                                        on=['Auditoría',
                                            'Auditoría Periodo Actual'],
                                        how='left')

# #### Resumen del porcentaje de cumplimiento

n_auditor_real = len(status_auditor[status_auditor['status_auditor'] == 'C'])
n_boss_real = len(status_boss[status_boss['status_boss'] == 'R'])
n_subg_real = len(status_subg[status_subg['status_subg'] == 'A'])

compliance_auditor = n_auditor_real/len(status_auditor)
compliance_boss = n_boss_real/len(status_boss)
compliance_subg = n_subg_real/len(status_subg)

summary_results_dict = {
    'Percentage': [compliance_auditor, compliance_boss, compliance_subg],
    'N_Real': [n_auditor_real, n_boss_real, n_subg_real],
    'N_Total': [len(status_auditor), len(status_boss), len(status_subg)],
    'Cargos': ['Auditor', 'Jefe', 'Subgerente']
}

summary_results = pd.DataFrame(summary_results_dict).set_index('Cargos')

list(result_per_audit['Auditoría'].unique())

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(output_path + 'pamc1_supervision.xlsx',
                        engine='xlsxwriter')

# Write each dataframe to a different worksheet.
summary_results.to_excel(writer, sheet_name='resumen')
result_per_audit.to_excel(writer, sheet_name='resultados_por_auditoria')
full_data.to_excel(writer, sheet_name='datos')

# Close the Pandas Excel writer and output the Excel file.
writer.save()
