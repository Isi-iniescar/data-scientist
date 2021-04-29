# Importar librerias

import pandas as pd
from datetime import datetime as dt
from datetime import date
from dateutil.relativedelta import relativedelta
import glob


# ## Utilidades


def read_xlsx(hoja, cols, path, n=5):

    all_files = pd.Series(glob.glob(path + "/*.xlsx"))
    datasheet = []

    for i in range(0, n):

        fecha = (date.today() - relativedelta(months=(+n-i))).strftime("%Y%m")
        archivo = all_files[
            all_files.astype(str).str.contains(fecha)].tolist()[0]
        df = pd.read_excel(archivo, sheet_name=hoja, usecols=cols)
        df['Mes'] = i+1
        datasheet.append(df)

    datasheet = pd.concat(datasheet).reset_index(drop=True)

    return datasheet


# Cargar tablas

path = r'C:/ig03'

columns_base = ['BP SUCURSAL',
                'BP RUT',
                'CRITICIDAD SUCURSAL',
                'FUSIÓN CARTERA EP']

columns_responsables = ['CARTERA',
                        'RESPONSABLE POR CARTERA']

base_df = read_xlsx('Base', columns_base, path)
responsables_df = read_xlsx('Responsables por cartera',
                            columns_responsables,
                            path)


# Ejecución

# Empresas con criticidad roja (RO)
base_ro = base_df[base_df['CRITICIDAD SUCURSAL'] == 'RO']


# Empresas Rojas sin cartera por 5 meses

# Filtrar por sucursales sin cartera
base_sin_cartera = base_ro[base_ro['FUSIÓN CARTERA EP'].isnull()]

# Agrupamos para ver cuantos meses llevan sin cartera
base_group = base_sin_cartera[['BP SUCURSAL',
                               'BP RUT',
                               'CRITICIDAD SUCURSAL',
                               'Mes']].groupby(['BP SUCURSAL',
                                                'BP RUT',
                                                'CRITICIDAD SUCURSAL'],
                                               as_index=False).agg('count')

# Filtramos las sucursales que lleven 5 meses sin cartera
base_group_filtro = base_group[base_group['Mes'] == 5].reset_index(drop=True)

# Empresas rojas con cartera, pero sin experto por 5 meses

# Agregamos el nombre de los responsables por cartera
base_resp = pd.merge(base_ro[['BP SUCURSAL',
                              'BP RUT',
                              'CRITICIDAD SUCURSAL',
                              'FUSIÓN CARTERA EP']],
                     responsables_df,
                     left_on='FUSIÓN CARTERA EP',
                     right_on='CARTERA',
                     how='inner')

# Filtramos por las empresas que tienen responsable "Vacante"
base_vacantes = base_resp[
    base_resp['RESPONSABLE POR CARTERA'] == 'VACANTE'].drop_duplicates()

# Agrupamos para ver cuantos meses llevan sin responsable
base_group_2 = base_vacantes[['BP SUCURSAL',
                              'BP RUT',
                              'CRITICIDAD SUCURSAL',
                              'CARTERA',
                              'RESPONSABLE POR CARTERA',
                              'Mes']].groupby(['BP SUCURSAL',
                                               'BP RUT',
                                               'CRITICIDAD SUCURSAL',
                                               'CARTERA',
                                               'RESPONSABLE POR CARTERA'],
                                              as_index=False).agg(
                                                  {'Mes': 'count'})

# Filtramos las sucursales que llevan 5 meses sin responsable
base_group_vacantes = base_group_2[
    base_group_2['Mes'] == 5].reset_index(drop=True)

# Guardar archivo xlsx

nombre_archivo = 'IG03 ' + dt.now().strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')

base_group_filtro.to_excel(writer, sheet_name='Sin cartera')
base_group_vacantes.to_excel(writer, sheet_name='Sin Experto')

writer.save()
