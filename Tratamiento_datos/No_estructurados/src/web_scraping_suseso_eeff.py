# Importar librerías

import pandas as pd
from scrapy import Selector
import requests
from datetime import datetime, timedelta
import wget

output_path = 'C:/projects/data_lake/data/input/current/eeff_emitidos/'

# Utilidades


def val(row):
    if row['Mes'] == 'Enero':
        val = 1
    elif row['Mes'] == 'Febrero':
        val = 2
    elif row['Mes'] == 'Marzo':
        val = 3
    elif row['Mes'] == 'Abril':
        val = 4
    elif row['Mes'] == 'Mayo':
        val = 5
    elif row['Mes'] == 'Junio':
        val = 6
    elif row['Mes'] == 'Julio':
        val = 7
    elif row['Mes'] == 'Agosto':
        val = 8
    elif row['Mes'] == 'Septiembre':
        val = 9
    elif row['Mes'] == 'Octubre':
        val = 10
    elif row['Mes'] == 'Noviembre':
        val = 11
    elif row['Mes'] == 'Diciembre':
        val = 12
    else:
        val = 'error'
    return val


# Cargar código html a utilizar

html = requests.get(
    'https://www.suseso.cl/609/w3-propertyvalue-10342.html').content

# Crear selectors para después buscar la información

sel = Selector(text=html)

# Búsqueda de información

# Lista periodos

anno = sel.xpath('//td[contains(@class,"fecha periodo")]/span[contains(@class,"pv-branch pnid-545 cid-501")]/span[1]/text()').extract()
mes = sel.xpath('//td[contains(@class,"fecha periodo")]/span[contains(@class,"pv-branch pnid-545 cid-501")]/span[2]/text()').extract()

# Lista tipo de informe

informe = sel.xpath('//span[contains(@class,"pv-branch pnid-500 cid-501")]/span/text()').extract()

# Lista títulos

titulo = sel.xpath('//td[contains(@class,"titulo aid")]/a/text()').extract()
titulo_estatuto = titulo.pop()

# Lista con links

links = sel.xpath('//td[contains(@class,"text-center cid-509 aid")]/a/@href').extract()
link_estuto = links.pop()
urls = []
for i in links:
    url = 'https://www.suseso.cl/609/' + i
    urls.append(url)

# Almacenar información obtenida y agregar formatos

df_eeff = pd.DataFrame({
    'Año': anno,
    'Mes': mes,
    'Tipo Informe': informe,
    'Título': titulo,
    'Link': urls})

# Creamos nueva columna con meses en numeros

df_eeff['mes_n'] = df_eeff.apply(val, axis=1).astype('string')

# Creamos nueva columna auxiliar para filtrar novedades luego

df_eeff['aux'] = df_eeff['Año'] + df_eeff['mes_n'] + str(30)

# Aplicamos formato fecha a columna "aux"

df_eeff['aux'] = pd.to_datetime(df_eeff['aux'], format='%Y%m%d')

# Quitamos ":" de los títulos y reemplazamos por " "

df_eeff['Título'] = df_eeff['Título'].str.replace(':', ' ')

# Filtrar novedades

novedad = datetime.now() - timedelta(days=180)

df_eeff_novedad = df_eeff.loc[(df_eeff['aux'] >= novedad)]
df_eeff_novedad

nombre_archivo = df_eeff_novedad['Año'] + df_eeff_novedad['Mes'] + df_eeff_novedad['Tipo Informe'] + '.pdf'
nombre_archivo = nombre_archivo.str.replace(' ', '')

urls_novedad = df_eeff_novedad['Link']

# Descargar novedades

# Rutas para almacenar la información

paths = []
for i in nombre_archivo:
    # La ruta escrita entre '' debe ser cambiada por quien ejecute el código.
    path = output_path + i
    paths.append(path)

# Descargar archivos

i = 0
for url in urls_novedad:
    wget.download(url, paths[i]+'.pdf')
    i = i + 1

# Eliminamos columnas auxiliares

df_eeff = df_eeff.drop(['mes_n', 'aux'], axis=1)

# Guardar DF históricos en Excel

nombre_archivo = 'estados_financieros_emitidos'
df_eeff.to_csv(output_path + datetime.now().strftime('%Y%m%d') + '_' + nombre_archivo + '.csv')
