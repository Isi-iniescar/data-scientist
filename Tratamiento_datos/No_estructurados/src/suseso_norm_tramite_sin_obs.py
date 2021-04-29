# Importar librerías
import pandas as pd
from scrapy import Selector
import requests
from datetime import datetime, timedelta, date
import wget

output_path = 'C:/projects/data_lake/data/output/current/cambios_reg/suseso/norm_tramite/'
output_path_pdf = 'C:/projects/data_lake/data/input/current/cambios_reg/suseso/norm_tramite/'

# Cargar códigos html de las páginas a utilizar
html = requests.get(
    'https://www.suseso.cl/612/w3-propertyvalue-34036.html'
    ).content

# Crear selectors para después buscar la información
sel = Selector(text=html)

# Búsqueda de información

# Lista de fechas
fechas = sel.xpath(
    '//td[contains(@class,"fecha cid-512 pnid-524")]/text()'
    ).extract()

# Se elimina registro sin link de 2011
fechas.remove("13/09/2011")

# Lista Materia
materia = sel.xpath(
    '//td[contains(@class,"materia cid-512 pnid-525")]/text()'
    ).extract()

# Se elimina registro sin link de 2011
materia.remove("Proyecto de Circular conjunta de las Superintendencias de Pensiones, Valores y Seguros y de Seguridad Social. Establece regulaciones comunes relativas a las obligaciones de las Administradoras de Fondos de Pensiones, Compañías de Seguros de Vida, Mutualidades de Empleadores de la Ley N° 16.744, Instituto de Seguridad Laboral e Instituto de Previsión Social, respecto de la determinación y pago de los beneficios relativos a la exención de la cotización legal de salud. Agrega nuevo Título XVI al Libro III del Compendio de Normas del Sistema de Pensiones de la Superintendencia de Pensiones.")


# Lista fecha para observaciones
fecha_obs = sel.xpath(
    '//td[contains(@class,"fecha cid-512 pnid-555")]/text()'
    ).extract()

# Se elimina registro sin link de 2011
fecha_obs.remove("23/09/2011 ")

# Lista links
links = sel.xpath(
    '//td[contains(@class,"normativa-consultar")]/span[contains(@class,"figure cid-512")]/a/@href'
    ).extract()

urls = []
for i in links:
    url = 'https://www.suseso.cl/612/'+i
    urls.append(url)

# Almacenar información obtenida y agregar formatos
df_norm = pd.DataFrame({
    'Fecha de Publicación': fechas,
    'Título de la Norma': materia,
    'Fecha fin Vigencia': fecha_obs,
    'Url': urls})

# Creamos columna auxiliar con nombre de archivo
df_norm['aux'] = df_norm.index.tolist()
df_norm['Tipo/Número'] = df_norm['aux'].astype(str) + '_' + df_norm['Fecha de Publicación'] + '_' + df_norm['Fecha fin Vigencia']
df_norm['Tipo/Número'] = df_norm['Tipo/Número'].str.replace('/', '-')
df_norm = df_norm.drop(['aux'], axis=1)

# Limpiamos espacios en columnas de interés
df_norm['Fecha de Publicación'] = df_norm['Fecha de Publicación'].str.replace(" ", "")
df_norm['Fecha fin Vigencia'] = df_norm['Fecha fin Vigencia'].str.replace(" ", "")

# Agregamos formato fecha
df_norm['Fecha de Publicación'] = pd.to_datetime(
    df_norm['Fecha de Publicación'], format='%d/%m/%Y')
df_norm['Fecha fin Vigencia'] = pd.to_datetime(
    df_norm['Fecha fin Vigencia'], format='%d/%m/%Y')

# Filtrar novedades
novedad = datetime.now() - timedelta(30)
df_norm_novedad = df_norm.loc[(df_norm['Fecha de Publicación'] >= novedad)]
links_novedad = df_norm_novedad['Url']
nombre_archivo = df_norm_novedad['Tipo/Número']

# Descargar novedades

# Rutas para almacenar la información
paths = []
for i in nombre_archivo:
    # La ruta escrita entre '' debe ser cambiada por quien ejecute el código.
    path = output_path_pdf + i
    paths.append(path)

# Descargar archivos
i = 0
for url in links_novedad:
    wget.download(url, paths[i]+'.pdf')
    i = i + 1

# Guardar DF históricos en Excel
nombre_archivo = 'Normativa_trámite(fuera_de_plazo)_SUSESO'
df_norm.to_csv(output_path + datetime.now().strftime('%Y%m%d') + '_' + nombre_archivo + '.csv', sep='|')
