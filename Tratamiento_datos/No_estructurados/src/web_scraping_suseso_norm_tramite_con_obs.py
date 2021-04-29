# Importar librerías
import pandas as pd
from scrapy import Selector
import requests
from datetime import datetime, timedelta
import wget

output_path = 'C:/projects/data_lake/data/output/current/cambios_reg/suseso/norm_tramite/'
output_path_pdf = 'C:/projects/data_lake/data/input/current/cambios_reg/suseso/norm_tramite/'

# Cargar código html a utilizar
html = requests.get(
    'https://www.suseso.cl/612/w3-propertyvalue-34035.html').content

# Crear selectors para después buscar la información
sel = Selector(text=html)

# Búsqueda de información

# Lista fecha publicación
fecha_publicacion = sel.xpath(
    '//td[contains(@class,"fecha cid-512 pnid-524")]/text()').extract()

# Lista Detalle
materia = sel.xpath(
    '//td[contains(@class," cid-512 pnid-525 pn-attr")]/text()').extract()

# Lista fecha para observaciones
fecha_observacion = sel.xpath(
    '//td[contains(@class,"fecha cid-512 pnid-555")]/text()').extract()

# Lista email para observaciones
email_observacion = sel.xpath(
    '//td[contains(@class,"cid-512 pnid-554")]/text()').extract()

# Lista con links
links = sel.xpath(
    '//td[contains(@class,"normativa-consultar")]/span[contains(@class,"figure cid-512")]/a/@href').extract()

urls = []
for i in links:
    url = 'https://www.suseso.cl/612/' + i
    urls.append(url)

# Almacenar información obtenida y agregar formatos

df_norm_tramite = pd.DataFrame({
    'Fecha de Publicación': fecha_publicacion,
    'Título de la Norma': materia,
    'Fecha fin Vigencia': fecha_observacion,
    'Url': urls})


# Creamos nueva columna con fecha publicación y observación.
df_norm_tramite['aux'] = df_norm_tramite.index.tolist()
df_norm_tramite['Tipo/Número'] = df_norm_tramite['aux'].astype(str) + '_' +  df_norm_tramite['Fecha de Publicación'] + "_" + df_norm_tramite['Fecha fin Vigencia']

# Aplicamos formato fecha a columna "Fecha Publicación" y "Fecha envio observaciones"
df_norm_tramite['Fecha de Publicación'] = pd.to_datetime(
    df_norm_tramite['Fecha de Publicación'],
    format='%d/%m/%Y')

# Eliminar columna aux y reordenar
df_norm_tramite = df_norm_tramite.drop(['aux'], axis=1)
df_norm_tramite = df_norm_tramite[['Tipo/Número','Fecha de Publicación', 'Título de la Norma', 'Fecha fin Vigencia', 'Url']]

# Filtrar novedades
novedad = datetime.now() - timedelta(days=60)
df_norm_novedad = df_norm_tramite.loc[(df_norm_tramite['Fecha de Publicación'] >= novedad)]
df_norm_novedad
nombre_archivo = df_norm_novedad['Tipo/Número']
nombre_archivo = nombre_archivo.str.replace('/', '-')
urls_novedad = df_norm_novedad['Url']

# Descargar novedades

# Rutas para almacenar la información

paths = []

for i in nombre_archivo:
    # La ruta escrita entre '' debe ser cambiada por quien ejecute el código.
    path = output_path_pdf + i
    paths.append(path)

# Descargar archivos

i = 0

for url in urls_novedad:
    wget.download(url, paths[i]+',pdf')
    i = i + 1

# Guardar DF históricos en Excel
nombre_archivo = 'Normativa_trámite(en_plazo)_SUSESO'
df_norm_tramite.to_csv(output_path + datetime.now().strftime('%Y%m%d') + '_' + nombre_archivo + '.csv', sep='|')