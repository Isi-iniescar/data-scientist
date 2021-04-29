import pandas as pd
from scrapy import Selector
import requests
from datetime import datetime, timedelta
import wget

# Cargar códigos html de las páginas a utilizar
html_circ_interna = requests.get(
    'http://www.supersalud.gob.cl/normativa/668/w3-propertyvalue-6258.html'
    ).content
html_resoluciones = requests.get(
    'http://www.supersalud.gob.cl/normativa/668/w3-propertyvalue-6259.html'
    ).content
html_oficio_circ = requests.get(
    'http://www.supersalud.gob.cl/normativa/668/w3-propertyvalue-6260.html'
    ).content

# Crear selectors para después buscar la información
sel_ci = Selector(text=html_circ_interna)
sel_r = Selector(text=html_resoluciones)
sel_oc = Selector(text=html_oficio_circ)

# Búsqueda de información

# Lista de fechas
fechas_ci = sel_ci.xpath(
    '//td[contains(@class,"fecha-publicacion")]/text()'
    ).extract()
fechas_r = sel_r.xpath(
    '//td[contains(@class,"fecha-publicacion")]/text()'
    ).extract()
fechas_oc = sel_oc.xpath(
    '//td[contains(@class,"fecha-publicacion")]/text()'
    ).extract()

# Lista Títulos
titulos_ci = sel_ci.xpath(
    '//td/span[contains(@class, "td_30_porciento titulo-principal")]/a/text()'
    ).extract()
titulos_r = sel_r.xpath(
    '//td/span[contains(@class, "td_30_porciento titulo-principal")]/a/text()'
    ).extract()
titulos_oc = sel_oc.xpath(
    '//td/span[contains(@class, "td_30_porciento titulo-principal")]/a/text()'
    ).extract()

# Lista con descipción
descripcion_ci = sel_ci.xpath(
    '//span[contains(@class,"td_30_porciento resumen-modificaciones")]/text()'
    ).extract()
descripcion_r = sel_r.xpath(
    '//span[contains(@class,"td_30_porciento resumen-modificaciones")]/text()'
    ).extract()
descripcion_oc = sel_oc.xpath(
    '//span[contains(@class,"td_30_porciento resumen-modificaciones")]/text()'
    ).extract()

# Lista con links
links_ci = sel_ci.xpath(
    '//td/span[contains(@class, "td_20_porciento cid-19")]/a/@href'
    ).extract()
url_circ_internas = []
for i in links_ci:
    url = 'http://www.supersalud.gob.cl/normativa/668/'+i
    url_circ_internas.append(url)

links_r = sel_r.xpath(
    '//td/span[contains(@class, "td_20_porciento cid-19")]/a/@href'
    ).extract()
url_resoluciones = []
for i in links_r:
    url = 'http://www.supersalud.gob.cl/normativa/668/'+i
    url_resoluciones.append(url)

links_oc = sel_oc.xpath(
    '//td/span[contains(@class, "td_20_porciento cid-19")]/a/@href'
    ).extract()
url_oficio_circ = []
for i in links_oc:
    url = 'http://www.supersalud.gob.cl/normativa/668/'+i
    url_oficio_circ.append(url)

# Almacenar información obtenida y agregar formatos
df_circ_internas = pd.DataFrame({'Tipo/Número': titulos_ci,
                                 'Fecha de Publicación': fechas_ci,
                                 'Título de la Norma': descripcion_ci,
                                 'Fecha fin Vigencia': '',
                                 'Url': url_circ_internas})
df_circ_internas['Fecha de Publicación'] = pd.to_datetime(
    df_circ_internas['Fecha de Publicación']
    )
df_circ_internas['Tipo/Número'] = df_circ_internas['Tipo/Número'].str.replace('/', ' ')

df_resoluciones = pd.DataFrame({'Tipo/Número': titulos_r,
                                'Fecha de Publicación': fechas_r,
                                'Título de la Norma': descripcion_r,
                                'Fecha fin Vigencia': '',
                                'Url': url_resoluciones})
df_resoluciones['Fecha de Publicación'] = pd.to_datetime(
    df_resoluciones['Fecha de Publicación']
    )
df_resoluciones['Tipo/Número'] = df_resoluciones['Tipo/Número'].str.replace('/', ' ')

df_oficio_circ = pd.DataFrame({'Tipo/Número': titulos_oc,
                               'Fecha de Publicación': fechas_oc,
                               'Título de la Norma': descripcion_oc,
                               'Fecha fin Vigencia': '',
                               'Url': url_oficio_circ})
df_oficio_circ['Fecha de Publicación'] = pd.to_datetime(
    df_oficio_circ['Fecha de Publicación']
    )
df_oficio_circ['Tipo/Número'] = df_oficio_circ['Tipo/Número'].str.replace('/', ' ')


# Filtrar novedades
novedad = datetime.now() - timedelta(days=30)

df_circ_internas_novedad = df_circ_internas.loc[
    (df_circ_internas['Fecha de Publicación'] >= novedad)]
df_circ_internas_novedad
link_circ_internas_novedad = df_circ_internas_novedad['Url']
titulos_ci_novedad = df_circ_internas_novedad['Tipo/Número'].str.replace(' ', '')

df_resoluciones_novedad = df_resoluciones.loc[
    (df_resoluciones['Fecha de Publicación'] >= novedad)]
df_resoluciones_novedad
link_resoluciones_novedad = df_resoluciones_novedad['Url']
titulos_r_novedad = df_resoluciones_novedad['Tipo/Número'].str.replace(' ', '')

df_oficio_circ_novedad = df_oficio_circ.loc[
    (df_oficio_circ['Fecha de Publicación'] >= novedad)]
df_oficio_circ_novedad
link_oficio_circ_novedad = df_oficio_circ_novedad['Url']
titulos_oc_novedad = df_oficio_circ_novedad['Tipo/Número'].str.replace(' ', '')

# Rutas para almacenar la información
path_ci = []
for i in titulos_ci_novedad:
    # La ruta escrita entre '' debe ser cambiada por quien ejecute el código.
    path = 'C:/projects/data_lake/data/input/current/cambios_reg/super_salud/circulares/'+i
    path_ci.append(path)

path_r = []
for i in titulos_r_novedad:
    # La ruta escrita entre '' debe ser cambiada por quien ejecute el código.
    path = 'C:/projects/data_lake/data/input/current/cambios_reg/super_salud/resoluciones/'+i
    path_r.append(path)

path_oc = []
for i in titulos_oc_novedad:
    # La ruta escrita entre '' debe ser cambiada por quien ejecute el código.
    path = 'C:/projects/data_lake/data/input/current/cambios_reg/super_salud/oficio_circular/'+i
    path_oc.append(path)

# Descargar archivos
i = 0
for url in link_circ_internas_novedad:
    wget.download(url, path_ci[i]+'.pdf')
    i = i + 1

i = 0
for url in link_resoluciones_novedad:
    wget.download(url, path_r[i]+'.pdf')
    i = i + 1

i = 0

for url in link_oficio_circ_novedad:
    wget.download(url, path_oc[i]+'.pdf')
    i = i + 1

# Guardar DF históricos en csv
output_path = 'C:/projects/data_lake/data/output/current/cambios_reg/super_salud/'

df_circ_internas.to_csv(output_path + datetime.now().strftime('%Y%m%d') + '_' + 'circulares_internas_SuperSalud' + '.csv', sep='|')
df_resoluciones.to_csv(output_path + datetime.now().strftime('%Y%m%d') + '_' + 'resoluciones_SuperSalud' + '.csv', sep='|')
df_oficio_circ.to_csv(output_path + datetime.now().strftime('%Y%m%d') + '_' + 'oficio_circular_SuperSalud' + '.csv', sep='|')
