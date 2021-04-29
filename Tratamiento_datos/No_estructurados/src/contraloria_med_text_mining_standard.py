# Importar librerías
import PyPDF2
import pandas as pd
import glob
from datetime import datetime

# Utilidades


def read_pdf(input_path):
    '''Lee los archivos pdf del path y crea diccionario con respectivas listas'''
    all_files = pd.Series(glob.glob(input_path + "/*.pdf"))
    d = {}
    for i in range(0, len(all_files)):
        file = all_files[i]
        index_from = file.rfind("\\") + 1
        pdf_file = open(file, 'rb')
        d[str(file[index_from:-4])] = PyPDF2.PdfFileReader(pdf_file)
    return d


def page_content(read_pdf):
    '''Almacena el contenido de las páginas'''
    page1 = read_pdf.getPage(1)
    page1_content = page1.extractText()
    page2 = read_pdf.getPage(2)
    page2_content = page2.extractText()
    page3 = read_pdf.getPage(3)
    page3_content = page3.extractText()
    page4 = read_pdf.getPage(4)
    page4_content = page4.extractText()
    global pages
    pages = [page1_content, page2_content, page3_content, page4_content]
    return pages


def limpieza(lista, pag, text):
    '''Limpia las listas para que las palabras queden separadas'''
    aux = []
    for i in lista:
        row = i.split(' ')
        aux.append(row)
    for i in range(len(aux)):
        pag.extend(aux[i])
    for i in pag:
        i = i.lower()
        i = i.replace('.', '').replace(',', '')
        text.append(i)
    return text


def nivel_riesgo(paginas):
    '''Busca la palabra asociada al nivel de riesgo por cada página'''
    global res
    for i in paginas:
        if 'severo' in i:
            res = 4
        elif 'serio' in i:
            res = 4
        elif 'alto' in i:
            res = 3
        elif 'medio' in i:
            res = 2
        elif 'bajo' in i:
            res = 1


# Cargar archivos y páginas a leer
input_path = 'C:/projects/data_lake/data/input/current/direccion_salud/'
dicc = read_pdf(input_path)
keys = list(dicc.keys())
dicc_values = list(dicc.values())

# Procesamiento y obtención de resultado de auditoría

# **Cargar el contenido de las páginas de los pdf**

pages_pdf = []

for i in dicc_values:
    content = page_content(i)
    pages_pdf.append(pages)

# **Limpiar el contenido de las páginas**

n = 0
dicc_pag = {}

for listas in pages_pdf:
    for i in listas:
        lista = []
        text = []
        pag = []
        aux = []
        lista = i.split('\n')
        limpieza(lista, pag, text)
        dicc_pag[keys[n] + '_pag' + str(listas.index(i))] = [text]
    n = n + 1

# **Separar las páginas de un mismo documento en sublistas**

pages_all = list(dicc_pag.values())
paginas = [pages_all[i:i+4] for i in range(0, len(pages_all), 4)]

# **Extraer el nivel de riesgo del informe**

resultados = []

for i in paginas:
    for j in i:
        res = 0
        nivel_riesgo(j)
        if res != 0:
            resultados.append(res)
            break

# **Almacenar resultados en un DataFrame**

df = pd.DataFrame({'Nombre archivo': keys,
                   'Resultado': resultados})

# **Agregar de forma manual ISP Droguería por formato distinto**

df = df.append({'Nombre archivo': 'ISP Drogueria Comunicacion - Informe de Auditoría a la Drogueria_',
                'Resultado': 2}, ignore_index=True)

# ## Guardar archivo en csv

output_path = 'C:/projects/data_lake/data/output/current/direccion_salud/'
nombre_archivo = 'resultados_contraloria_med.csv'

df.to_csv(output_path+datetime.now().strftime('%Y%m%d')+'_'+nombre_archivo, sep=';')
