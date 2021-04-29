
# ## Importar librerías

import pandas as pd
import numpy as np
import glob
from datetime import datetime as dt
import datetime
from datetime import date
import string
import unicodedata


# ## Utilidades

def rreplace(s, old, new, n_occurrence):
    # Replace the last n_occurrence (n_occurrence = 1 is the last occurrence) of an expression in a string s
    li = s.rsplit(old, n_occurrence)
    return new.join(li)

def str_format(stringg):
    
    #Elimina simbolos de puntuación, u otros simbolos de un string
    
    punctuation = list(string.punctuation)          
    symbols = ['^','°', 'ª']                     
    new_string = unicodedata.normalize('NFKD', stringg).encode('ascii', errors='ignore').decode('utf-8') # Elimina tildes 

    for i in range(len(punctuation)):
        new_string = new_string.replace(punctuation[i],'_')   # Elimina simbolos de puntuación
    
    for i in range(len(symbols)):
        new_string = new_string.replace(symbols[i],'')       # Elimina otros simbolos
    
    new_string = new_string.replace(' ','_').lower()         # Elimina espacios 
    
    while new_string[-1] == '_':
        new_string = rreplace(new_string,'_','',1)           # Elimina los "espacios" (_) al final del string
    
    return new_string


def read_txt(path, header_int, sep = '|', encoding = 'latin1'):
# Lee los archivos txt del path (correspondientes a la fecha de descarga), y crea diccionario con respectivos dataframes
    all_files = pd.Series(glob.glob(path + "/*.txt"))
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

def clean_columns(dict, del_from, col=0):
    for keys in dict:
        # Elimina la primera y última columna 
        dict[keys] = dict[keys].drop(columns=dict[keys].columns[0]).        drop(columns = dict[keys].columns[-1]).dropna(how = 'all').reset_index(drop = True)
        del_from_index = dict[keys].index[dict[keys][dict[keys].columns[col]].str.contains(del_from)].tolist()[0]
        
        #Elimina filas al final de txt que presentan el resumen del libro de compras/ventas
        dict[keys] = dict[keys].iloc[0:del_from_index,:]
    return dict

def save_as_csv(dict, path, suffixes='', sep=';', encoding = 'latin1'):
    
    for keys in dict:
        dict[keys].to_csv(path + '\\' + dt.now().strftime('%Y%m%d') + str(keys) + suffixes + '.csv', sep=sep)
        
def read_xlsx(path, header_int, sheet_name_str):
# Lee los archivos xlsx del path (correspondientes a la fecha de descarga), y crea diccionario con respectivos dataframes
    all_files = pd.Series(glob.glob(path + "/*.xlsx"))
    fecha_str = date.today().strftime("%Y%m")
    files = all_files[all_files.astype(str).str.contains(fecha_str)].tolist()
    d = {}
    
    for i in range(0,len(files)):
        file_path = files[i]
        index_from = file_path.rfind("\\") + 1
        file_name = str_format(file_path[index_from:-4])
        
        if file_name[:8].isdigit():
            file_name = file_name[9:]
        
        d[str(file_name)] = pd.read_excel(file_path, header=header_int, sheet_name=sheet_name_str, dtype=str)
    return d

def col_names_format(dict):
    
    for keys in dict:
        df_col = dict[keys].columns
        col_list = []
        for col in df_col:
            col_list.append(str_format(col))
        dict[keys].columns = col_list
    
    return dict

def schema_cols(dict, col_names, empty_col_value = np.nan):
    for keys in dict:
        for col in col_names:
            if col not in dict[keys].columns:
                dict[keys][col] = empty_col_value
        dict[keys] = dict[keys][col_names]
    return dict


# ## Ejecutar limpieza
# ### Leer archivos y guardar en diccionario dict

dict_mrc = read_xlsx(r'C:/projects/data_lake/data/input/current/pentana',2,"Matriz de Riesgo")

for keys in dict_mrc:
    print(keys, len(dict_mrc[keys].columns))


# ### Cambiar formato de nombre de columnas

# In[10]:


dict_mrc = col_names_format(dict_mrc)


dict_mrc['mrc_seguridad_de_la_informacion_2020'].columns


# ### Establecer columnas que serán parte del esquema

col_names = ['unnamed__0', 'proceso', 'subproceso', 'riesgo_relevante', 'actividad',
       'numero_de_riesgo', 'que_puede_fallar', 'como_puede_fallar',
       'quien_puede_fallar', 'donde_puede_fallar', 'riesgo_especifico',
       'unnamed__11', 'participacion_en_el_mercado', 'reputacion', 'economico',
       'legal', 'medio_ambiente__rse',
       'perjuicio_a_colaboradores_o_beneficiarios', 'operativa__continuidad',
       'unnamed__19', 'tipo_de_proceso', 'recursos', 'frecuencia', 'masividad',
       'unnamed__24', 'impacto', 'probabilidad', 'nivel', 'unnamed__28',
       'area_que_levanta_el_riesgo', 'incorporacion_en_el_programa_de_trabajo',
       'unnamed__31', 'unnamed__32']

dict_mrc = schema_cols(dict_mrc, col_names)

for keys in dict_mrc:
    print(keys, len(dict_mrc[keys].columns))


# ### Guardar en archivos csv

save_as_csv(dict_mrc, r'C:/projects/data_lake/data/output/current/pentana')
