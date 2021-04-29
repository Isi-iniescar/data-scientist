#!/usr/bin/env python
# coding: utf-8

# # IG08 - Apertura de periodos contables anteriores

# El indicador IG08 verifica que no se abran periodos contables ya cerrados. El área responsable es la Gerencia de Administración y Finanzas.
# 
# El siguiente código es para ejecutar el indicador de Auditoría Continua IG08 - Apertura periodos contables.
# 
#  - ***Fecha creación: 21.12.2020***

# ## Importar librerías
# ---

# In[ ]:


import pandas as pd
import numpy as np
from datetime import datetime
import string


# ## Utilidades
# ---

# In[ ]:


def clean_blankspace(df):
    # Eliminar espacios en nombres de columnas
    columns = list(df.columns)
    columns = [w.replace(' ','') for w in columns]
    df.columns = columns
    #Eliminar espacios en cada celda del dataframe
    for columna in list(df.columns):
        if df.dtypes[columna] == np.dtype('O'):
            df[columna] = df[columna].str.replace(' ','')
    return df

def clean_columns(txt):
    df = txt.drop(columns=txt.columns[0:2]).drop(columns=txt.columns[-1]).dropna(how='all').reset_index(drop=True)
    return df

def desviacion_mes(fecha_ingreso,fecha_contab): 
    if fecha_contab < fecha_ingreso:
        var = True
    else:
        var = False
    return var


# ## Cargar tablas
# ---

# In[ ]:


doctos = pd.read_csv('PER_CONTAB.txt', sep='|', header=3, encoding='latin1')


# ## Limpieza
# ---

# **Limpieza columnas y espacios**

# In[ ]:


# Se hace la limpieza de las columnas y espacios en blanco
doctos = clean_columns(doctos)
doctos = clean_blankspace(doctos)

# Se eliminan columnas innecesarias
doctos.drop(columns=['Mandante', 'Horadeentr.', 'Fe.conversión', 'Moneda'], inplace=True)


# **Definir columnas auxiliares**

# In[ ]:


#creo columnas mes para comparar fechas
doctos['Mes_Registro'] = doctos['Registradoel'].str[3:5]
doctos['Mes_Contab'] = doctos['Fechacontab.'].str[3:5]


# **Definir el esquema**

# In[ ]:


schema_doctos = {'Sociedad': str,            # Sociedad
                'Nºdocumento': 'float64',    # N_documento
                'Ejercicio': 'float64',      # Año
                'Clasedoc.': str,            # Clase_documento
                'Fechadocumento': str,       # Fecha_docto
                'Fechacontab.': str,         # Fecha_docto
                'Período': 'float64',        # Año
                'Registradoel': str,         # Fecha_registro
                'Nombreusuario': str,        # Nombre_usuario
                'Cód.transacción': str,      # Codigo_transaccion
                'Referencia': str,           # referencia_docto
                'Anuladocon': str,           # referencia anulacion
                'Txt.cab.doc.': str,         # Codigo_transaccion
                'Mes_Registro': int,         # mes de campo Registradoel
                'Mes_Contab': int}           # mes de campo Fechacontab.
    
doctos = doctos.astype(schema_doctos)


# ## Ejecución
# ---

# In[ ]:


# Debemos utilizar el metodo lambda en conjunto con la función definida
doctos['CASOS'] = doctos.apply(lambda x: desviacion_mes(x['Mes_Registro'], x['Mes_Contab']), axis=1)


# **Filtrar los casos que sean desviaciones**
# 
# Son aquellos en donde el Mes_registro y Mes_contab es distinto y el primero es mayor al segundo

# In[ ]:


doctos_des = doctos[doctos['CASOS']== True]


# ## Guardar archivo
# ---

# In[ ]:


nombre_archivo = 'IGXX '+datetime.now().strftime("%d-%m-%y_%Hh%Mm")+'.xlsx'
writer = pd.ExcelWriter(nombre_archivo, engine='xlsxwriter')

doctos_des.to_excel(writer, sheet_name='IGXX')

writer.save()

