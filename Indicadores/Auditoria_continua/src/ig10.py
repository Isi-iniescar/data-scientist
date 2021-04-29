#!/usr/bin/env python
# coding: utf-8

# # IG10 - Relaciones de parentesco entre colaboradores en un mismo grupo societario
# 
# La finalidad de este indicador es identificar los casos de parientes (padres, hijos, hermanos o cónyuges) contratados en el mismo grupo societario. El riesgo que busca mitigar son posibles conflictos de interés entre parientes.
# 
# ***Fecha creación: 29-12-2020***

# ## Importar librerías

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import string


# ## Cargar datos

# In[2]:


# Ruta en donde se encuentran los archivos a cargar
input_path = 'C:/Users/ISINIESCARC/Desktop/AC_RCR-202012/'

# Ruta en donde se deja el resultado del indicador
output_path = 'C:/Users/ISINIESCARC/Desktop/AC_RCR-202012/'


# In[3]:


colab = pd.read_excel(input_path + 'Colaboradores.xlsx', sheet_name='Colaboradores')
equifax = pd.read_excel(input_path + 'Equifax- colaboradores.xlsx', sheet_name='Colaboradores')


# ## Limpieza

# **Establecer mismo formato de RUT**

# In[4]:


equifax['rutid_clean'] = ''
equifax['rutid_familiar_clean'] = ''


# In[5]:


# limpiar rutid: se quita el 0 del inicio
n = 0

for i in equifax['rutid']:
    aux = i[-9:]
    equifax['rutid_clean'][n] = aux
    n = n + 1


# In[6]:


# limpiar rutid_familiar: se quita el 0 del inicio
n = 0

for i in equifax['rutid_familiar']:
    aux = i[-9:]
    equifax['rutid_familiar_clean'][n] = aux
    n = n + 1


# In[7]:


# Se reordenan las columnas y se eliminan aquellas con los rut no limpios
equifax = equifax[['rutid_clean',
                   'nombre_completo',
                   'cargo',
                   'entidad',
                   'rutid_familiar_clean',
                   'nombre_completo_familiar',
                   'parentesco',
                   'defuncion']]


# In[8]:


# Se elimina el símbolo '-' del rut
colab['RUT'] = colab['RUT'].str.replace('-','')


# **Filtrar por familiares vivos**

# In[10]:


equifax = equifax[equifax['defuncion'] == 'NO']


# **Filtrar por colaboradores vigentes a partir de Abril 2020**

# In[11]:


colab['FECHA_DE_BAJA'] = colab['FECHA_DE_BAJA'].fillna(0)
colab['FEC_FIN_CONTRATO'] = colab['FEC_FIN_CONTRATO'].fillna(0)


# In[12]:


# Se crea una columna con la vigencia del colaborador.
# Si su fecha de baja en menor a la fecha de hoy, entonces no está vigente

hoy = date.today()
colab['Estado'] = ''
n = 0

for i in colab['FECHA_DE_BAJA']:
    if i != 0:
        if i < hoy:
            colab['Estado'][n] = 'No vigente'
    else: 
        colab['Estado'][n] = 'Vigente'
    n = n + 1


# In[13]:


# Formato fecha: año, mes, día
abril = datetime(2020, 4, 1)

# Filtro para obtener colaboradores vigentes que hayan ingresado después de la fecha establecida
colab_vig = colab[(colab['Estado']=='Vigente') & (colab['FEC_INGRESO'] >= abril)].reset_index(drop=True)


# ## Ejecución de indicador

# **Buscar si existen familiares trabajando en RCR a partir de Abril 2020**

# In[14]:


# Hacemos el cruce entre los familiares de equifax y colaboradores vigentes
aux_fam = pd.merge(equifax, colab_vig, left_on='rutid_familiar_clean', right_on='RUT', how='inner')


# In[15]:


# Reordenamos y renombramos las columnas
aux_fam = aux_fam[['rutid_clean',
                   'nombre_completo',
                   'rutid_familiar_clean',
                   'nombre_completo_familiar',
                   'CARGO',
                   'parentesco',
                   'GRUPO',
                   'SOCIEDAD',
                   'FEC_INGRESO',
                   'FECHA_DE_BAJA',
                   'TIPO_CONTRATO',
                   'FEC_FIN_CONTRATO']]

aux_fam.columns = ['rut_colaborador',
                   'nombre_colaborador',
                   'rut_familiar',
                   'nombre_familiar',
                   'cargo_familiar',
                   'parentesco',
                   'grupo',
                   'sociedad',
                   'fecha_ingreso',
                   'fecha_baja',
                   'tipo_contrato',
                   'fecha_fin_contrato']


# **Incluimos información sobre grupo y sociedad del colaborador, independiente de la fecha de contratación de este**

# In[16]:


# Creamos un DF auxiliar del original de colaboradores, solo con las columnas que nos interesan
colab_extract = colab[['GRUPO',
                       'RUT',
                       'SOCIEDAD',
                       'CARGO',
                       'FEC_INGRESO',
                       'FECHA_DE_BAJA',
                       'TIPO_CONTRATO',
                       'FEC_FIN_CONTRATO']]


# In[17]:


# Cruzamos el resultado obtenido de los familiares contratados desde abril 2020 con los colaboradores.
# Esto para obtener la información del colaborador asociado al familiar encontrado
ind_1 = pd.merge(aux_fam, colab_extract, left_on='rut_colaborador', right_on='RUT', how='inner')

# Reordenamos y renombramos las columnas
ind_1 = ind_1[['rut_colaborador',
               'nombre_colaborador',
               'CARGO',
               'GRUPO',
               'SOCIEDAD',
               'FEC_INGRESO',
               'FECHA_DE_BAJA',
               'TIPO_CONTRATO',
               'FEC_FIN_CONTRATO',
               'rut_familiar',
               'nombre_familiar',
               'cargo_familiar',
               'parentesco',
               'grupo',
               'sociedad',
               'fecha_ingreso',
               'fecha_baja',
               'tipo_contrato',
               'fecha_fin_contrato']]

ind_1.columns = ['rut_colaborador',
                 'nombre_colaborador',
                 'cargo_colaborador',
                 'grupo_colaborador',
                 'sociedad_colaborador',
                 'fecha_ingreso_colab',
                 'fecha_baja_colab',
                 'tipo_contrato_colab',
                 'fecha_fin_contrato_colab',
                 'rut_familiar',
                 'nombre_familiar',
                 'cargo_familiar',
                 'parentesco',
                 'grupo_familiar',
                 'sociedad_familiar',
                 'fecha_ingreso_fam',
                 'fecha_baja_fam',
                 'tipo_contrato_fam',
                 'fecha_fin_contrato_fam']


# **Filtramos por aquellos colaboradores y parientes que trabajan en la misma sociedad**

# In[18]:


ind_1 = ind_1[ind_1['grupo_colaborador'] == ind_1['grupo_familiar']].reset_index(drop=True)


# ## Guardar información

# In[20]:


nombre_archivo = 'RCR-ind_1_' + datetime.now().strftime("%d-%m-%y_%Hh%Mm") + '.xlsx'
writer = pd.ExcelWriter(output_path + nombre_archivo, engine='xlsxwriter')

ind_1.to_excel(writer, sheet_name='Indicador_1')

writer.save()

