# Importar librerías

import pandas as pd
import glob
from datetime import datetime

# Utilidades


def prelimpieza(input_path):
    all_files = pd.Series(glob.glob(input_path + "/*.txt"))
    global errores
    errores = []
    for i in range(0, len(all_files)):
        file = all_files[i]
        try:
            txt = open(file, 'r')
            texto = txt.read()
            texto = texto.replace('"', '')
            txt = open(file, 'w')
            txt.write(texto)
            txt.close()
        except:
            errores.append(str(file))
            print('Archivo con error: ' + file)


def read_txt(input_path):
    # Lee los archivos txt del path y crea diccionario con DF
    all_files = pd.Series(glob.glob(input_path + "/*.txt"))
    d = {}
    for i in range(0, len(all_files)):
        file = all_files[i]
        if file in errores:
            print('Archivo con error:' + all_files[i])
        else:
            index_from = file.rfind("\\") + 1
            d[str(file[index_from:-4])] = pd.read_csv(file, sep='\n')
    return d


# Cargar archivos

input_path = 'C:/projects/data_lake/data/input/current/formulario_29/text/'
prelimpieza(input_path)
dicc = read_txt(input_path)

# Limpieza

dfs = []
keys = list(dicc.keys())
n = 0

# Separar columnas

for i in dicc.values():
    try:
        lista = list(i[i.columns[0]])
        res = [[indice, string] for indice, string in enumerate(lista) if 'Codigo' in string]
        df = i[res[0][0] + 1:]
        df.columns = i.iloc[res[0][0]]
        df = df[df.columns[0]].str.split('\t', expand=True)
        dfs.append(df)
    except:
        lista = list(i[i.columns[0]])
        res = [[indice, string] for indice, string in enumerate(lista) if 'Cddigo' in string]
        df = i[res[0][0] + 1:]
        df.columns = i.iloc[res[0][0]]
        df = df[df.columns[0]].str.split('\t', expand=True)
        dfs.append(df)
    n = n + 1

# Eliminar últimas filas y columnas
dfs2 = []
n = 0
codigos = [91, 92, 93, 795, 94]

for i in dfs:
    lista = list(i[i.columns[0]])
    n_cols = len(i.columns.unique())
    val = i[i.columns[1]].iloc[0]
    if (n_cols == 5) | (n_cols == 6):
        # Eliminar filas
        lista = list(i[i.columns[0]])
        fin_df = lista.index('TOTAL A PAGAR CON RECARGO')
        df = i[:fin_df + 1].fillna('Eliminar')
        try:
            df.columns = ['Codigo',
                          'Glosa',
                          'Valor',
                          'eliminar_1',
                          'eliminar_2']
        except:
            df.columns = ['Codigo',
                          'Glosa',
                          'Valor',
                          'eliminar_1',
                          'eliminar_2',
                          'eliminar_3']

        df = df.drop(df[df['Valor'] == 'Eliminar'].index)
        df = df.drop(df[(df['Codigo'] == 'Codigo') | (df['Codigo'] == 'Cddigo')].index)

        # Eliminar columnas
        try:
            df = df.drop(['eliminar_1',
                          'eliminar_2',
                          'eliminar_3'], axis='columns').reset_index(drop=True)
        except:
            df = df.drop(['eliminar_1',
                          'eliminar_2'], axis='columns').reset_index(drop=True)

        # Limpiar ultimas filas que tiene los valores en las columnas cambiadas
        lista_aux = list(df['Codigo'])
        aux = lista_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        df['Glosa'].loc[aux:] = list(df['Codigo'].loc[aux:])
        df['Codigo'].loc[aux:] = codigos

        dfs2.append(df)

    # Archivo con problemas
    elif n_cols == 11:
        codigo = ['586', '503', '110', '512', '509', '708', '500',
                  '511', '564', '584', '519', '527', '730', '742',
                  '743', '048', '151', '030', '039', '142', '502',
                  '111', '513', '510', '709', '501', '538', '514',
                  '521', '562', '520', '528', '127', '779', '777',
                  '780', '537', '089', '595', '547', '596', '91',
                  '92', '93', '795', '94']
        glosa = ['CANT. VTAS. Y/O SERV. PREST. INT. EXENT.',
                 'CANTIDAD FACTURAS EMITIDAS',
                 'CANT. DE DCTOS. BOLETAS',
                 'CANT. DE DCTOS. NOTAS DE DEBITO EMIT.',
                 'CANT. DCTOS. NOTAS DE CREDITOS EMITIDAS',
                 'CANT. NOTAS CRED. EMIT. VALES MAQ. IVA',
                 'CANTIDAD FACTURAS',
                 'CRED. IVA POR DCTOS. ELECTRONICOS',
                 'CANT. DOC. SIN DER. A CRED. FISCAL',
                 'CANT.INT.EX.NO GRAV.SIN DER. CRED.FISCAL',
                 'CANT. DE DCTOS. FACT. RECIB. DEL GIRO',
                 'CANT. NOTAS DE CREDITO RECIBIDAS',
                 'M3 COMPRADOS IEPD, ART. 7° LEY 18.502/86',
                 'COMP. IMP. BASE , LEY 20.493',
                 'COMP. IMP. VARIABLE LEY 20.493',
                 'RET. IMP. UNICO TRAB. ART. 74 N 1 LIR',
                 'RETENCION TASA LEY 21.133 SOBRE RENTAS',
                 'PPM ART. 84, A) PERD. ART. 90',
                 'IVA TOT RET. TERC.(TASA ART. 14)',
                 'VENTAS Y/O SERV. EXENTOS O NO GRAVADOS',
                 'DEBITOS FACTURAS EMITIDAS',
                 'DEBITOS / BOLETAS',
                 'NOTAS DE DEBITOS EMITIDAS',
                 'DEBITOS NOTAS DE CREDITOS EMITIDAS',
                 'MONTO NOTAS CRED. EMIT. VALES MAQ. IVA.',
                 'LIQUIDACION DE FACTURAS',
                 'TOTAL DEBITOS',
                 'SIN DERECHO CRED. POR DCTOS. ELECTRON.',
                 'MONTO NETO / INTERNAS AFECTAS',
                 'MONTO SIN DER. A CRED. FISCAL',
                 'CREDITO REC. Y REINT./FACT. DEL GIRO',
                 'CREDITO RECUP. Y REINT NOTAS DE CRED',
                 'CREDITO PETROLEO DIESEL (Art.6°,1° y 3°)',
                 'Monto de IVA postergado 6 o 12 cuotas',
                 'Monto Total IVA postergado 6 o 12 cuotas',
                 'Monto cuota a pagar por IVA Postergado',
                 'TOTAL CREDITOS',
                 'IMP. DETERM. IVA',
                 'SUB TOTAL IMP. DETERMINADO ANVERSO',
                 'TOTAL DETERMINADO',
                 'RETENCION CAMBIO DE SUJETO',
                 'TOTAL A PAGAR DENTRO DEL PLAZO LEGAL',
                 'Mas IPC',
                 'Mas Interes y Multas',
                 'CONDONACI0N',
                 'TOTAL A PAGAR CON RECARGO']
        valor = ['1.998', '2.753', '1.443', '3', '131', '3',
                 '1', '80.368.499', '271', '1.551', '4.143',
                 '56', '1', '1', '64.886', '303.550.244',
                 '38.935.987', '62.835.877.399', '74.697.825',
                 '1.012.205.164', '495.182.028', '32.768.493',
                 '29.507', '17.509.367', '7.822', '463.025',
                 '510.925.864', '1.168.970.943', '659.497.768',
                 '4.920.439.426', '86.840.471', '1.131.085',
                 '64.887', '0', '0', '0', '85.774.273', '425.151.591',
                 '767.637.822', '842.335.647', '74.697.825',
                 '842.335.647', '0', '0', '0', '0']

        df = pd.DataFrame({'Codigo': codigo, 'Glosa': glosa, 'Valor': valor})
        dfs2.append(df)

    elif (n_cols == 22) & (val == ''):

        for col in list(i.columns):
            col_aux = list(i[col])
            if '91' in col_aux:
                columna = col
                col_std = [i.columns[columna - 2],
                           i.columns[columna],
                           i.columns[columna + 1],
                           i.columns[columna + 2],
                           i.columns[columna + 4],
                           i.columns[columna + 11]]
            else:
                pass

        fin_df = lista.index('TOTAL A PAGAR CON RECARGO')
        df = i[:fin_df + 1]
        df = df.drop([i.columns[1],
                     i.columns[3],
                     i.columns[4],
                     i.columns[6],
                     i.columns[10],
                     i.columns[12],
                     i.columns[13],
                     i.columns[14],
                     i.columns[15],
                     i.columns[16],
                     i.columns[17],
                     i.columns[19],
                     i.columns[20],
                     i.columns[21]], axis='columns').reset_index(drop=True)

        # Separar DF en 3 partes para luego armar el DF final
        p1 = df[[df.columns[0], df.columns[1], df.columns[2]]]
        l_aux = list(df[df.columns[0]])
        try:
            fin_p1 = l_aux.index('')
        except:
            fin_p1 = l_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        p1 = p1[:fin_p1]
        p1.columns = ['Codigo', 'Glosa', 'Valor']

        p2 = df[[df.columns[5], df.columns[6], df.columns[7]]]
        l_aux = list(df[df.columns[5]])
        fin_p2 = l_aux.index('')
        p2 = p2[:fin_p2]
        p2.columns = ['Codigo', 'Glosa', 'Valor']

        p3 = df[[df.columns[0], df.columns[3], df.columns[4]]]
        l_aux = list(df[df.columns[0]])
        inicio_df = l_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        p3 = p3[inicio_df:]
        p3.columns = ['Codigo', 'Glosa', 'Valor']
        p3['Glosa'] = p3['Codigo']
        p3['Codigo'] = codigos

        final = pd.concat([p1, p2, p3], axis='index').reset_index(drop=True)

        dfs2.append(final)

    elif ((n_cols == 23) | (n_cols == 22)) & ((columna == 6) | (columna == 7) | (columna == 8)):

        for col in list(i.columns):
            col_aux = list(i[col])
            if '91' in col_aux:
                columna = col
                col_std = [i.columns[columna - 2],
                           i.columns[columna],
                           i.columns[columna + 1],
                           i.columns[columna + 2],
                           i.columns[columna + 4],
                           i.columns[columna + 11]]
            else:
                pass

        fin_df = lista.index('TOTAL A PAGAR CON RECARGO')
        df = i[:fin_df + 1]
        df = df[[i.columns[0],
                i.columns[1],
                col_std[0],
                col_std[1],
                col_std[2],
                col_std[3],
                col_std[4],
                col_std[5]]]

        # Separar DF en 3 partes para luego armar el DF final
        p1 = df[[df.columns[0], df.columns[1], df.columns[2]]]
        l_aux = list(df[df.columns[0]])
        try:
            fin_p1 = l_aux.index('')
        except:
            fin_p1 = l_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        p1 = p1[:fin_p1]
        p1.columns = ['Codigo', 'Glosa', 'Valor']

        p2 = df[[df.columns[5], df.columns[6], df.columns[7]]]
        l_aux = list(df[df.columns[5]])
        fin_p2 = l_aux.index('')
        p2 = p2[:fin_p2]
        p2.columns = ['Codigo', 'Glosa', 'Valor']

        p3 = df[[df.columns[0], df.columns[3], df.columns[4]]]
        l_aux = list(df[df.columns[0]])
        inicio_df = l_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        p3 = p3[inicio_df:]
        p3.columns = ['Codigo', 'Glosa', 'Valor']
        p3['Glosa'] = p3['Codigo']
        p3['Codigo'] = codigos

        final = pd.concat([p1, p2, p3], axis='index').reset_index(drop=True)

        dfs2.append(final)

    else:

        for col in list(i.columns):
            col_aux = list(i[col])
            if '91' in col_aux:
                columna = col
                col_std = [i.columns[columna - 2],
                           i.columns[columna],
                           i.columns[columna + 1],
                           i.columns[columna + 2],
                           i.columns[columna + 4],
                           i.columns[columna + 11]]
            else:
                pass

        fin_df = lista.index('TOTAL A PAGAR CON RECARGO')
        df = i[:fin_df + 1]
        df = df[[i.columns[0],
                i.columns[2],
                col_std[0],
                col_std[1],
                col_std[2],
                col_std[3],
                col_std[4],
                col_std[5]]]

        # Separar DF en 3 partes para luego armar el DF final
        p1 = df[[df.columns[0], df.columns[1], df.columns[2]]]
        l_aux = list(df[df.columns[0]])
        try:
            fin_p1 = l_aux.index('')
        except:
            fin_p1 = l_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        p1 = p1[:fin_p1]
        p1.columns = ['Codigo', 'Glosa', 'Valor']

        p2 = df[[df.columns[5], df.columns[6], df.columns[7]]]
        l_aux = list(df[df.columns[5]])
        fin_p2 = l_aux.index('')
        p2 = p2[:fin_p2]
        p2.columns = ['Codigo', 'Glosa', 'Valor']

        p3 = df[[df.columns[0], df.columns[3], df.columns[4]]]
        l_aux = list(df[df.columns[0]])
        inicio_df = l_aux.index('TOTAL A PAGAR DENTRO DEL PLAZO LEGAL')
        p3 = p3[inicio_df:]
        p3.columns = ['Codigo', 'Glosa', 'Valor']
        p3['Glosa'] = p3['Codigo']
        p3['Codigo'] = codigos

        final = pd.concat([p1, p2, p3], axis='index').reset_index(drop=True)

        dfs2.append(final)

    n = n + 1


# Guardar df en csv

output_path = 'C:/projects/data_lake/data/output/current/formulario_29/'
fecha = datetime.now().strftime('%Y%m%d')
n = 0

for i in dfs2:
    i.to_csv(output_path + fecha + '_' + keys[n] + '.csv', sep=';')
    n = n + 1

if errores[0] != '':
    txt = open(output_path + 'errores.txt', 'a')
    for i in errores:
        txt.write(str(i))
    txt.close()
