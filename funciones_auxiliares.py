import pandas as pd

import matplotlib.pyplot as plt

from functools import reduce

import funciones_auxiliares_hito_2 as aux2

RUTA_CIE = 'input/CIE-10.xlsx'


LEER_ANIO_INICIO = 2001
LEER_ANIO_FINAL = 2020
ARCHIVOS_A_LEER = [f'input/Egresos_Hospitalarios_{i}.csv' for i in range(LEER_ANIO_INICIO,
                                                                         LEER_ANIO_FINAL + 1)]

DICT_ENCODE_VARIABLES = {
    'ID_PACIENTE': 'object',
    'ESTABLECIMIENTO_SALUD': 'Int32',
    'GLOSA_ESTABLECIMIENTO_SALUD': 'category',
    'PERTENENCIA_ESTABLECIMIENTO_SALUD': 'category',
    'SEREMI': 'Int8',
    'SERVICIO_DE_SALUD': 'Int8',
    'SEXO': 'Int8',
    'EDAD_CANT': 'Int8',
    'TIPO_EDAD': 'Int8',
    'EDAD_A_OS': 'Int8',
    'PUEBLO_ORIGINARIO': 'Int8',
    'PAIS_ORIGEN': 'Int16',
    'GLOSA_COMUNA_RESIDENCIA': 'category',
    'REGION_RESIDENCIA': 'category',
    'GLOSA_REGION_RESIDENCIA': 'category',
    'PREVISION': 'Int8',
    'BENEFICIARIO': 'category',
    'MODALIDAD': 'Int8',
    'PROCEDENCIA': 'Int8',
    'ANO_EGRESO': 'Int16',
    'FECHA_EGRESO': 'object',
    'AREA_FUNCIONAL_EGRESO': 'Int16',
    # 'DIAS_ESTADA': 'Int16',
    'CONDICION_EGRESO': 'Int8',
    'DIAG1': 'object',
    'GLOSA_DIAG1': 'object',
    'DIAG2': 'object',
    'GLOSA_DIAG2': 'object',
    'INTERV_Q': 'Int8',
    'CODIGO_INTERV_Q_PPAL': 'Int32',
    'GLOSA_INTERV_Q_PPAL': 'object'
}


def lectura_archivos():
    dfs_archivos = (pd.read_csv(f, encoding='latin-1', delimiter=';', on_bad_lines='warn',
                                usecols=[i for i in range(34)],
                                dtype=DICT_ENCODE_VARIABLES) for f in ARCHIVOS_A_LEER)

    df = pd.concat(dfs_archivos)

    return df


def cambiar_cie_con_x(valor):
    '''Funcion que cambia los codigos CIE con largo 3 (Ej: A22), por la nomenclatura
    de 4 caracteres (Ej: A22X). Si es un codigo CIE de largo 4, entonces se deja como esta

    :param valor: Codigo CIE-10 que se quiere cambiar. Puede tener largo 3 o 4
    :type valor: str

    :returns: El Codigo CIE-10 con largo 4
    :rtype: str
    '''
    if len(valor) == 3:
        return f'{valor}X'

    return valor


def tratar_columna_codigo_cie(df):
    '''Funcion que modifica la columna de código CIE-10 del diccionario CIE-10 de FONASA.
    Quita el punto de los código, y agrega X a los códigos de largo 3.

    :param df: Es el DataFrame diccionario de CIE-10
    :type df: pd.DataFrame

    :returns: El diccionario CIE-10 con la columna de códigos CIE-10 sin puntos y todos con largo 4
    :rtype: pd.DataFrame
    '''
    tmp = df.copy()

    tmp['CodigoSinPunto'] = tmp['Código'].str.replace('.', '', regex=False)
    tmp['CodigoSinPunto'] = tmp['CodigoSinPunto'].apply(cambiar_cie_con_x)

    return tmp


def obtener_diccionario_cie():
    cie = pd.read_excel(RUTA_CIE)

    cie = tratar_columna_codigo_cie(cie)

    return cie


def aplanar_columnas(df):
    tmp = df.copy()

    tmp.columns = ['_'.join(col).strip('_') for col in tmp.columns]

    return tmp


def obtener_diagnosticos_hospital(df, glosa):
    diagnosticos_hospital = df.query('GLOSA_ESTABLECIMIENTO_SALUD == @glosa')

    return diagnosticos_hospital['DIAG1'].unique()


def calcular_porcentaje_metrica_por_diagnostico(df, subgrupo_ranking, variable_a_analizar):
    return df[variable_a_analizar] / df.groupby(subgrupo_ranking)[variable_a_analizar].transform('sum')


def obtener_ranking_total(df, agrupar_por, subgrupo_ranking, variable_a_analizar):
    tmp = df.copy()

    df_agrupada = tmp.groupby(agrupar_por).agg(N_Egresos=('PERTENENCIA_ESTABLECIMIENTO_SALUD', 'count'),
                                               DIAS_ESTADA_Promedio=('DIAS_ESTADA', 'mean'))

    orden_ranking = subgrupo_ranking + variable_a_analizar
    df_agrupada = df_agrupada.sort_values(orden_ranking, ascending=False)

    df_agrupada['%_Egresos'] = calcular_porcentaje_metrica_por_diagnostico(
        df_agrupada, subgrupo_ranking, 'N_Egresos')

    df_agrupada['%_DIAS_ESTADA_Promedio'] = calcular_porcentaje_metrica_por_diagnostico(
        df_agrupada, subgrupo_ranking, 'DIAS_ESTADA_Promedio')

    orden_columnas = ['N_Egresos', '%_Egresos', 'DIAS_ESTADA_Promedio', '%_DIAS_ESTADA_Promedio']

    df_agrupada = df_agrupada[orden_columnas]

    df_agrupada = df_agrupada.reset_index()

    return df_agrupada


def obtener_tabla_posicion_hospital_en_ranking(ranking, glosa_hospital, subgrupo_ranking):
    tmp = ranking.copy()

    tmp['Ranking'] = (tmp.groupby(subgrupo_ranking).cumcount()) + 1
    tmp = tmp.query('GLOSA_ESTABLECIMIENTO_SALUD == @glosa_hospital')
    tmp = tmp.sort_values('Ranking').reset_index(drop=True)

    return tmp


def analizar_ranking_hospital(df, hospital_a_analizar, agrupar_por, subgrupo_ranking, variable_a_analizar):
    tabla_ranking_global = obtener_ranking_total(
        df, agrupar_por, subgrupo_ranking, variable_a_analizar)
    tabla_posicion_hospital = obtener_tabla_posicion_hospital_en_ranking(tabla_ranking_global,
                                                                         hospital_a_analizar,
                                                                         subgrupo_ranking)

    return tabla_ranking_global, tabla_posicion_hospital


def agregar_sufijo_especifico(dfs, sufijos):
    tmps = dfs.copy()

    for i in range(len(tmps)):
        tmps[i] = tmps[i].add_suffix(sufijos[i])

    return tmps


def unir_tablas_de_posicion_por_diagnostico(diccionario_tablas_posicion, unir_en):
    dfs_con_indice = list(map(lambda x: x.set_index(unir_en), diccionario_tablas_posicion.values()))
    dfs_con_sufijo = agregar_sufijo_especifico(dfs_con_indice,
                                               list(diccionario_tablas_posicion.keys()))

    unidas = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                 how='outer'), dfs_con_sufijo)

    unidas = unidas.reset_index()

    return unidas
