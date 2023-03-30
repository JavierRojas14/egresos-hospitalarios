import pandas as pd

import matplotlib.pyplot as plt

from functools import reduce

RUTA_CIE = 'input/CIE-10.xlsx'


LEER_ANIO_INICIO = 2019
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
    '''Funcion que lee los archivos de Egresos Hospitalarios en la carpeta input. Para leer los años
    se utilizan las variables globales LEER_ANIO_INICIO y LEER_ANIO_FINAL. Además, utiliza el
    diccionario global para asignar el tipo de dato para cada columna.

    :returns: El DataFrame concatenado de los años a analizar en Egresos Hospitalarios
    :rtype: pd.DataFrame
    '''
    dfs_archivos = (pd.read_csv(f, encoding='latin-1', delimiter=';', on_bad_lines='warn',
                                usecols=[i for i in range(34)],
                                dtype=DICT_ENCODE_VARIABLES) for f in ARCHIVOS_A_LEER)

    df = pd.concat(dfs_archivos)
    df['INTERV_Q'] = df['INTERV_Q'].replace({2: 0})
    df['CONDICION_EGRESO'] = df['CONDICION_EGRESO'].replace({1: 0, 2: 1})

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
    '''Funcion que obtiene el diccionario CIE-10 de FONASA. La funcion trata la columna de Codigo
    CIE-10 para dejarlos todos sin punto y con largo 4.

    :returns: El DataFrame del diccionario CIE-10
    :rtype: pd.DataFrame
    '''
    cie = pd.read_excel(RUTA_CIE)

    cie = tratar_columna_codigo_cie(cie)

    return cie


def obtener_diagnosticos_hospital(df, glosa):
    '''Funcion que permite obtener los diagnosticos unicos de un hospital en especifico. Se
    especifica la glosa del hospital que se quieren buscar los datos.

    :param df: El DataFrame donde se quieren buscar los diagnosticos del hospital
    :type df: pd.DataFrame

    :param glosa: Glosa del Hospital que se quieren buscar los diagnosticos unicos
    :type glosa: str

    :returns: Una lista con los diagnosticos unicos que posee el Hospital
    :rtype: list
    '''
    diagnosticos_hospital = df.query('GLOSA_ESTABLECIMIENTO_SALUD == @glosa')

    return diagnosticos_hospital['DIAG1'].unique()


def calcular_porcentaje_metrica_por_diagnostico(df, subgrupo_ranking, variable_a_analizar):
    '''Funcion que calcula el porcentaje para cada valor de una columna (indicada por el
    argumento variable a analizar). El total utilizado para calculcar el porcentaje 
    corresponde a el subgrupo del DataFrame indicado por la variable subgrupo_ranking

    :param df: Es el DataFrame que contiene la variable que se le quiere calcular el porcentaje
    a sus valores, y que contiene los subgrupos
    :type df: pd.DataFrame

    :param subgrupo_ranking: Contiene las variables a utilizar para generar cada grupo que
    constituira el 100% o el total del conteo para la variable a analizar. Ej: ANO_EGRESO y DIAG_1
    :type subgrupo_ranking: list

    :param variable_a_analizar: Es la variable en donde se quiere calcular el porcentaje para cada
    uno de sus valores
    :type variable_a_analizar: str

    :returns: Un array con todos los porcentajes para cada valor de la variable analizada
    :rtype: pd.Series
    '''
    return (df[variable_a_analizar] /
            df.groupby(subgrupo_ranking)[variable_a_analizar].transform('sum'))


def calcular_metricas_de_egresos_agrupados(df, agrupar_por, subgrupo_ranking, variable_a_analizar):
    '''Funcion que 
    '''
    tmp = df.copy()

    df_agrupada = tmp.groupby(agrupar_por).agg(N_Egresos=('DIAG1', 'count'),
                                               DIAS_ESTADA_Promedio=('DIAS_ESTADA', 'mean'),
                                               N_Int_Q=('INTERV_Q', 'sum'),
                                               N_Muertos=('CONDICION_EGRESO', 'sum'))

    orden_ranking = subgrupo_ranking + variable_a_analizar
    df_agrupada = df_agrupada.sort_values(orden_ranking, ascending=False)

    for columna in df_agrupada.columns:
        df_agrupada[f'%_{columna}'] = calcular_porcentaje_metrica_por_diagnostico(
            df_agrupada, subgrupo_ranking, columna
        )

    df_agrupada = df_agrupada.reset_index()

    return df_agrupada


def obtener_tabla_posicion_hospital_en_ranking(ranking, glosa_hospital, subgrupo_ranking):
    tmp = ranking.copy()

    tmp['Ranking'] = (tmp.groupby(subgrupo_ranking).cumcount()) + 1
    tmp = tmp.query('GLOSA_ESTABLECIMIENTO_SALUD == @glosa_hospital')
    tmp = tmp.sort_values('Ranking').reset_index(drop=True)

    return tmp


def analizar_ranking_hospital(df, hospital_a_analizar, agrupar_por, subgrupo_ranking, variable_a_analizar):
    tabla_ranking_global = calcular_metricas_de_egresos_agrupados(
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
