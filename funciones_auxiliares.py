import pandas as pd

import matplotlib.pyplot as plt

from functools import reduce

import funciones_auxiliares_hito_2 as aux2

RUTA_2013 = 'input/Egresos_Hospitalarios_2013/Egresos_Hospitalarios_2013.csv'
RUTA_2014 = 'input/Egresos_Hospitalarios_2014-20230216T134804Z-001/Egresos_Hospitalarios_2014/Egresos_Hospitalarios_2014.csv'
RUTA_2015 = 'input/Egresos_Hospitalarios_2015-20230216T124219Z-001/Egresos_Hospitalarios_2015/Egresos_Hospitalarios_2015.csv'
RUTA_2016 = 'input/Egresos_Hospitalarios_2016-20230216T124117Z-001/Egresos_Hospitalarios_2016/Egresos_Hospitalarios_2016.csv'
RUTA_2017 = 'input/Egresos_Hospitalarios_2017-20230216T123902Z-001/Egresos_Hospitalarios_2017/Egresos_Hospitalarios_2017.csv'
RUTA_2018 = 'input/Egresos_Hospitalarios_2018-20230216T123758Z-001/Egresos_Hospitalarios_2018/Egresos_Hospitalarios_2018.csv'
RUTA_2019 = 'input/Egresos_Hospitalarios_2019-20230216T123639Z-001/Egresos_Hospitalarios_2019/Egresos_Hospitalarios_2019.csv'
RUTA_CIE = 'input/CIE-10.xlsx'

ARCHIVOS = {'2013': RUTA_2013,
            '2014': RUTA_2014,
            '2015': RUTA_2015,
            '2016': RUTA_2016,
            '2017': RUTA_2017,
            '2018': RUTA_2018,
            '2019': RUTA_2019}


def convertir_ints_a_strs(valor):
    '''Función que convierte los valores de la columna Región de Los Egresos Hospitalarios. Esta
    columna tiene ints y objects, y los consolida solamente a objects

    :param valor: Es uno de los valores de la columna Región de los egresos Hospitalarios
    :type valor: str/int

    :returns: El valor convertido a string/object
    :rtype: str
    '''
    if isinstance(valor, int):
        if len(str(valor)) == 1:
            return f'0{valor}'
        else:
            return str(valor)

    return valor


def leer_anios_egresos():
    '''Función que lee todos los DataFrames de los egresos contenidos en la variable global 
    ARCHIVOS. Además, convierte la columna REGION_RESIDENCIA a strings.

    :returns: Un diccionario con todos los archivos que estén contenido en la variable ARCHIVOS
    :rtype: dict
    '''
    dfs = {}
    for anio, ruta in ARCHIVOS.items():
        dfs[anio] = pd.read_csv(ruta, delimiter=';', encoding='latin-1', on_bad_lines='skip')
        dfs[anio]['REGION_RESIDENCIA'] = dfs[anio]['REGION_RESIDENCIA'].apply(
            convertir_ints_a_strs)

    return dfs


def graficar_conteo_valores(serie_tiempo):
    '''Funcion que grafica un conteo de los valores distintos de una variable

    :param serie_tiempo: Es la serie que se quiere graficar
    :type serie_tiempo: pd.Series
    '''
    serie_tiempo.plot(kind='bar')
    plt.axhline(serie_tiempo.mean(), color='tomato')
    plt.show()


def analizar_conteo_de_variable(df, variable):
    conteo = df[variable].value_counts()
    display(conteo.to_frame())
    display(conteo.describe().to_frame())
    graficar_conteo_valores(conteo.sort_index())
    aux2.graficar_distribucion_variable_numerica(conteo.reset_index(drop=True), 'egresos')


def analizar_ranking_diagnosticos_hospital(df, glosa_hospital):
    df_hospital = df.query('GLOSA_ESTABLECIMIENTO_SALUD == @glosa_hospital')
    diagnosticos_hospital = df_hospital.GLOSA_DIAG1.value_counts()

    resultados_hospital = []
    for nombre_diagnostico, cantidad_hospital in diagnosticos_hospital.items():
        df_total = df.query('GLOSA_DIAG1 == @nombre_diagnostico')
        cantidad_total = df_total.shape[0]

        df_publico_diagnostico = df.query(
            'GLOSA_DIAG1 == @nombre_diagnostico and PERTENENCIA_ESTABLECIMIENTO_SALUD == "Pertenecientes al Sistema Nacional de Servicios de Salud, SNSS"')
        cantidad_publico = df_publico_diagnostico.shape[0]

        porcentaje_hospital = cantidad_hospital/cantidad_total

        resultados_hospital.append([nombre_diagnostico, cantidad_total, cantidad_hospital,
                                    porcentaje_hospital])

    ranking_hospital = pd.DataFrame(resultados_hospital, columns=['GLOSA_DIAG1', 'TOTAL_NACIONAL',
                                                                  'TOTAL_HOSPITAL', 'PORCENTAJE_ATENDIMIENTO'])

    ranking_hospital = ranking_hospital.sort_values(by='PORCENTAJE_ATENDIMIENTO', ascending=False)

    return ranking_hospital


def obtener_resumen_por_nivel(resumen_por_anio_de_variable, variable_analizada):
    '''Función que describe describe cada uno de los los subgrupos presentes
    en una variable de un DataFrame

    :param resumen_por_anio_de_variable:
    '''
    df_variable_por_anio = resumen_por_anio_de_variable.reset_index(level=variable_analizada)
    for valor in df_variable_por_anio[variable_analizada].unique():
        mask_valor = df_variable_por_anio[variable_analizada] == valor
        df_del_valor = df_variable_por_anio[mask_valor].drop(columns=variable_analizada)
        print(f'El resumen de {valor} es:\n')
        display(df_del_valor.describe())


def analisis_variable_en_el_tiempo(df, variable_a_agrupar):
    '''Función que permite analizar una variable de un DataFrame a lo largo del tiempo.
    El DataFrame debe tener una variable llamada "ANO_EGRESO", que corresponde a una variable
    de tiempo

    :param df: Es el DataFrame que se quiere analizar
    :type df: pd.DataFrame

    :param variable_a_agrupar: Es la variable del DataFrame que se quiere analizar a lo 
    largo del tiempo
    :type variable_a_agrupar: str
    '''
    agrupado = df.groupby(by=['ANO_EGRESO'])[variable_a_agrupar].value_counts()
    agrupado_porcentaje = df.groupby(by=['ANO_EGRESO'])[variable_a_agrupar].value_counts('%')
    juntas = pd.concat([agrupado, agrupado_porcentaje], axis=1)
    juntas.columns = ['CANTIDAD', 'PORCENTAJE']

    agrupado_largo = agrupado.unstack()
    agrupado_largo.plot(kind='bar')

    display(juntas)
    plt.show()

    obtener_resumen_por_nivel(juntas, variable_a_agrupar)

    return juntas


def cambiar_cie_con_x(valor):
    if len(valor) == 3:
        return f'{valor}X'

    return valor


def tratar_columna_codigo(df):
    tmp = df.copy()

    tmp['CodigoSinPunto'] = tmp['Código'].str.replace('.', '', regex=False)
    tmp['CodigoSinPunto'] = tmp['CodigoSinPunto'].apply(cambiar_cie_con_x)

    return tmp


def obtener_diccionario_cie():
    cie = pd.read_excel(RUTA_CIE)

    cie = tratar_columna_codigo(cie)

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

    df_agrupada = tmp.groupby(agrupar_por).agg(N_Egresos=('TIPO_EDAD', 'sum'),
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
