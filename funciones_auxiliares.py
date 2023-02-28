import pandas as pd

import matplotlib.pyplot as plt

import funciones_auxiliares_hito_2 as aux2

RUTA_2013 = 'egresos/Egresos_Hospitalarios_2013/Egresos_Hospitalarios_2013.csv'
RUTA_2014 = 'egresos/Egresos_Hospitalarios_2014-20230216T134804Z-001/Egresos_Hospitalarios_2014/Egresos_Hospitalarios_2014.csv'
RUTA_2015 = 'egresos/Egresos_Hospitalarios_2015-20230216T124219Z-001/Egresos_Hospitalarios_2015/Egresos_Hospitalarios_2015.csv'
RUTA_2016 = 'egresos/Egresos_Hospitalarios_2016-20230216T124117Z-001/Egresos_Hospitalarios_2016/Egresos_Hospitalarios_2016.csv'
RUTA_2017 = 'egresos/Egresos_Hospitalarios_2017-20230216T123902Z-001/Egresos_Hospitalarios_2017/Egresos_Hospitalarios_2017.csv'
RUTA_2018 = 'egresos/Egresos_Hospitalarios_2018-20230216T123758Z-001/Egresos_Hospitalarios_2018/Egresos_Hospitalarios_2018.csv'
RUTA_2019 = 'egresos/Egresos_Hospitalarios_2019-20230216T123639Z-001/Egresos_Hospitalarios_2019/Egresos_Hospitalarios_2019.csv'

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

        porcentaje_hospital = cantidad_hospital/cantidad_total

        resultados_hospital.append([nombre_diagnostico, cantidad_total, cantidad_hospital,
                                    porcentaje_hospital])

    ranking_hospital = pd.DataFrame(resultados_hospital, columns=['GLOSA_DIAG1', 'TOTAL_NACIONAL',
                                                                  'TOTAL_HOSPITAL', 'PORCENTAJE_ATENDIMIENTO'])

    ranking_hospital = ranking_hospital.sort_values(by='PORCENTAJE_ATENDIMIENTO', ascending=False)

    return ranking_hospital


def obtener_resumen_por_nivel(resumen_por_anio_de_variable, variable_analizada):
    df_variable_por_anio = resumen_por_anio_de_variable.reset_index(level=variable_analizada)
    for valor in df_variable_por_anio[variable_analizada].unique():
        mask_valor = df_variable_por_anio[variable_analizada] == valor
        df_del_valor = df_variable_por_anio[mask_valor].drop(columns=variable_analizada)
        print(f'El resumen de {valor} es:\n')
        display(df_del_valor.describe())


def analisis_grafico_anidado_por_anio(df, variable_a_agrupar):
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
