import pandas as pd

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
