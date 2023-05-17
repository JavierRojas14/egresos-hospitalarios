"""
This module contains functions for data processing and analysis.

It utilizes the 'polars' library for working with DataFrames.

Module Constants:
    DICT_VARIABLES (dict): A dictionary mapping variable names to their corresponding data types.
    MAPPING_METRICAS_EGRESOS (dict): A dictionary mapping column names to their mapping dictionaries 
    for column mapping.
    MAPPING_SOCIODEMOGRAFICO (dict): A dictionary mapping column names to their mapping dictionaries 
    for sociodemographic analysis.

Module Functions:
    - mappear_columnas: Map columns in the DataFrame based on a mapping dictionary.
    - obtener_diagnosticos_unicos_de_hospital: Get unique diagnoses from a specific hospital in the 
    DataFrame.
    - leer_archivos: Read and process the DEIS's public databases from the input directory, 
    filtering by a specific hospital.
    - leer_cie: Read the CIE-10 codes from an Excel file and preprocess them.
    - agregar_columnas_localizacion: Add location-related columns to the DataFrame based on 
    region and comuna information.
    - agregar_categorizacion_edad: Add age category column to the DataFrame based on age in years.
    - obtener_df_inicial_sociodemografico: Obtain the initial sociodemographic DataFrame for a 
    specific hospital.
"""


import polars as pl

DICT_VARIABLES = {
    "ESTABLECIMIENTO_SALUD": pl.Int32,
    "GLOSA_ESTABLECIMIENTO_SALUD": pl.Categorical,
    "PERTENENCIA_ESTABLECIMIENTO_SALUD": pl.Categorical,
    "SEREMI": pl.Int8,
    "SERVICIO_DE_SALUD": pl.Int8,
    "SEXO": pl.Int8,
    "EDAD_CANT": pl.Int8,
    "TIPO_EDAD": pl.Int8,
    "EDAD_A_OS": pl.Int8,
    "PUEBLO_ORIGINARIO": pl.Int8,
    "PAIS_ORIGEN": pl.Int16,
    "GLOSA_COMUNA_RESIDENCIA": pl.Categorical,
    "REGION_RESIDENCIA": pl.Categorical,
    "GLOSA_REGION_RESIDENCIA": pl.Categorical,
    "PREVISION": pl.Int8,
    "BENEFICIARIO": pl.Categorical,
    "MODALIDAD": pl.Int8,
    "PROCEDENCIA": pl.Int8,
    "ANO_EGRESO": pl.Int16,
    "FECHA_EGRESO": pl.Date,
    "AREA_FUNCIONAL_EGRESO": pl.Int16,
    "DIAS_ESTADA": pl.Int16,
    "CONDICION_EGRESO": pl.Int8,
    "INTERV_Q": pl.Int8,
    "CODIGO_INTERV_Q_PPAL": pl.Int32,
    "PROCED": pl.Categorical,
    "CODIGO_PROCED_PPAL": pl.Categorical,
    "GLOSA_PROCED_PPAL": pl.Categorical,
}

MAPPING_METRICAS_EGRESOS = {"INTERV_Q": {2: 0}, "CONDICION_EGRESO": {1: 0, 2: 1}}


MAPPING_SOCIODEMOGRAFICO = {
    "GLOSA_REGION_RESIDENCIA": {
        "Del Libertador B. O'Higgins": "del Libertador General Bernardo O'Higgins",
        "De Aisén del Gral. C. Ibáñez del Campo": "Aysén del General Carlos Ibáñez del Campo",
    },
    "SEXO": {1: "Hombre", 2: "Mujer", 3: "Intersex", 99: "Desconocido"},
    "PUEBLO_ORIGINARIO": {
        1: "MAPUCHE",
        2: "AYMARA",
        3: "RAPA NUI (PASCUENSE)",
        4: "LICAN ANTAI",
        5: "QUECHUA",
        6: "COLLA",
        7: "DIAGUITA",
        8: "KAWÉSQAR",
        9: "YAGÁN (YÁMANA)",
        10: "OTRO (ESPECIFICAR)",
        96: "NINGUNO",
    },
    "PREVISION": {
        1: "FONASA",
        2: "ISAPRE",
        3: "CAPREDENA",
        4: "DIPRECA",
        5: "SISA",
        96: "NINGUNA",
        99: "DESCONOCIDO",
    },
}


def mappear_columnas(df, dict_mapeo):
    """
    Map columns in the DataFrame based on a mapping dictionary.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :param dict_mapeo: A dictionary containing the mapping for columns.
    :type dict_mapeo: dict

    :return: The DataFrame with mapped columns.
    :rtype: pl.DataFrame
    """
    tmp = df.clone()

    for variable, dict_cambio in dict_mapeo.items():
        tmp = tmp.with_columns(pl.col(variable).map_dict(dict_cambio, default=pl.col(variable)))

    return tmp


def obtener_diagnosticos_unicos_de_hospital(df, hospital_a_analizar):
    """
    Get unique diagnoses from a specific hospital in the DataFrame.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :param hospital_a_analizar: The ID of the hospital to analyze.
    :type hospital_a_analizar: int

    :return: A series containing the unique diagnoses.
    :rtype: pl.Series
    """
    diags_hospital = (
        df.filter(pl.col("ESTABLECIMIENTO_SALUD") == hospital_a_analizar)
        .select(pl.col("DIAG1"))
        .unique()
    )

    return diags_hospital


def leer_archivos(filtro_hospital=11203):
    """
    Read and process the DEIS's public databases from the input directory, filtering by a specific
    hospital.

    :param filtro_hospital: The ID of the hospital to filter by. Defaults to 11203
    (Instituto Nacional del Torax's code).
    :type filtro_hospital: int, optional

    :return: The processed DataFrame filtered by the specified hospital.
    :rtype: pl.DataFrame
    """
    with pl.StringCache():
        df_nacional = pl.scan_csv("input/utf-8/*.csv", separator=";")
        diags_torax = (
            obtener_diagnosticos_unicos_de_hospital(df_nacional, filtro_hospital).collect(
                streaming=True
            )
        ).to_series()

        df = df_nacional.filter(pl.col("DIAG1").is_in(diags_torax))
        df = mappear_columnas(df, MAPPING_METRICAS_EGRESOS)

        return df


def leer_cie():
    """
    Read the CIE-10 codes from an Excel file and preprocess them.

    :return: The processed CIE-10 codes DataFrame.
    :rtype: pl.DataFrame
    """
    cie = pl.read_excel("input/CIE-10.xlsx").with_columns(
        pl.col("Código").str.replace(".", "", literal=True).str.ljust(4, "X").alias("DIAG1")
    )

    cie = cie.drop(["Código", "Versión"])

    return cie


# ANALISIS SOCIODEMOGRAFICO


def agregar_columnas_localizacion(df):
    """
    Add location-related columns to the DataFrame based on region and comuna information.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :return: The DataFrame with added location-related columns.
    :rtype: pl.DataFrame
    """
    tmp = df.with_columns(
        ("Region " + pl.col("GLOSA_REGION_RESIDENCIA") + ", Chile").alias("region_pais")
    )

    tmp = tmp.with_columns(
        (pl.col("GLOSA_COMUNA_RESIDENCIA") + ", " + pl.col("region_pais")).alias(
            "comuna_region_pais"
        )
    )

    return tmp


def agregar_categorizacion_edad(df):
    """
    Add age category column to the DataFrame based on age in years.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :return: The DataFrame with added age category column.
    :rtype: pl.DataFrame
    """
    tmp = df.with_columns(
        (df.get_column("EDAD_A_OS"))
        .cut(bins=range(0, 101, 10), maintain_order=True)
        .select(pl.col("category").alias("EDAD_CATEGORIA"))
    )

    return tmp


def obtener_df_inicial_sociodemografico(filtro_hospital=112103):
    """
    Obtain the initial sociodemographic DataFrame for a specific hospital.

    :param filtro_hospital: The ID of the hospital to filter by. Defaults to 112103
    (Instituto Nacional del Torax's code).
    :type filtro_hospital: int, optional

    :return: The initial sociodemographic DataFrame for the specified hospital.
    :rtype: pl.DataFrame
    """
    with pl.StringCache():
        df_socio = pl.scan_csv("input/utf-8/*.csv", separator=";", dtypes=DICT_VARIABLES).filter(
            pl.col("ESTABLECIMIENTO_SALUD") == filtro_hospital
        )

        df_socio = mappear_columnas(df_socio, MAPPING_SOCIODEMOGRAFICO)
        df_socio = agregar_columnas_localizacion(df_socio).collect()
        df_socio = agregar_categorizacion_edad(df_socio)

    return df_socio
