"""
This module provides functions for analyzing DEIS's public databases for hospital discharges in 
Chile using the Polars library.

Module Constants:
    - PERTENECE_SNSS: Represents "Pertenecientes al Sistema Nacional de Servicios de Salud, SNSS".
    - NO_PERTENECE_SNSS: Represents "No Pertenecientes al Sistema Nacional de Servicios de Salud, 
    SNSS".
    - HOSPITALES_GRD: A list of hospital codes.

Module Functions:
    - obtener_metricas_egresos: Calculates metrics for hospital discharges, such as the number of
    discharges, total length of stay, surgical interventions, and number of deaths per diagnosis
    at the specified aggregation level.
    - obtener_diccionario_estratos: Obtains a dictionary of hospitals belonging to different 
    Chilean strata, including public and private hospitals, national hospital codes, 'grd' 
    hospitals, and the hospital being analyzed.
    - obtener_metricas_para_un_estrato: Obtains metrics for a specific stratum in the DataFrame 
    based on the analysis variable and ranking subgroup.
    - obtener_resumen_por_estratos: Obtains a summary of metrics for different strata in the 
    DataFrame based on provided dictionaries, variables to rank, and ranking subgroup.
    - left_join_consecutivo: Performs a left join operation on two DataFrames based on a specified
    column.
"""

import polars as pl
import pandas as pd


PERTENECE_SNSS = "Pertenecientes al Sistema Nacional de Servicios de Salud, SNSS"
NO_PERTENECE_SNSS = "No Pertenecientes al Sistema Nacional de Servicios de Salud, SNSS"
HOSPITALES_GRD = [
    118100,
    110100,
    115100,
    121117,
    103100,
    116110,
    119100,
    113100,
    114101,
    105101,
    116108,
    116105,
    101100,
    114105,
    105100,
    112102,
    133150,
    126100,
    121110,
    121114,
    129106,
    113150,
    107100,
    106100,
    113130,
    112100,
    121121,
    109100,
    106103,
    113180,
    122100,
    123100,
    107102,
    110120,
    105102,
    111101,
    111100,
    108101,
    124105,
    128109,
    109101,
    114103,
    102100,
    103101,
    120101,
    117101,
    121109,
    112101,
    104103,
    115107,
    107101,
    110130,
    116100,
    118105,
    115110,
    112103,
    104100,
    108100,
    112104,
    117102,
    106102,
    111195,
    129100,
    110150,
    125100,
]


UNIR_EN = [
    "ANO_EGRESO",
    "ESTABLECIMIENTO_SALUD",
    "GLOSA_ESTABLECIMIENTO_SALUD",
    "DIAG1",
    "n_egresos",
    "dias_estada_totales",
    "n_int_q",
    "n_muertos",
]


def obtener_metricas_egresos(df, agrupar_por):
    """
    Calculates the number of discharges, total length of stay, surgical interventions,
    and number of deaths per diagnosis. This calculation is performed at the specified
    aggregation level.

    :param df: The hospital discharge data table to be analyzed.
    :type df: pl.DataFrame or pl.LazyFrame

    :param agrupar_por: The grouping level to work with.
    :type agrupar_por: str

    :returns: Returns a DataFrame with metrics including the number of discharges,
    total length of stay, surgical interventions, and number of deaths per diagnosis
    and grouping level.
    :rtype: pl.DataFrame or pl.LazyFrame
    """
    metricas_agregadas = df.groupby(agrupar_por).agg(
        [
            pl.col("DIAG1").count().alias("n_egresos"),
            pl.col("DIAS_ESTADA").sum().alias("dias_estada_totales"),
            pl.col("INTERV_Q").sum().alias("n_int_q"),
            pl.col("CONDICION_EGRESO").sum().alias("n_muertos"),
        ]
    )

    return metricas_agregadas


def obtener_diccionario_estratos(df_nacional, hospital_interno):
    """
    Function that obtains a dictionary of hospitals belonging to different Chilean strata.
    The strata are: Public Hospitals and Private Hospitals. Additionally, it includes
    national hospital codes, 'grd' hospitals, and the hospital being analyzed in the ranking.

    :param df_nacional: The input DataFrame containing national hospital data.
    :type df_nacional: pl.DataFrame

    :param hospital_interno: The ID of the internal hospital to analyze.
    :type hospital_interno: int

    :return: A dictionary of hospitals belonging to different strata.
    :rtype: dict
    """
    df_publicos = df_nacional.filter(pl.col("PERTENENCIA_ESTABLECIMIENTO_SALUD") == PERTENECE_SNSS)
    df_privados = df_nacional.filter(
        (pl.col("PERTENENCIA_ESTABLECIMIENTO_SALUD") == NO_PERTENECE_SNSS)
        | (pl.col("ESTABLECIMIENTO_SALUD") == hospital_interno)
    )

    codigos_nacionales = (
        df_nacional.select(pl.col("ESTABLECIMIENTO_SALUD")).unique().collect(streaming=True)
    )
    codigos_publicos = (
        df_publicos.select(pl.col("ESTABLECIMIENTO_SALUD")).unique().collect(streaming=True)
    )
    codigos_privados = (
        df_privados.select(pl.col("ESTABLECIMIENTO_SALUD")).unique().collect(streaming=True)
    )

    diccionario_estratos = {
        "nacionales": codigos_nacionales.to_series(),
        "publicos": codigos_publicos.to_series(),
        "privados": codigos_privados.to_series(),
        "grd": HOSPITALES_GRD,
        "interno": [hospital_interno],
    }

    return diccionario_estratos


def obtener_metricas_para_un_estrato(df, glosa_estrato, variable_analisis, subgrupo_del_ranking):
    """
    Obtain metrics for a specific stratum in the DataFrame based on the analysis variable
    and ranking subgroup.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :param glosa_estrato: The description of the stratum being analyzed.
    :type glosa_estrato: str

    :param variable_analisis: The variable to analyze.
    :type variable_analisis: str

    :param subgrupo_del_ranking: The subgroup for ranking.
    :type subgrupo_del_ranking: list

    :return: A DataFrame with the obtained metrics for the specific stratum.
    :rtype: pl.DataFrame
    """
    if glosa_estrato == "interno":
        subgrupo_del_ranking.remove("DIAG1")

    df = df.sort(subgrupo_del_ranking + [variable_analisis], descending=True)

    sufijo_cols = f"_{glosa_estrato}_{variable_analisis}"
    var_rank = f"ranking{sufijo_cols}"
    var_porc = f"%{sufijo_cols}"
    var_total = f"total{sufijo_cols}"

    resumen = df.with_columns(
        (pl.col("DIAG1").cumcount().over(subgrupo_del_ranking) + 1).alias(var_rank),
        (pl.col("n_egresos").sum().over(subgrupo_del_ranking)).alias(var_total),
    )

    resumen = resumen.with_columns((pl.col("n_egresos") / pl.col(var_total)).alias(var_porc))

    return resumen


def obtener_resumen_por_estratos(df, dict_estratos, variables_a_rankear, subgrupo_del_ranking):
    """
    Obtain a summary of metrics for different strata in the DataFrame based on the provided 
    dictionaries, variables to rank, and ranking subgroup.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :param dict_estratos: A dictionary containing the strata and their respective hospital codes.
    :type dict_estratos: dict

    :param variables_a_rankear: The variables to rank.
    :type variables_a_rankear: list

    :param subgrupo_del_ranking: The subgroup for ranking.
    :type subgrupo_del_ranking: list

    :return: A dictionary with the summary of metrics for different strata.
    :rtype: dict
    """
    for variable_analisis in variables_a_rankear:
        resultado_estrato = {}

        for glosa_estrato, codigos_en_estrato in dict_estratos.items():
            df_estrato = df.filter(pl.col("ESTABLECIMIENTO_SALUD").is_in(codigos_en_estrato))
            resumen = obtener_metricas_para_un_estrato(
                df_estrato, glosa_estrato, variable_analisis, subgrupo_del_ranking
            )

            resultado_estrato[glosa_estrato] = resumen

    return resultado_estrato


def left_join_consecutivo(left, right):
    """
    Perform a left join operation on the 'left' and 'right' DataFrames based on the specified 
    column. The columns to join are defined in the global variable UNIR_EN.

    :param left: The left DataFrame.
    :type left: pl.DataFrame

    :param right: The right DataFrame.
    :type right: pl.DataFrame

    :return: The result of the left join operation.
    :rtype: pl.DataFrame
    """
    return left.join(right, how="left", on=UNIR_EN)

def leer_cie():
    df = pl.read_excel("../data/external/CIE-10 - sin_puntos_y_X.xlsx")
    return df
