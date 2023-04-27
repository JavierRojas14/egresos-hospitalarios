"""Modulo para calcular diversas metricas para los Egresos Hospitalarios de la base de datos del
Departamento de Estadisticas e Informacion de Salud de Chile

Estas funciones son utilizadas en conjunto con el archivo Jupyter Notebook para realizar los
diversos analisis

Este script necesita que se utilice polars para funcionar
"""

import polars as pl


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
    "dias_estada_promedio",
    "n_int_q",
    "n_muertos",
]


def obtener_metricas_egresos(df, agrupar_por):
    metricas_agregadas = df.groupby(agrupar_por).agg(
        [
            pl.col("DIAG1").count().alias("n_egresos"),
            pl.col("DIAS_ESTADA").sum().alias("dias_estada_totales"),
            pl.col("INTERV_Q").sum().alias("n_int_q"),
            pl.col("CONDICION_EGRESO").sum().alias("n_muertos"),
        ]
    )

    metricas_agregadas = metricas_agregadas.with_columns(
        (pl.col("dias_estada_totales") / pl.col("n_egresos")).alias("dias_estada_promedio")
    )

    return metricas_agregadas


def obtener_diccionario_estratos(df_nacional, hospital_interno):
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
    return left.join(right, how="left", on=UNIR_EN)
