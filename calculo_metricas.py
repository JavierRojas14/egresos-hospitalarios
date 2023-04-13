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


def obtener_metricas_egresos(df, agrupar_por):
    metricas_agregadas = df.groupby(agrupar_por).agg(
        [
            pl.col("DIAG1").count().alias("n_egresos"),
            pl.col("DIAS_ESTADA").mean().alias("dias_estada_promedio"),
            pl.col("INTERV_Q").sum().alias("n_int_q"),
            pl.col("CONDICION_EGRESO").sum().alias("n_muertos"),
        ]
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