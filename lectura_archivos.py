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


def remapear_columnas_egresos(df):
    tmp = df.with_columns(
        [
            (pl.col("INTERV_Q").map_dict({2: 0}, default=pl.col("INTERV_Q"))),
            (pl.col("CONDICION_EGRESO").map_dict({1: 0, 2: 1}, default=pl.col("CONDICION_EGRESO"))),
            (
                pl.col("GLOSA_REGION_RESIDENCIA").map_dict(
                    {
                        "Del Libertador B. O'Higgins": "del Libertador General Bernardo O'Higgins",
                        "De Aisén del Gral. C. Ibáñez del Campo": "Aysén del General Carlos Ibáñez del Campo",
                    },
                    default=pl.col("GLOSA_REGION_RESIDENCIA"),
                )
            ),
        ]
    )

    return tmp


def obtener_diagnosticos_unicos_de_hospital(df, hospital_a_analizar):
    diags_hospital = (
        df.filter(pl.col("ESTABLECIMIENTO_SALUD") == hospital_a_analizar)
        .select(pl.col("DIAG1"))
        .unique()
    )

    return diags_hospital


def leer_archivos(filtro_hospital=11203):
    with pl.StringCache():
        df_nacional = pl.scan_csv("input/utf-8/*.csv", separator=";")
        diags_torax = (
            obtener_diagnosticos_unicos_de_hospital(df_nacional, filtro_hospital).collect(
                streaming=True
            )
        ).to_series()
        df = df_nacional.filter(pl.col("DIAG1").is_in(diags_torax))
        df = remapear_columnas_egresos(df)
        df = df.with_columns(
            ("Region " + pl.col("GLOSA_REGION_RESIDENCIA") + ", Chile").alias("region_pais")
        )

        df = df.with_columns(
            (pl.col("GLOSA_COMUNA_RESIDENCIA") + ", " + pl.col("region_pais")).alias(
                "comuna_region_pais"
            )
        )

        return df
