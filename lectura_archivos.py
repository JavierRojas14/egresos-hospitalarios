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
    tmp = df.clone()

    for variable, dict_cambio in dict_mapeo.items():
        tmp = tmp.with_columns(pl.col(variable).map_dict(dict_cambio, default=pl.col(variable)))

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
        df = mappear_columnas(df, MAPPING_METRICAS_EGRESOS)

        return df


def leer_cie():
    cie = pl.read_excel("input/CIE-10.xlsx").with_columns(
        pl.col("Código").str.replace(".", "", literal=True).str.ljust(4, "X").alias("DIAG1")
    )

    cie = cie.drop(["Código", "Versión"])

    return cie


# ANALISIS SOCIODEMOGRAFICO


def agregar_columnas_localizacion(df):
    tmp = df.with_columns(
        ("Region " + pl.col("GLOSA_REGION_RESIDENCIA") + ", Chile").alias("region_pais")
    )

    tmp = df.with_columns(
        (pl.col("GLOSA_COMUNA_RESIDENCIA") + ", " + pl.col("region_pais")).alias(
            "comuna_region_pais"
        )
    )

    return tmp


def agregar_categorizacion_edad(df):
    tmp = df.with_columns(
        (df.get_column("EDAD_A_OS"))
        .cut(bins=range(0, 101, 10), maintain_order=True)
        .select(pl.col("category").alias("EDAD_CATEGORIA"))
    )

    return tmp


def obtener_df_inicial_sociodemografico(filtro_hospital=112103):
    with pl.StringCache():
        df_socio = pl.scan_csv("input/utf-8/*.csv", separator=";", dtypes=DICT_VARIABLES).filter(
            pl.col("ESTABLECIMIENTO_SALUD") == filtro_hospital
        )

        df_socio = mappear_columnas(df_socio, MAPPING_SOCIODEMOGRAFICO)
        df_socio = agregar_columnas_localizacion(df_socio).collect()
        df_socio = agregar_categorizacion_edad(df_socio)

    return df_socio
