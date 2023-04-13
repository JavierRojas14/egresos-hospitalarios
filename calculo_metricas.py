import polars as pl


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
