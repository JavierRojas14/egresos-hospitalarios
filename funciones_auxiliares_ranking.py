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


def obtener_ranking_subgrupo(df_estrato, subgrupo_del_ranking):
    return df_estrato.groupby(subgrupo_del_ranking).cumcount() + 1


def obtener_porcentaje_subgrupo(df_estrato, subgrupo_del_ranking, variable_analisis):
    return df_estrato[variable_analisis] / df_estrato.groupby(subgrupo_del_ranking)[
        variable_analisis
    ].transform("sum")


def obtener_total_subgrupo(df_estrato, subgrupo_del_ranking, variable_analisis):
    return df_estrato.groupby(subgrupo_del_ranking)[variable_analisis].transform("sum")


def obtener_metricas_para_un_estrato(
    df_estrato,
    estrato,
    subgrupo_del_ranking,
    variable_analisis,
):
    tmp = df_estrato.copy()

    sufijo_cols = f"_{estrato}_{variable_analisis}"
    var_rank = f"ranking{sufijo_cols}"
    var_porc = f"%{sufijo_cols}"
    var_total = f"total{sufijo_cols}"

    tmp[var_rank] = obtener_ranking_subgrupo(tmp, subgrupo_del_ranking)
    tmp[var_porc] = obtener_porcentaje_subgrupo(tmp, subgrupo_del_ranking, variable_analisis)
    tmp[var_total] = obtener_total_subgrupo(tmp, subgrupo_del_ranking, variable_analisis)

    tmp = tmp.drop(columns=["n_egresos", "dias_estada_promedio", "n_int_q", "n_muertos"])

    return tmp


def calcular_metricas_por_estrato(df, estratos, variables_para_rankear, subgrupo_del_ranking):
    """Esta es una funcion que permite calcular el ranking, % y total de un hospital en un
    estrato.
    """
    tmp = df.copy()
    for variable_analisis in variables_para_rankear:
        resultados_estrato = {}
        for estrato, codigos_en_estrato in estratos.items():
            if estrato == "interno":
                subgrupo_del_ranking.remove("DIAG1")

            df_estrato = tmp[tmp["ESTABLECIMIENTO_SALUD"].isin(codigos_en_estrato)]
            df_estrato = df_estrato.sort_values(
                subgrupo_del_ranking + [variable_analisis], ascending=False
            )

            df_resumen = obtener_metricas_para_un_estrato(
                df_estrato, estrato, subgrupo_del_ranking, variable_analisis
            )

            resultados_estrato[estrato] = df_resumen

    return resultados_estrato


def realizar_ranking_por_estrato(df_nacional, estratos, variables_para_rankear, subgrupo_del_ranking, unir_por):
    resultados_estratos = calcular_metricas_por_estrato(
        df_nacional, estratos, variables_para_rankear, subgrupo_del_ranking
    )

    tmp_global = df_nacional.copy().set_index(unir_por)

    for resultado_estrato in resultados_estratos.values():
        tmp_estrato = resultado_estrato.copy().set_index(unir_por)
        tmp_global = tmp_global.merge(tmp_estrato, how="left", left_index=True, right_index=True)

    tmp_global = tmp_global.reset_index()

    return tmp_global


def obtener_codigos_de_estratos(completa_concatenada, hospital_interno):
    df_publicos = completa_concatenada.query("PERTENENCIA_ESTABLECIMIENTO_SALUD == @PERTENECE_SNSS")

    query_privados = (
        "PERTENENCIA_ESTABLECIMIENTO_SALUD == @NO_PERTENECE_SNSS or "
        "ESTABLECIMIENTO_SALUD == @hospital_interno"
    )

    df_privados = completa_concatenada.query(query_privados, engine="python")

    columna_codigos = "ESTABLECIMIENTO_SALUD"
    codigos_nacionales = completa_concatenada[columna_codigos].unique()
    codigos_publicos = df_publicos[columna_codigos].unique()
    codigos_privados = df_privados[columna_codigos].unique()

    diccionario_estratos = {
        "nacionales": codigos_nacionales,
        "publicos": codigos_publicos,
        "privados": codigos_privados,
        "grd": HOSPITALES_GRD,
        "interno": [hospital_interno],
    }

    return diccionario_estratos
