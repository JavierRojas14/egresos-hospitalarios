def obtener_ranking_subgrupo(df_estrato, subgrupo_del_ranking):
    return (df_estrato.groupby(subgrupo_del_ranking).cumcount() + 1)


def obtener_porcentaje_subgrupo(df_estrato, subgrupo_del_ranking, variable_analisis):
    return (df_estrato[variable_analisis] /
            df_estrato.groupby(subgrupo_del_ranking)[variable_analisis].transform('sum'))


def obtener_total_subgrupo(df_estrato, subgrupo_del_ranking, variable_analisis):
    return (df_estrato.groupby(
            subgrupo_del_ranking)[variable_analisis].transform('sum'))


def obtener_metricas_para_un_estrato(subgrupo_del_ranking, variable_analisis, estrato, df_estrato):
    tmp = df_estrato.copy()

    sufijo_cols = f'_{estrato}_{variable_analisis}'
    var_rank = f'ranking{sufijo_cols}'
    var_porc = f'%{sufijo_cols}'
    var_total = f'total{sufijo_cols}'

    tmp[var_rank] = obtener_ranking_subgrupo(tmp, subgrupo_del_ranking)
    tmp[var_porc] = obtener_porcentaje_subgrupo(tmp, subgrupo_del_ranking,
                                                variable_analisis)
    tmp[var_total] = obtener_total_subgrupo(tmp, subgrupo_del_ranking,
                                            variable_analisis)

    tmp = tmp.drop(columns=['n_egresos', 'dias_estada_promedio',
                            'n_int_q', 'n_muertos'])

    return tmp


def calcular_metricas_por_estrato(df, estratos, variables_para_rankear, subgrupo_del_ranking):
    '''Esta es una funcion que permite calcular el ranking, % y total de un hospital en un
    estrato.
    '''
    tmp = df.copy()
    for variable_analisis in variables_para_rankear:
        resultados_estrato = {}
        for estrato, codigos_en_estrato in estratos.items():
            df_estrato = tmp[tmp['ESTABLECIMIENTO_SALUD'].isin(
                codigos_en_estrato)]
            df_estrato = df_estrato.sort_values(subgrupo_del_ranking + [variable_analisis],
                                                ascending=[False, True, False])

            df_resumen = obtener_metricas_para_un_estrato(subgrupo_del_ranking, variable_analisis,
                                                          estrato, df_estrato)

            resultados_estrato[estrato] = df_resumen

    return resultados_estrato


def realizar_ranking_por_estrato(df_nacional, estratos, variables_para_rankear,
                                 subgrupo_del_ranking,
                                 unir_por):

    resultados_estratos = calcular_metricas_por_estrato(df_nacional, estratos,
                                                        variables_para_rankear,
                                                        subgrupo_del_ranking)

    tmp_global = df_nacional.copy().set_index(unir_por)

    for resultado_estrato in resultados_estratos.values():
        tmp_estrato = resultado_estrato.copy().set_index(unir_por)
        tmp_global = tmp_global.merge(
            tmp_estrato, how='left', left_index=True, right_index=True)

    tmp_global = tmp_global.reset_index()

    return tmp_global
