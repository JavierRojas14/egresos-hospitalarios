import unittest
import calculo_metricas as aux

import pickle
import polars as pl


with open(r"df_prueba.pickle", "rb") as file:
    DF_PRUEBA = pickle.load(file)

AGRUPACION = [
    "ANO_EGRESO",
    "ESTABLECIMIENTO_SALUD",
    "GLOSA_ESTABLECIMIENTO_SALUD",
    "DIAG1",
]

TESTS_DIAGNOSTICOS = [
    [112101, "P027"],  # Testeo aleatorio
    [111195, "J189"],  # Testeo aleatorio
    [103100, "P073"],  # Testeo aleatorio
    [111195, "K359"],
]  # Testeo con Int Q


def testear_n_egresos(resultado_metricas, df_prueba, cod_hospital, diag):
    expr_filtro = (pl.col("ESTABLECIMIENTO_SALUD") == cod_hospital) & (pl.col("DIAG1") == diag)
    resultado_filtro = df_prueba.filter(expr_filtro).shape[0]

    resultado_metricas = resultado_metricas.filter(expr_filtro).select("n_egresos").item()

    return resultado_filtro == resultado_metricas


def testear_int_q(resultado_metricas, df_prueba, cod_hospital, diag):
    expr_filtro = (pl.col("ESTABLECIMIENTO_SALUD") == cod_hospital) & (pl.col("DIAG1") == diag)
    filtro_interv_q = (expr_filtro) & (pl.col("INTERV_Q") == 1)

    resultado_filtro = df_prueba.filter(filtro_interv_q).shape[0]
    resultado_metricas = resultado_metricas.filter(expr_filtro).select("n_int_q").item()

    return resultado_filtro == resultado_metricas


class TestMetricasEgresos(unittest.TestCase):
    def testear_metricas(self):
        resultados = aux.obtener_metricas_egresos(DF_PRUEBA, AGRUPACION)

        for cod_hospital, diag in TESTS_DIAGNOSTICOS:
            self.assertTrue(testear_n_egresos(resultados, DF_PRUEBA, cod_hospital, diag))
            self.assertTrue(testear_int_q(resultados, DF_PRUEBA, cod_hospital, diag))


if __name__ == "__main__":
    unittest.main()
