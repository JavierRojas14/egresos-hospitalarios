import unittest
import pickle

import polars as pl

from calculo_metricas import obtener_metricas_egresos

with open(r"df_prueba.pickle", "rb") as file:
    DF_PRUEBA = pickle.load(file)

AGRUPACION = [
    "ANO_EGRESO",
    "ESTABLECIMIENTO_SALUD",
    "GLOSA_ESTABLECIMIENTO_SALUD",
    "DIAG1",
]


class TestMetricasEgresos(unittest.TestCase):
    def testear_metricas(self):
        resultados = obtener_metricas_egresos(DF_PRUEBA, AGRUPACION)
        filtros = [[112101, "P027"], [111195, "J189"], [103100, "P073"]]

        for cod_hospital, diag in filtros:
            self.assertTrue(self.testear_n_egresos(resultados, DF_PRUEBA, cod_hospital, diag))

    def testear_n_egresos(self, resultado_metricas, df_prueba, cod_hospital, diag):
        resultado_filtro = df_prueba.filter(
            (pl.col("ESTABLECIMIENTO_SALUD") == cod_hospital) & (pl.col("DIAG1") == diag)
        ).shape[0]

        resultado_metricas = (
            resultado_metricas.filter(
                (pl.col("ESTABLECIMIENTO_SALUD") == cod_hospital) & (pl.col("DIAG1") == diag)
            )
            .select("n_egresos")
            .item()
        )

        return resultado_filtro == resultado_metricas


if __name__ == "__main__":
    unittest.main()
