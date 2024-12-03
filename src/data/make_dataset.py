# -*- coding: utf-8 -*-
import logging
from pathlib import Path

import click
import polars as pl
from dotenv import find_dotenv, load_dotenv

DICT_VARIABLES = {
    "ID_PACIENTE": str,
    "ESTABLECIMIENTO_SALUD": pl.Float64,
    "GLOSA_ESTABLECIMIENTO_SALUD": pl.Categorical,
    "PERTENENCIA_ESTABLECIMIENTO_SALUD": pl.Categorical,
    "SEREMI": pl.Float64,
    "SERVICIO_DE_SALUD": pl.Float64,
    "SEXO": pl.Float64,
    "EDAD_CANT": pl.Float64,
    "TIPO_EDAD": pl.Float64,
    "EDAD_A_OS": pl.Float64,
    "PUEBLO_ORIGINARIO": pl.Float64,
    "PAIS_ORIGEN": pl.Float64,
    "COMUNA_RESIDENCIA": pl.Float64,
    "GLOSA_COMUNA_RESIDENCIA": pl.Categorical,
    "REGION_RESIDENCIA": pl.Float32,
    "GLOSA_REGION_RESIDENCIA": pl.Categorical,
    "PREVISION": pl.Float64,
    "BENEFICIARIO": pl.Categorical,
    "MODALIDAD": pl.Float64,
    "PROCEDENCIA": pl.Float64,
    "ANO_EGRESO": pl.Float64,
    "FECHA_EGRESO": pl.Date,
    "AREA_FUNCIONAL_EGRESO": pl.Float64,
    "DIAS_ESTADA": pl.Float64,
    "CONDICION_EGRESO": pl.Float64,
    "INTERV_Q": pl.Float64,
    "CODIGO_INTERV_Q_PPAL": pl.Float64,
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

VALORES_NULOS = {
    "REGION_RESIDENCIA": "Extranjero",
    "FECHA_EGRESO": "",
}

# Imputa codigo del torax
CODIGO_TORAX = 112103


def leer_egresos_deis(ruta_carpeta_contenedora):
    """
    Read and process the DEIS's public databases for hospital discharges in Chile from the input
    directory, filtering by a specific hospital.

    :param filtro_hospital: The ID of the hospital to filter by. Defaults to 11203
    (Instituto Nacional del Torax's code).
    :type filtro_hospital: int, optional

    :return: The processed DataFrame filtered by the specified hospital.
    :rtype: pl.DataFrame
    """
    with pl.StringCache():
        df_nacional = pl.scan_csv(
            f"{ruta_carpeta_contenedora}/*.csv",
            separator=";",
            dtypes=DICT_VARIABLES,
            null_values=VALORES_NULOS,
        )
        df_nacional = mappear_columnas(df_nacional, MAPPING_METRICAS_EGRESOS)
        df_nacional = mappear_columnas(df_nacional, MAPPING_SOCIODEMOGRAFICO)
        df_nacional = agregar_columnas_region_y_comuna(df_nacional)
        df_nacional = agregar_categorizacion_edad(df_nacional)

        return df_nacional


def mappear_columnas(df, dict_mapeo):
    """
    Map columns in the DataFrame based on a mapping dictionary.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :param dict_mapeo: A dictionary containing the mapping for columns.
    :type dict_mapeo: dict

    :return: The DataFrame with mapped columns.
    :rtype: pl.DataFrame
    """
    tmp = df.clone()

    for variable, dict_cambio in dict_mapeo.items():
        tmp = tmp.with_columns(pl.col(variable).map_dict(dict_cambio, default=pl.col(variable)))

    return tmp


def agregar_columnas_region_y_comuna(df):
    """
    Add location-related columns to the DataFrame based on region and comuna information.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :return: The DataFrame with added location-related columns.
    :rtype: pl.DataFrame
    """
    tmp = df.with_columns(
        ("Region " + pl.col("GLOSA_REGION_RESIDENCIA") + ", Chile").alias("region_pais")
    )

    tmp = tmp.with_columns(
        (pl.col("GLOSA_COMUNA_RESIDENCIA") + ", " + pl.col("region_pais")).alias(
            "comuna_region_pais"
        )
    )

    return tmp


def agregar_categorizacion_edad(df):
    """
    Add age category column to the DataFrame based on age in years.

    :param df: The input DataFrame.
    :type df: pl.DataFrame

    :return: The DataFrame with added age category column.
    :rtype: pl.DataFrame
    """
    tmp = df.with_columns(EDAD_CATEGORIA=pl.col("EDAD_A_OS").cut(range(0, 121, 10)))

    return tmp


def filtrar_hospital_de_interes(df, codigo_hospital):
    return df.filter(pl.col("ESTABLECIMIENTO_SALUD") == codigo_hospital)


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    with pl.StringCache():
        # Lee y procesa base de DEIS y filtra la base del Torax
        df_nacional = leer_egresos_deis(input_filepath).collect()
        df_hospital_de_interes = filtrar_hospital_de_interes(df_nacional, CODIGO_TORAX)
        df_roberto = filtrar_hospital_de_interes(df_nacional, 109101)

        # Define la ruta a guardar ambas bases de datos
        ruta_egresos_nacionales = f"{output_filepath}/egresos_procesados.csv"
        ruta_egresos_hospital = f"{output_filepath}/egresos_procesados_{CODIGO_TORAX}.csv"
        ruta_egresos_roberto = f"{output_filepath}/egresos_procesados_109101.csv"

        # Exporta bases de datos
        print(f"> Guardando {ruta_egresos_nacionales}")
        df_nacional.write_csv(ruta_egresos_nacionales)

        print(f"> Guardando {ruta_egresos_hospital}")
        df_hospital_de_interes.write_csv(ruta_egresos_hospital)

        print(f"> Guardando {ruta_egresos_roberto}")
        df_roberto.write_csv(ruta_egresos_roberto)


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
