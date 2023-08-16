# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

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

def leer_archivos(ruta_carpeta_contenedora):
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
        df_nacional = pl.scan_csv(f"{ruta_carpeta_contenedora}/*.csv", separator=";")
        df_nacional = mappear_columnas(df_nacional, MAPPING_METRICAS_EGRESOS)

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

@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
