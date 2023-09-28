import glob
import sys
import click

from pdf2image import convert_from_path

POPPLER_PATH = r"C:\Users\ppizarro\Downloads\Release-23.08.0-0\poppler-23.08.0\Library\bin"


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def guardar_pdfs_de_ruta(input_filepath, output_filepath):
    print("Guardando archivos")
    archivos_pdfs = glob.glob(f"{input_filepath}/*.pdf")
    for input_filepath in archivos_pdfs:
        todas_las_paginas = convert_from_path(input_filepath, poppler_path=POPPLER_PATH, dpi=300)

        for i, pagina in enumerate(todas_las_paginas):
            nombre_pdf = input_filepath.split("\\")[-1].split(".")[0]
            nombre_archivo = f"{nombre_pdf}_page{i + 1}.png"
            pagina.save(f"{output_filepath}/{nombre_archivo}", "PNG")


if __name__ == "__main__":
    guardar_pdfs_de_ruta()
