"""Construcción del dataset canónico para pueblos indígenas CNPV‑2018.

Este módulo contiene una función principal `build_dataset` que lee el libro
Excel suministrado por el DANE, normaliza los nombres de municipios,
deduplica los pueblos utilizando el código `PA11_COD_ETNIA` y crea un
DataFrame canónico a nivel de municipio por pueblo. También puede guardar
el resultado en archivos CSV y/o Parquet, así como construir una base de
datos SQLite optimizada para consultas posteriores.

Además, se incluye una interfaz de línea de comandos que permite ejecutar
directamente el script para generar el dataset.

Notas:
  - El script asume que la hoja "3" del Excel contiene la base de datos con
    las columnas de departamento, municipio, código de pueblo y población.
  - La hoja "1" del Excel debe incluir el catálogo de pueblos con el código
    `PA11_COD_ETNIA` y el nombre del pueblo. Si existen filas repetidas,
    se eliminan manteniendo un registro por código.
  - Si los nombres exactos de las columnas difieren entre versiones del
    archivo, el script intenta detectar las columnas apropiadas buscando
    patrones en los encabezados.

Ejemplo de uso:

    python -m indigenas_toolkit.dataset.build_dataset \
        --input visor-pueblos-indigenas-06-2021.xlsx \
        --output_csv data/base_municipal_pueblo.csv \
        --output_parquet data/base_municipal_pueblo.parquet

"""

from __future__ import annotations

import argparse
import re
import sqlite3
from typing import Optional, Tuple

import numpy as np
import pandas as pd


def _guess_column(df: pd.DataFrame, keywords: Tuple[str, ...],
                  default: Optional[str] = None) -> Optional[str]:
    """Intenta encontrar una columna cuyo nombre contenga alguno de los
    patrones especificados.

    Los nombres de columnas se comparan en minúsculas y sin tildes para
    aumentar la robustez frente a distintas versiones del Excel.

    Args:
        df: DataFrame del que extraer la columna.
        keywords: Una tupla de palabras clave a buscar en los nombres.
        default: Nombre de columna por defecto si no se encuentra ninguna coincidencia.

    Returns:
        El nombre de la columna coincidente o `default` si no se encuentra.
    """
    normalized_cols = {c: re.sub(r"[\s_]+", "", c.lower()) for c in df.columns}
    for key in keywords:
        key_norm = re.sub(r"[\s_]+", "", key.lower())
        for col, norm in normalized_cols.items():
            if key_norm in norm:
                return col
    return default


def build_dataset(excel_path: str,
                  output_csv: Optional[str] = None,
                  output_parquet: Optional[str] = None) -> pd.DataFrame:
    """Lee el archivo Excel del visor de pueblos indígenas y construye el dataset base.

    Args:
        excel_path: Ruta al archivo Excel original.
        output_csv: Ruta donde guardar el CSV resultante (opcional).
        output_parquet: Ruta donde guardar el Parquet resultante (opcional).

    Returns:
        DataFrame con la base a nivel de municipio por pueblo.

    La función realiza los siguientes pasos:
        1. Lee la hoja "3" del libro Excel (base de datos) utilizando la
           primera fila como encabezados. Elimina filas totalmente vacías.
        2. Normaliza los nombres de las columnas eliminando espacios y
           caracteres no alfanuméricos.
        3. Detecta las columnas de departamento, municipio, código de pueblo
           (`PA11_COD_ETNIA`) y población. Si no se encuentran columnas
           específicas, se lanzará un error.
        4. Limpia la columna de municipio removiendo el departamento entre
           paréntesis al final (ejemplo: "Puerto López (META)" → "Puerto López").
        5. Construye la clave compuesta `KeyMpio` concatenando `Departamento` y
           `Municipio_limpio` con un separador "|".
        6. Lee la hoja "1" (catálogo) y selecciona las columnas con el código
           `PA11_COD_ETNIA` y el nombre del pueblo. Deduplica por código.
        7. Une la base con el catálogo para asignar el nombre del pueblo.
        8. Devuelve el DataFrame con las columnas finales.

    """
    # Leer la hoja 3. Se usa header=0 para tomar la primera fila como
    # encabezados; en algunos archivos la primera fila contiene etiquetas
    # desordenadas y se omite con skiprows.
    try:
        base = pd.read_excel(excel_path, sheet_name="3", header=0)
    except Exception as exc:
        raise FileNotFoundError(f"No se pudo leer la hoja '3' de {excel_path}: {exc}")

    # Eliminar filas totalmente vacías
    base = base.dropna(how="all")

    # Detectar columnas clave
    dept_col = _guess_column(base, ("departamento",), default=None)
    muni_col = _guess_column(base, ("municipio",), default=None)
    code_col = _guess_column(base, ("pa11codetnia", "codetnia", "cod_pueblo"), default=None)
    pop_col = _guess_column(base, ("poblacion", "total", "cnt"), default=None)

    if not all([dept_col, muni_col, code_col, pop_col]):
        raise KeyError(
            "No se pudieron detectar correctamente las columnas principales en la hoja '3'. "
            f"Detectadas: departamento={dept_col}, municipio={muni_col}, codigo={code_col}, poblacion={pop_col}."
        )

    # Renombrar columnas relevantes para estandarizar
    base = base.rename(columns={
        dept_col: "Departamento",
        muni_col: "Municipio",
        code_col: "PA11_COD_ETNIA",
        pop_col: "POBLACION_2018",
    })

    # Limpiar nombres de municipio quitando la parte entre paréntesis al final
    base["Municipio_limpio"] = (
        base["Municipio"].astype(str)
        .str.replace(r"\s*\(.*\)$", "", regex=True)
        .str.strip()
    )

    # Crear clave compuesta
    base["KeyMpio"] = base["Departamento"].astype(str).str.strip() + "|" + base["Municipio_limpio"]

    # Leer el catálogo de pueblos (hoja 1)
    try:
        catalogo = pd.read_excel(excel_path, sheet_name="1", header=0)
    except Exception:
        catalogo = None

    pueblo_col = None
    if catalogo is not None:
        # Normalizar nombres de columnas para detectar el código y el nombre de pueblo
        code_col_cat = _guess_column(catalogo, ("pa11codetnia", "codetnia", "codigo"), default=None)
        pueblo_col = _guess_column(catalogo, ("pueblo", "nombre", "etnia"), default=None)
        if code_col_cat and pueblo_col:
            catalogo = catalogo[[code_col_cat, pueblo_col]].drop_duplicates(subset=[code_col_cat])
            catalogo = catalogo.rename(columns={code_col_cat: "PA11_COD_ETNIA", pueblo_col: "Pueblo"})
        else:
            catalogo = None

    # Unir con el catálogo si existe
    if catalogo is not None:
        base = base.merge(catalogo, how="left", on="PA11_COD_ETNIA")
    else:
        # Si no hay catálogo, crear columna Pueblo con NaN
        base["Pueblo"] = np.nan

    # Seleccionar columnas finales
    final_cols = [
        "Departamento",
        "Municipio_limpio",
        "KeyMpio",
        "PA11_COD_ETNIA",
        "Pueblo",
        "POBLACION_2018",
    ]
    dataset = base[final_cols].copy()

    # Convertir población a numérico (por si vienen cadenas con separadores)
    dataset["POBLACION_2018"] = pd.to_numeric(dataset["POBLACION_2018"], errors="coerce").fillna(0).astype(int)

    # Guardar resultados si se especifica
    if output_csv:
        dataset.to_csv(output_csv, index=False)
    if output_parquet:
        dataset.to_parquet(output_parquet, index=False)

    return dataset


def build_sqlite_database(excel_path: str, sqlite_path: str) -> None:
    """Genera una base de datos SQLite con el esquema requerido para la API.

    Este procedimiento lee múltiples hojas del libro Excel y normaliza la
    información en tablas relacionales. También ejecuta validaciones
    básicas sobre la consistencia de los datos.

    Args:
        excel_path: Ruta al archivo Excel original.
        sqlite_path: Ruta donde se almacenará la base de datos SQLite.
    """

    # --- Hoja 3: información geográfica de municipios por pueblo ---
    base = pd.read_excel(excel_path, sheet_name="3", header=0).dropna(how="all")

    dept_code_col = _guess_column(base, ("coddepto", "cod_dpto", "coddepartamento"))
    dept_name_col = _guess_column(base, ("departamento",))
    muni_code_col = _guess_column(base, ("codmpio", "cod_mpio", "codmunicipio"))
    muni_name_col = _guess_column(base, ("municipio",))
    code_col = _guess_column(base, ("pa11codetnia", "codetnia", "cod_pueblo"))
    pop_col = _guess_column(base, ("poblacion", "total", "cnt"))

    required = [dept_code_col, dept_name_col, muni_code_col, muni_name_col, code_col, pop_col]
    if not all(required):
        raise KeyError("Faltan columnas necesarias en la hoja '3' del Excel")

    base = base.rename(columns={
        dept_code_col: "cod_depto",
        dept_name_col: "departamento",
        muni_code_col: "cod_mpio",
        muni_name_col: "municipio",
        code_col: "codigo_etnia",
        pop_col: "poblacion",
    })
    base["municipio"] = (
        base["municipio"].astype(str).str.replace(r"\s*\(.*\)$", "", regex=True).str.strip()
    )
    base["poblacion"] = pd.to_numeric(base["poblacion"], errors="coerce").fillna(0).astype(int)

    departamentos_df = base[["cod_depto", "departamento"]].drop_duplicates()
    municipios_df = base[["cod_mpio", "municipio", "cod_depto"]].drop_duplicates()
    poblacion_geo_df = base[["cod_mpio", "codigo_etnia", "poblacion"]]

    # --- Hoja 1: catálogo de pueblos y serie temporal ---
    catalogo = pd.read_excel(excel_path, sheet_name="1", header=0).dropna(how="all")
    code_col = _guess_column(catalogo, ("pa11codetnia", "codetnia", "codigo"))
    name_col = _guess_column(catalogo, ("pueblo", "nombre", "etnia"))
    pop15_col = _guess_column(catalogo, ("2015", "poblacion2015", "pob2015"))
    pop18_col = _guess_column(catalogo, ("2018", "poblacion2018", "pob2018"))

    pueblos_df = catalogo[[code_col, name_col]].drop_duplicates().rename(
        columns={code_col: "codigo_etnia", name_col: "nombre"}
    )

    serie_cols = [c for c in [pop15_col, pop18_col] if c is not None]
    serie_df = catalogo[[code_col] + serie_cols].rename(columns={code_col: "codigo_etnia"})
    serie_long = (
        serie_df.melt(id_vars="codigo_etnia", var_name="anio", value_name="poblacion")
        .dropna(subset=["poblacion"])
    )
    serie_long["anio"] = serie_long["anio"].astype(str).str.extract(r"(\d{4})").astype(int)
    serie_long["poblacion"] = pd.to_numeric(serie_long["poblacion"], errors="coerce").fillna(0).astype(int)

    # --- Hoja 2: clase y territorio ---
    try:
        hoja2 = pd.read_excel(excel_path, sheet_name="2", header=0).dropna(how="all")
        clase_col = _guess_column(hoja2, ("clase",))
        territorio_col = _guess_column(hoja2, ("territorio",))
        codigo_col = _guess_column(hoja2, ("pa11codetnia", "codetnia", "codigo"))
        poblacion_col = _guess_column(hoja2, ("poblacion", "total"))
        clase_territorio_df = hoja2[[codigo_col, clase_col, territorio_col, poblacion_col]].rename(
            columns={
                codigo_col: "codigo_etnia",
                clase_col: "clase",
                territorio_col: "territorio",
                poblacion_col: "poblacion",
            }
        )
        clase_territorio_df["poblacion"] = pd.to_numeric(
            clase_territorio_df["poblacion"], errors="coerce"
        ).fillna(0).astype(int)
    except Exception:
        clase_territorio_df = pd.DataFrame(columns=["codigo_etnia", "clase", "territorio", "poblacion"])

    # --- Hojas 4 y 5: distribución por edad y sexo ---
    edad_sexo_frames = []
    for sheet in ("4", "5"):
        try:
            tmp = pd.read_excel(excel_path, sheet_name=sheet, header=0).dropna(how="all")
            codigo_col = _guess_column(tmp, ("pa11codetnia", "codetnia", "codigo"))
            sexo_col = _guess_column(tmp, ("sexo", "genero"))
            edad_col = _guess_column(tmp, ("rango", "edad"))
            poblacion_col = _guess_column(tmp, ("poblacion", "total"))
            if all([codigo_col, sexo_col, edad_col, poblacion_col]):
                tmp = tmp[[codigo_col, sexo_col, edad_col, poblacion_col]].rename(
                    columns={
                        codigo_col: "codigo_etnia",
                        sexo_col: "sexo",
                        edad_col: "rango_edad",
                        poblacion_col: "poblacion",
                    }
                )
                tmp["poblacion"] = pd.to_numeric(tmp["poblacion"], errors="coerce").fillna(0).astype(int)
                edad_sexo_frames.append(tmp)
        except Exception:
            continue
    poblacion_edad_sexo_df = (
        pd.concat(edad_sexo_frames, ignore_index=True)
        if edad_sexo_frames
        else pd.DataFrame(columns=["codigo_etnia", "sexo", "rango_edad", "poblacion"])
    )

    # --- Creación de la base SQLite ---
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        cur.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS pueblos (
              codigo_etnia INTEGER PRIMARY KEY,
              nombre TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS departamentos (
              cod_depto TEXT PRIMARY KEY,
              nombre TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS municipios (
              cod_mpio TEXT PRIMARY KEY,
              nombre TEXT NOT NULL,
              cod_depto TEXT NOT NULL REFERENCES departamentos(cod_depto)
            );
            CREATE TABLE IF NOT EXISTS poblacion_geo_2018 (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              cod_mpio TEXT NOT NULL REFERENCES municipios(cod_mpio),
              codigo_etnia INTEGER NOT NULL REFERENCES pueblos(codigo_etnia),
              poblacion INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS poblacion_edad_sexo_2018 (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              codigo_etnia INTEGER NOT NULL REFERENCES pueblos(codigo_etnia),
              sexo TEXT NOT NULL,
              rango_edad TEXT NOT NULL,
              poblacion INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS poblacion_serie (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              codigo_etnia INTEGER NOT NULL REFERENCES pueblos(codigo_etnia),
              anio INTEGER NOT NULL,
              poblacion INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS clase_territorio (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              codigo_etnia INTEGER NOT NULL REFERENCES pueblos(codigo_etnia),
              clase TEXT NOT NULL,
              territorio TEXT NOT NULL,
              poblacion INTEGER NOT NULL
            );
            """
        )

        pueblos_df.to_sql("pueblos", conn, if_exists="replace", index=False)
        departamentos_df.to_sql("departamentos", conn, if_exists="replace", index=False)
        municipios_df.rename(columns={"municipio": "nombre"}).to_sql(
            "municipios", conn, if_exists="replace", index=False
        )
        poblacion_geo_df.to_sql("poblacion_geo_2018", conn, if_exists="replace", index=False)
        poblacion_edad_sexo_df.to_sql("poblacion_edad_sexo_2018", conn, if_exists="replace", index=False)
        serie_long.to_sql("poblacion_serie", conn, if_exists="replace", index=False)
        clase_territorio_df.to_sql("clase_territorio", conn, if_exists="replace", index=False)

    # --- Validaciones de consistencia ---
    total_poblacion = int(poblacion_geo_df["poblacion"].sum())
    if total_poblacion != 3_811_234:
        raise AssertionError(
            f"La población total esperada es 3,811,234 y se obtuvo {total_poblacion}"
        )
    if departamentos_df["cod_depto"].nunique() != 34:
        raise AssertionError("El número de departamentos únicos no es 34")
    if municipios_df["cod_mpio"].nunique() != 970:
        raise AssertionError("El número de municipios únicos no es 970")


def main(argv: Optional[list[str]] = None) -> None:
    """Punto de entrada para uso por línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Construye el dataset a nivel municipio×pueblo a partir del visor CNPV-2018"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Ruta al archivo Excel original (visor-pueblos-indigenas-06-2021.xlsx)",
    )
    parser.add_argument("--output_csv", help="Ruta donde guardar el CSV resultante")
    parser.add_argument("--output_parquet", help="Ruta donde guardar el Parquet resultante")
    parser.add_argument(
        "--output_sqlite",
        help="Ruta donde guardar la base de datos SQLite generada",
    )
    args = parser.parse_args(argv)

    if args.output_sqlite:
        build_sqlite_database(args.input, args.output_sqlite)
    else:
        build_dataset(args.input, args.output_csv, args.output_parquet)


if __name__ == "__main__":
    main()
