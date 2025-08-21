"""Exportación de indicadores y generación de reportes en Excel.

Este módulo toma como entrada el dataset canónico a nivel
municipio × pueblo (CSV o Parquet) y produce un libro Excel con
indicadores agregados por municipio y departamento, una matriz
Municipio × Pueblo, la base filtrada y un diccionario de variables.

Los filtros opcionales permiten seleccionar subconjuntos específicos de
pueblos (`pueblos`), municipios (`municipios`) o departamentos
(`departamentos`). Si se proporcionan listas vacías o `None`, se
consideran todas las categorías.

Ejemplo de uso desde la línea de comandos:

    python -m indigenas_toolkit.dataset.export_excel \
        --dataset data/base_municipal_pueblo.csv \
        --pueblos 570 571 \
        --departamentos META VICHADA \
        --municipios "PUERTO LOPEZ" "CUMARIBO" \
        --output reporte_filtrado.xlsx

"""

from __future__ import annotations

import argparse
import io
from typing import Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd


def _filter_dataset(df: pd.DataFrame,
                    pueblos: Optional[Sequence[str]] = None,
                    departamentos: Optional[Sequence[str]] = None,
                    municipios: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Filtra el DataFrame según las listas de pueblos, departamentos y municipios.

    El filtrado es case‑insensitive: convierte tanto las columnas de origen
    como los valores proporcionados a mayúsculas para la comparación.

    Args:
        df: DataFrame original.
        pueblos: Códigos o nombres de pueblos a incluir (opcional).
        departamentos: Nombres de departamentos a incluir (opcional).
        municipios: Nombres de municipios (columna `Municipio_limpio`) a incluir (opcional).

    Returns:
        DataFrame filtrado.
    """
    mask = pd.Series(True, index=df.index)
    if pueblos:
        pueblos_set = {str(x).upper() for x in pueblos}
        mask &= df["PA11_COD_ETNIA"].astype(str).str.upper().isin(pueblos_set) | df["Pueblo"].astype(str).str.upper().isin(pueblos_set)
    if departamentos:
        deps_set = {str(x).upper() for x in departamentos}
        mask &= df["Departamento"].astype(str).str.upper().isin(deps_set)
    if municipios:
        mun_set = {str(x).upper() for x in municipios}
        mask &= df["Municipio_limpio"].astype(str).str.upper().isin(mun_set)
    return df.loc[mask].copy()


def _compute_indicadores_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula indicadores por municipio.

    Retorna un DataFrame con las columnas:

    - Departamento
    - Municipio_limpio
    - poblacion_indigena_total
    - num_pueblos
    - pueblos_y_poblacion (cadena con formato "Pueblo:conteo; ...")
    - HHI
    - Simpson (1-HHI)
    - Shannon

    """
    registros = []
    for (dep, mun, key), sub in df.groupby(["Departamento", "Municipio_limpio", "KeyMpio"]):
        pop_total = sub["POBLACION_2018"].sum()
        # Agrupar por pueblo y ordenar de mayor a menor
        pueblos_series = sub.groupby("Pueblo", dropna=False)["POBLACION_2018"].sum().sort_values(ascending=False)
        pueblos_y_poblacion = "; ".join(f"{p}:{int(c)}" for p, c in pueblos_series.items())
        num_pueblos = (pueblos_series > 0).sum()
        # Calcular índices de diversidad
        shares = pueblos_series / pop_total if pop_total > 0 else pd.Series(dtype=float)
        hhi = float((shares ** 2).sum()) if not shares.empty else None
        simpson = float(1 - hhi) if hhi is not None else None
        # Evitar log(0) asignando 0 cuando la participación es 0
        shannon = float(-(shares[shares > 0] * np.log(shares[shares > 0])).sum()) if not shares.empty else None
        registros.append({
            "Departamento": dep,
            "Municipio_limpio": mun,
            "poblacion_indigena_total": int(pop_total),
            "num_pueblos": int(num_pueblos),
            "pueblos_y_poblacion": pueblos_y_poblacion,
            "HHI": hhi,
            "Simpson (1-HHI)": simpson,
            "Shannon": shannon,
        })
    return pd.DataFrame(registros)


def _compute_indicadores_departamento(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula indicadores agregados por departamento.

    Devuelve un DataFrame con:
    - Departamento
    - poblacion_indigena_total
    - num_pueblos (pueblos únicos en el departamento)
    - num_municipios (número de municipios con presencia indígena)
    """
    registros = []
    for dep, sub in df.groupby("Departamento"):
        pop_total = sub["POBLACION_2018"].sum()
        num_pueblos = sub["PA11_COD_ETNIA"].nunique()
        num_municipios = sub["KeyMpio"].nunique()
        registros.append({
            "Departamento": dep,
            "poblacion_indigena_total": int(pop_total),
            "num_pueblos": int(num_pueblos),
            "num_municipios": int(num_municipios),
        })
    return pd.DataFrame(registros)


def _compute_matriz_mpio_pueblo(df: pd.DataFrame) -> pd.DataFrame:
    """Genera una matriz Municipio × Pueblo con la suma de población.

    Retorna un DataFrame en formato ancho listo para ser utilizado como
    tabla dinámica. Las filas se identifican por Departamento y
    Municipio_limpio; las columnas son los pueblos y los valores son la
    población total.
    """
    pivot = df.pivot_table(
        index=["Departamento", "Municipio_limpio"],
        columns="Pueblo",
        values="POBLACION_2018",
        aggfunc="sum",
        fill_value=0,
    )
    # Asegurarse de que el índice sea una columna regular cuando se escriba a Excel
    pivot = pivot.reset_index()
    return pivot


def _build_diccionario() -> pd.DataFrame:
    """Crea un diccionario de variables con su descripción.

    Este diccionario se incluye como una de las hojas del Excel exportado.
    """
    data = [
        ("Departamento", "Nombre del departamento"),
        ("Municipio_limpio", "Nombre del municipio sin sufijo del departamento"),
        ("KeyMpio", "Clave compuesta Departamento|Municipio"),
        ("PA11_COD_ETNIA", "Código del pueblo indígena según el microdato CNPV‑2018"),
        ("Pueblo", "Nombre del pueblo indígena"),
        ("POBLACION_2018", "Población censada en 2018 que pertenece a ese pueblo en ese municipio"),
        ("poblacion_indigena_total", "Población indígena total en el municipio o departamento"),
        ("num_pueblos", "Número de pueblos diferentes presentes"),
        ("pueblos_y_poblacion", "Cadena con cada pueblo y su población en el municipio"),
        ("HHI", "Índice de Herfindahl–Hirschman de concentración por pueblos (0=diverso, 1=un solo pueblo)"),
        ("Simpson (1-HHI)", "Índice de diversidad de Simpson (1 - HHI)"),
        ("Shannon", "Índice de diversidad de Shannon (entropy)"),
        ("num_municipios", "Número de municipios con presencia indígena en el departamento"),
    ]
    return pd.DataFrame(data, columns=["variable", "descripcion"])


def export_excel(dataset_path: str,
                 pueblos: Optional[Sequence[str]] = None,
                 departamentos: Optional[Sequence[str]] = None,
                 municipios: Optional[Sequence[str]] = None,
                 output_path: Optional[str] = None) -> bytes:
    """Filtra el dataset y exporta un archivo Excel con cinco hojas.

    Si `output_path` es `None`, la función devuelve el contenido binario del
    archivo Excel en memoria. Si se proporciona una ruta, guarda el archivo
    directamente y devuelve los bytes generados.

    Args:
        dataset_path: Ruta al archivo CSV o Parquet con la base a nivel
            municipio × pueblo.
        pueblos: Lista opcional de códigos o nombres de pueblos a incluir.
        departamentos: Lista opcional de departamentos a incluir.
        municipios: Lista opcional de municipios (Municipio_limpio) a incluir.
        output_path: Ruta de salida del archivo Excel. Si no se especifica,
            devuelve los bytes del archivo generado.

    Returns:
        Bytes del archivo Excel generado.
    """
    # Cargar dataset (detecta CSV o Parquet por extensión)
    if dataset_path.lower().endswith(".csv"):
        df = pd.read_csv(dataset_path)
    elif dataset_path.lower().endswith(".parquet"):
        df = pd.read_parquet(dataset_path)
    else:
        raise ValueError("El dataset debe ser un archivo CSV o Parquet")

    # Filtrar según parámetros
    df_filt = _filter_dataset(df, pueblos=pueblos, departamentos=departamentos, municipios=municipios)

    # Generar indicadores
    indicadores_mpio = _compute_indicadores_municipio(df_filt)
    indicadores_dep = _compute_indicadores_departamento(df_filt)
    matriz = _compute_matriz_mpio_pueblo(df_filt)
    diccionario = _build_diccionario()

    # Crear el libro Excel en un buffer de memoria
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        # Hoja 1: base filtrada
        df_filt.to_excel(writer, sheet_name="base_municipal_pueblo", index=False)
        # Hoja 2: indicadores por municipio
        indicadores_mpio.to_excel(writer, sheet_name="indicadores_municipio", index=False)
        # Hoja 3: indicadores por departamento
        indicadores_dep.to_excel(writer, sheet_name="indicadores_departamento", index=False)
        # Hoja 4: matriz Municipio × Pueblo
        matriz.to_excel(writer, sheet_name="matriz_mpio_x_pueblo", index=False)
        # Hoja 5: diccionario de variables
        diccionario.to_excel(writer, sheet_name="diccionario", index=False)
    excel_bytes = buffer.getvalue()
    # Guardar a disco si se especifica una ruta
    if output_path:
        with open(output_path, "wb") as f:
            f.write(excel_bytes)
    return excel_bytes


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Genera un Excel con indicadores filtrados a partir del dataset municipio×pueblo")
    parser.add_argument("--dataset", required=True, help="Ruta al archivo CSV o Parquet generado por build_dataset")
    parser.add_argument("--pueblos", nargs="*", help="Lista de códigos o nombres de pueblos a incluir")
    parser.add_argument("--departamentos", nargs="*", help="Lista de departamentos a incluir")
    parser.add_argument("--municipios", nargs="*", help="Lista de municipios (Municipio_limpio) a incluir")
    parser.add_argument("--output", required=True, help="Ruta donde guardar el archivo Excel generado")
    args = parser.parse_args(argv)
    export_excel(
        dataset_path=args.dataset,
        pueblos=args.pueblos,
        departamentos=args.departamentos,
        municipios=args.municipios,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()