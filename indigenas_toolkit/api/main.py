"""API ligera para el visor de pueblos indígenas CNPV‑2018.

Esta aplicación FastAPI expone varias rutas para consultar listas de
pueblos, municipios y departamentos (`/options`), calcular indicadores
agregados sobre un subconjunto de la base (`/aggregate`), obtener una
tabla por pueblo (`/by_pueblo`) y descargar un Excel filtrado
(`/download_excel`).

Los filtros se pasan como parámetros de consulta (query params) y son
opcionalmente multivaluados:

    /aggregate?pueblos=570&pueblos=571&departamentos=META&municipios=PUERTO%20LOPEZ

Si no se especifica ningún filtro, se consideran todos los registros.

"""

from __future__ import annotations

import io
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd

from ..dataset.export_excel import (
    _filter_dataset,
    _compute_indicadores_municipio,
    _compute_indicadores_departamento,
    _build_diccionario,
    _compute_matriz_mpio_pueblo,
    export_excel,
)


# Ruta al dataset. Puede ajustarse aquí o leerla de una variable de entorno.
DATA_PATH = "data/base_municipal_pueblo.csv"


def load_data(path: str = DATA_PATH) -> pd.DataFrame:
    """Carga el dataset desde CSV o Parquet y lo cachea en memoria.

    Se utiliza una variable global para almacenar el DataFrame y evitar
    recargas repetidas durante el ciclo de vida de la aplicación. Si se
    modifica el contenido del archivo, será necesario reiniciar la API.
    """
    # La variable de caché se almacena en atributos de la función para evitar
    # el uso de globales a nivel de módulo.
    if not hasattr(load_data, "_cache"):
        if path.lower().endswith(".csv"):
            df = pd.read_csv(path)
        elif path.lower().endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            raise ValueError("El dataset debe ser un archivo CSV o Parquet")
        load_data._cache = df
    return load_data._cache


app = FastAPI(title="API Visor Pueblos Indígenas CNPV‑2018",
              description="Endpoints para consultar opciones, indicadores y descargar reportes filtrados",
              version="1.0.0")


@app.get("/options")
def get_options() -> dict:
    """Devuelve listas de pueblos, departamentos y municipios disponibles.

    La lista de pueblos incluye el nombre del pueblo; la de municipios se
    basa en la columna `Municipio_limpio`.
    """
    df = load_data()
    pueblos = sorted(df["Pueblo"].dropna().unique().tolist())
    departamentos = sorted(df["Departamento"].dropna().unique().tolist())
    municipios = sorted(df["Municipio_limpio"].dropna().unique().tolist())
    return {
        "pueblos": pueblos,
        "departamentos": departamentos,
        "municipios": municipios,
    }


@app.get("/aggregate")
def aggregate(
    pueblos: Optional[List[str]] = Query(None, description="Lista de códigos o nombres de pueblos"),
    departamentos: Optional[List[str]] = Query(None, description="Lista de departamentos"),
    municipios: Optional[List[str]] = Query(None, description="Lista de municipios (Municipio_limpio)"),
) -> dict:
    """Calcula KPIs agregados para la selección dada.

    Retorna la población indígena total, el número de pueblos presentes y los
    índices de diversidad (HHI, Simpson, Shannon). Si la selección está
    vacía se devuelve un error HTTP 404.
    """
    df = load_data()
    df_filt = _filter_dataset(df, pueblos=pueblos, departamentos=departamentos, municipios=municipios)
    if df_filt.empty:
        raise HTTPException(status_code=404, detail="La selección no contiene registros")
    total_pop = int(df_filt["POBLACION_2018"].sum())
    num_pueblos = int(df_filt["PA11_COD_ETNIA"].nunique())
    # Calcular diversidad global
    pueblos_series = df_filt.groupby("Pueblo", dropna=False)["POBLACION_2018"].sum()
    shares = pueblos_series / total_pop if total_pop > 0 else pd.Series(dtype=float)
    hhi = float((shares ** 2).sum()) if not shares.empty else None
    simpson = float(1 - hhi) if hhi is not None else None
    shannon = float(-(shares[shares > 0] * np.log(shares[shares > 0])).sum()) if not shares.empty else None
    return {
        "poblacion_indigena_total": total_pop,
        "num_pueblos": num_pueblos,
        "HHI": hhi,
        "Simpson": simpson,
        "Shannon": shannon,
    }


@app.get("/by_pueblo")
def by_pueblo(
    pueblos: Optional[List[str]] = Query(None),
    departamentos: Optional[List[str]] = Query(None),
    municipios: Optional[List[str]] = Query(None),
) -> List[dict]:
    """Devuelve una tabla con la población y la participación de cada pueblo.

    La participación se calcula como la población del pueblo dividido por la
    población total en la selección. Devuelve un listado de diccionarios
    ordenado de mayor a menor por población.
    """
    df = load_data()
    df_filt = _filter_dataset(df, pueblos=pueblos, departamentos=departamentos, municipios=municipios)
    if df_filt.empty:
        raise HTTPException(status_code=404, detail="La selección no contiene registros")
    total_pop = df_filt["POBLACION_2018"].sum()
    pueblos_series = df_filt.groupby("Pueblo", dropna=False)["POBLACION_2018"].sum().sort_values(ascending=False)
    result = []
    for pueblo, pop in pueblos_series.items():
        participacion = float(pop / total_pop) if total_pop > 0 else 0.0
        result.append({
            "Pueblo": pueblo,
            "poblacion": int(pop),
            "participacion": participacion,
        })
    return result


@app.get("/download_excel")
def download_excel(
    pueblos: Optional[List[str]] = Query(None),
    departamentos: Optional[List[str]] = Query(None),
    municipios: Optional[List[str]] = Query(None),
) -> StreamingResponse:
    """Genera un archivo Excel filtrado y lo devuelve como descarga.

    Si no existen registros para la selección se devuelve un error 404.
    """
    df = load_data()
    df_filt = _filter_dataset(df, pueblos=pueblos, departamentos=departamentos, municipios=municipios)
    if df_filt.empty:
        raise HTTPException(status_code=404, detail="La selección no contiene registros")
    # Generar Excel en memoria
    output_bytes = export_excel(dataset_path=DATA_PATH,
                               pueblos=pueblos,
                               departamentos=departamentos,
                               municipios=municipios,
                               output_path=None)
    buffer = io.BytesIO(output_bytes)
    return StreamingResponse(buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=report.xlsx"})
