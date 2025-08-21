"""Dashboard rápido en Streamlit para el visor de pueblos indígenas.

Este módulo utiliza `streamlit` para ofrecer una interfaz interactiva
con selectores de pueblo, departamento y municipio. Calcula KPIs
agregados y muestra una tabla con la población y la participación de
cada pueblo dentro de la selección. También incluye un botón para
descargar un archivo Excel con el subconjunto filtrado e indicadores.

Para ejecutar el dashboard:

    streamlit run indigenas_toolkit/app_streamlit/app.py

Necesita tener instalada la biblioteca `streamlit`.
"""

from __future__ import annotations

import io
from typing import List, Optional

import pandas as pd
import streamlit as st

from ..dataset.export_excel import (
    _filter_dataset,
    _compute_indicadores_municipio,
    export_excel,
)


# Ruta al dataset. Ajuste esta variable según su instalación
DATA_PATH = "data/base_municipal_pueblo.csv"


@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    """Carga el dataset desde CSV o Parquet y lo almacena en caché."""
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    elif path.lower().endswith(".parquet"):
        return pd.read_parquet(path)
    else:
        raise ValueError("El dataset debe ser un archivo CSV o Parquet")


def main() -> None:
    st.set_page_config(page_title="Visor Pueblos Indígenas", layout="wide")
    st.title("Visor de Pueblos Indígenas – CNPV‑2018")

    df = load_data(DATA_PATH)
    # Selectores
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_pueblos = st.multiselect("Pueblo(s)", sorted(df["Pueblo"].dropna().unique().tolist()))
    with col2:
        sel_deps = st.multiselect("Departamento(s)", sorted(df["Departamento"].dropna().unique().tolist()))
    with col3:
        sel_muns = st.multiselect("Municipio(s)", sorted(df["Municipio_limpio"].dropna().unique().tolist()))

    # Filtrar
    filt_df = _filter_dataset(df, pueblos=sel_pueblos, departamentos=sel_deps, municipios=sel_muns)
    if filt_df.empty:
        st.warning("La selección no contiene registros.")
        return
    # KPIs globales
    total_pop = int(filt_df["POBLACION_2018"].sum())
    num_pueblos = int(filt_df["PA11_COD_ETNIA"].nunique())
    pueblos_series = filt_df.groupby("Pueblo", dropna=False)["POBLACION_2018"].sum()
    shares = pueblos_series / total_pop if total_pop > 0 else pd.Series(dtype=float)
    hhi = float((shares ** 2).sum()) if not shares.empty else None
    simpson = float(1 - hhi) if hhi is not None else None
    shannon = float(-(shares[shares > 0] * np.log(shares[shares > 0])).sum()) if not shares.empty else None

    st.subheader("Indicadores globales de la selección")
    kpi_cols = st.columns(5)
    kpi_cols[0].metric("Población total", f"{total_pop:,}")
    kpi_cols[1].metric("Nº de pueblos", f"{num_pueblos}")
    kpi_cols[2].metric("HHI", f"{hhi:.3f}" if hhi is not None else "N/A")
    kpi_cols[3].metric("Simpson", f"{simpson:.3f}" if simpson is not None else "N/A")
    kpi_cols[4].metric("Shannon", f"{shannon:.3f}" if shannon is not None else "N/A")

    # Tabla por pueblo
    indicadores_mpio = _compute_indicadores_municipio(filt_df)
    # Tabla por pueblo: nos interesa Población y Participación
    total_pop_local = total_pop
    pueblo_table = (
        filt_df.groupby("Pueblo", dropna=False)["POBLACION_2018"].sum()
        .reset_index()
        .rename(columns={"POBLACION_2018": "Población"})
    )
    pueblo_table["Participación"] = pueblo_table["Población"] / total_pop_local
    pueblo_table = pueblo_table.sort_values(by="Población", ascending=False)

    st.subheader("Tabla por pueblo")
    st.dataframe(pueblo_table)

    # Botón de descarga
    excel_bytes = export_excel(
        dataset_path=DATA_PATH,
        pueblos=sel_pueblos or None,
        departamentos=sel_deps or None,
        municipios=sel_muns or None,
        output_path=None,
    )
    st.download_button(
        label="Descargar Excel (selección)",
        data=excel_bytes,
        file_name="reporte_pueblos_indigenas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()