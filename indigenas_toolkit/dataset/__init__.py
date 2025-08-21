"""Módulo de utilidades para la construcción y exportación del dataset CNPV‑2018.

Proporciona funciones para leer el archivo Excel original del visor de pueblos
indígenas, limpiarlo y generar un dataset tabular (`build_dataset`), así como
para filtrar ese dataset y exportar un archivo Excel con indicadores
agregados (`export_excel`).

"""

from .build_dataset import build_dataset
from .export_excel import export_excel

__all__ = ["build_dataset", "export_excel"]