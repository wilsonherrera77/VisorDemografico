# Toolkit para Visor de Pueblos Indígenas CNPV‑2018

Este repositorio contiene un **toolkit** completo para convertir el archivo Excel
**“visor‑pueblos‑indígenas‑06‑2021.xlsx”** del DANE (con los resultados del
Censo Nacional de Población y Vivienda 2018) en un dataset tabular limpio,
generar indicadores estadísticos a nivel municipal y departamental, y poner
estos datos a disposición a través de un **dashboard web** y un **Excel** listo
para análisis offline.

## Estructura del proyecto

```text
indigenas_toolkit/
├── api/                # API ligera construida con FastAPI
│   └── main.py         # Endpoints para opciones, agregados, detalle por pueblo y descarga
├── app_streamlit/      # Alternativa rápida de dashboard en Streamlit
│   └── app.py
├── dataset/            # Construcción y exportación del dataset
│   ├── __init__.py
│   ├── build_dataset.py
│   └── export_excel.py
├── web_frontend/       # Frontend HTML/JS para el dashboard web
│   └── index.html
└── README.md           # Este archivo
```

## Prerrequisitos

1. **Python 3.8 o superior** con las siguientes bibliotecas instaladas:

   - `pandas` (≥ 1.5)
   - `numpy`
   - `fastapi`
   - `uvicorn`
   - `openpyxl` (para escribir Excel)
   - `streamlit` (solo si desea usar el dashboard alternativo)

   Puede instalar estas dependencias ejecutando:

   ```bash
   pip install pandas numpy fastapi uvicorn openpyxl streamlit
   ```

2. El archivo original **visor‑pueblos‑indígenas‑06‑2021.xlsx** debe estar disponible
   para construir el dataset. Copie o descargue ese archivo en la carpeta en
   la que ejecutará los scripts.

## Paso 1: Construcción del dataset canónico

Use `dataset/build_dataset.py` para leer el libro Excel del DANE, limpiar
los nombres de municipios y deduplicar los pueblos, y generar un archivo CSV
(y opcionalmente Parquet) con la base de datos a nivel de municipio × pueblo.

### Ejemplo de uso

```bash
python -m indigenas_toolkit.dataset.build_dataset \
       --input visor-pueblos-indigenas-06-2021.xlsx \
       --output_csv data/base_municipal_pueblo.csv \
       --output_parquet data/base_municipal_pueblo.parquet
```

Este script lee las hojas **3** (base) y **1** (catálogo de pueblos) del
libro Excel, crea una columna `Municipio_limpio` eliminando el departamento
entre paréntesis en el nombre de municipio, construye una clave compuesta
`KeyMpio = Departamento + '|' + Municipio_limpio` y deduplica los pueblos
utilizando el código `PA11_COD_ETNIA`. El resultado contiene las siguientes
columnas:

| Columna            | Descripción                                                       |
|--------------------|-------------------------------------------------------------------|
| Departamento       | Nombre del departamento                                           |
| Municipio_limpio   | Nombre del municipio sin el sufijo del departamento               |
| KeyMpio            | Clave compuesta Departamento|Municipio                            |
| PA11_COD_ETNIA     | Código del pueblo indígena (variable del microdato CNPV-2018)     |
| Pueblo             | Nombre del pueblo indígena                                        |
| POBLACION_2018     | Población censada en 2018 perteneciente a ese pueblo y municipio  |

Si además cuenta con la codificación **DIVIPOLA** oficial, puede unirla con
la clave `Departamento/Municipio_limpio` para disponer de los códigos
municipales y departamentales. DIVIPOLA asigna un código de dos dígitos a
cada departamento y uno de cinco dígitos a cada municipio; las primeras dos
posiciones indican el departamento y las tres restantes identifican el
municipio dentro del departamento【262165748964348†L159-L166】.

## Paso 2: Generación de indicadores y exportación a Excel

El módulo `dataset/export_excel.py` permite filtrar el dataset por listas
específicas de pueblos, municipios o departamentos y generar un libro Excel
con cinco hojas:

1. **base_municipal_pueblo** – Base filtrada Municipio × Pueblo.
2. **indicadores_municipio** – Indicadores calculados por municipio (población total
   indígena, número de pueblos presentes, ranking de pueblos y tres índices de
   diversidad: **HHI**, **Simpson (1 – HHI)** y **Shannon**).
3. **indicadores_departamento** – Indicadores agregados por departamento
   (población indígena total, número de pueblos y número de municipios con
   presencia indígena).
4. **matriz_mpio_x_pueblo** – Tabla dinámica Municipio × Pueblo con la
   población total de cada combinación.
5. **diccionario** – Diccionario de variables y descripciones.

### Ejemplo de uso

```bash
python -m indigenas_toolkit.dataset.export_excel \
       --dataset data/base_municipal_pueblo.csv \
       --output reporte_total.xlsx

# Filtro de ejemplo: pueblo 570 (Sáliva) y departamentos Meta y Vichada
python -m indigenas_toolkit.dataset.export_excel \
       --dataset data/base_municipal_pueblo.csv \
       --pueblos 570 \
       --departamentos META VICHADA \
       --output reporte_saliva_meta_vichada.xlsx
```

Al ejecutar el script se generan las hojas en el archivo Excel destino.

## Paso 3: API ligera con FastAPI

El directorio `api/` contiene una aplicación FastAPI que expone cuatro
endpoints para consumir el dataset y los indicadores:

| Endpoint            | Descripción |
|---------------------|-------------|
| `/options`          | Devuelve listas de pueblos, municipios y departamentos disponibles. |
| `/aggregate`        | Recibe listas de pueblos, municipios y departamentos y devuelve KPIs agregados: población total, número de pueblos, índices de diversidad. |
| `/by_pueblo`        | Devuelve una tabla con la población y la participación de cada pueblo en la selección. |
| `/download_excel`   | Genera y descarga el Excel con las cinco hojas filtradas según la selección. |

### Levantar la API

```bash
uvicorn indigenas_toolkit.api.main:app --reload
```

Por defecto, la API cargará el dataset ubicado en `data/base_municipal_pueblo.csv`. Puede
cambiar esta ruta modificando la variable `DATA_PATH` en `api/main.py`.

## Paso 4: Frontend web simple

El archivo `web_frontend/index.html` implementa un dashboard sencillo en HTML/JS
que consume la API. Muestra tres selectores de multiselección (Pueblo,
Municipio y Departamento), un conjunto de indicadores clave y una tabla de
resultados por pueblo. Incluye un botón para descargar el Excel filtrado.

Para usarlo, abra el archivo en su navegador y configure la URL base de la API
(por defecto `http://localhost:8000`). También puede desplegar este archivo
en cualquier servidor web estático y apuntarlo a su instancia de la API.

## Paso 5 (opcional): Dashboard en Streamlit

Si prefiere un tablero rápido y listo para compartir, puede utilizar
`app_streamlit/app.py`. Este script crea una interfaz con selectores para
pueblos, municipios y departamentos; calcula los indicadores y muestra una
tabla dinámica. También ofrece un botón para descargar el Excel filtrado.

Ejecute el tablero con:

```bash
streamlit run indigenas_toolkit/app_streamlit/app.py
```

## Extensiones recomendadas

* **DIVIPOLA**: Agregue los códigos oficiales de departamento y municipio al
  dataset final para facilitar su unión con cartografía. La codificación
  DIVIPOLA utiliza dos dígitos para el departamento y cinco para el
  municipio; los tres últimos dígitos identifican el municipio dentro del
  departamento【262165748964348†L159-L166】.
* **Población total municipal**: Vincule la población total del municipio
  (CNPV‑2018) para calcular porcentajes de población indígena y tasas por
  10 000 habitantes.
* **Salida geoespacial**: Exporte la base agregada en formato GeoJSON o
  Shapefile, utilizando los códigos DIVIPOLA como clave geográfica.

## Notas

* Este toolkit se basa en la estructura del libro Excel proporcionado por el
  DANE. Si el formato cambia en futuras versiones, puede necesitar ajustar
  las funciones de lectura en `build_dataset.py`.
* El código está documentado para facilitar su mantenimiento. Consulte los
  docstrings y comentarios en cada módulo para más detalles.