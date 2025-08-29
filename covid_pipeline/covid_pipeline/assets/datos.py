"""
Módulo de ingestión y preprocesamiento de datos COVID desde Our World in Data.
Define dos assets de Dagster:

1. leer_datos:
   - Descarga el dataset crudo desde OWID (CSV).
   - Limpia y normaliza encabezados de columnas.
   - Convierte tipos numéricos.
   - Asegura presencia de columnas clave mínimas.

2. datos_procesados:
   - Toma los datos de `leer_datos`.
   - Valida columnas requeridas.
   - Elimina filas con valores nulos en métricas críticas.
   - Deduplica (location, date).
   - Filtra solo los países de interés (base y comparativo).
   - Devuelve un DataFrame reducido con columnas relevantes.
"""

import io
import os
import pandas as pd
import requests
from dagster import asset, OpExecutionContext

# URL oficial de OWID (Our World in Data)
OWID_URL = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"

# País de referencia y país comparativo (pueden configurarse vía variables de entorno)
PAIS_BASE = os.getenv("PAIS_BASE", "Ecuador")
PAIS_COMPARATIVO = os.getenv("PAIS_COMPARATIVO", "Peru")
PAISES = [PAIS_BASE, PAIS_COMPARATIVO]

# Diccionario de equivalencias de nombres de columnas
# Permite reconocer encabezados con distintos alias en datasets externos
ALT = {
    "location": ["location", "entity", "country", "country_name", "location_name", "place"],
    "date": ["date", "day"],
    "new_cases": ["new_cases", "new_cases_daily"],
    "people_vaccinated": ["people_vaccinated", "people_with_at_least_one_dose"],
    "population": ["population", "pop"],
}


def _canon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas:
    - Elimina espacios y caracteres raros (BOM).
    - Convierte a minúsculas.
    - Renombra columnas según alias en `ALT` para tener nombres canónicos.
    """
    # Limpieza de encabezados
    df.columns = (
        pd.Index(df.columns)
        .map(lambda c: c.strip())
        .map(lambda c: c.replace("\ufeff", ""))  # elimina BOM si existe
        .str.lower()
    )

    # Renombrado flexible con base en equivalencias ALT
    rename_map = {}
    for canon, candidates in ALT.items():
        found = next((c for c in candidates if c in df.columns), None)
        if found and found != canon:
            rename_map[found] = canon

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _require(df: pd.DataFrame, cols: list[str]):
    """
    Verifica que el DataFrame contenga todas las columnas requeridas.
    Si alguna falta → lanza un KeyError con mensaje claro.
    """
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Faltan columnas requeridas tras normalizar: {missing}")


@asset
def leer_datos(context: OpExecutionContext) -> pd.DataFrame:
    """
    Asset: Descarga y normaliza el dataset de OWID.
    Pasos:
    1. Descarga CSV desde OWID_URL.
    2. Limpia nombres de columnas (_canon).
    3. Convierte a numéricos columnas clave (new_cases, people_vaccinated, population).
    4. Garantiza que al menos existan 'location' y 'date'.
    """
    # Descarga con requests
    r = requests.get(OWID_URL, timeout=60)
    r.raise_for_status()

    # Cargar CSV en DataFrame
    df = pd.read_csv(io.StringIO(r.text))
    df = _canon(df)

    # Logging de columnas para depuración en Dagster
    context.log.info(f"Columnas leídas: {list(df.columns)}")

    # Conversión a numérico de columnas relevantes
    for col in ["new_cases", "people_vaccinated", "population"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Validación mínima
    _require(df, ["location", "date"])

    return df


@asset(deps=[leer_datos])
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
    Asset: Procesa los datos crudos de `leer_datos`.
    Pasos:
    1. Verifica columnas requeridas (location, date, métricas básicas).
    2. Elimina filas con NA en casos/vacunación.
    3. Elimina duplicados por (location, date).
    4. Filtra solo los países definidos en PAISES.
    5. Devuelve DataFrame reducido con columnas relevantes.
    """
    df = leer_datos.copy()

    # Confirmar columnas necesarias para análisis
    _require(df, ["location", "date", "new_cases", "people_vaccinated", "population"])

    # Limpieza de datos
    df = df.dropna(subset=["new_cases", "people_vaccinated"])
    df = df.drop_duplicates(subset=["location", "date"])
    df = df[df["location"].isin(PAISES)]

    # Selección final de columnas de interés
    cols = ["location", "date", "new_cases", "people_vaccinated", "population"]
    return df[cols].reset_index(drop=True)
