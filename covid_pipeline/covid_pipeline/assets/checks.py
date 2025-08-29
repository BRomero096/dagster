"""
Módulo de checks de calidad de datos para Dagster.
Contiene validaciones sobre el asset de entrada `leer_datos`
y sobre métricas de salida (`metrica_incidencia_7d`, `metrica_factor_crec_7d`).
Algunos checks son bloqueantes (FAIL) y otros son advertencias (no detienen el run).
"""

import datetime as dt
import pandas as pd
from dagster import asset_check, AssetCheckResult


def _columns_missing(df: pd.DataFrame, cols: list[str]) -> list[str]:
    """
    Devuelve la lista de columnas que no están presentes en el DataFrame.
    Se usa como utilidad en varios checks para validar estructura mínima.
    """
    return [c for c in cols if c not in df.columns]


# ----------------------------------------------------------------------
# -------------------- CHECKS DE ENTRADA -------------------------------
# ----------------------------------------------------------------------

@asset_check(asset="leer_datos")
def max_date_not_future(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Verifica que la fecha máxima en la columna `date` no sea futura.
    - Si no existe la columna → FAIL.
    - Si la fecha máxima es NaT → FAIL.
    - Si hay fecha futura → advertencia (no bloquea el run).
    """
    if "date" not in leer_datos.columns:
        return AssetCheckResult(passed=False, description="Columna 'date' no existe")

    maxd = pd.to_datetime(leer_datos["date"], errors="coerce").max()
    if pd.isna(maxd):
        return AssetCheckResult(passed=False, description="max(date)=NaT")

    ok = True  # siempre pasa, aunque sea futura → se marca como warning
    nota = "> hoy (advertencia)" if maxd.date() > dt.date.today() else "ok"
    return AssetCheckResult(passed=ok, description=f"max(date)={maxd.date()} {nota}")


@asset_check(asset="leer_datos")
def keys_not_null(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Comprueba que las columnas clave (`location`, `date`, `population`)
    no contengan valores nulos.
    - Si faltan columnas → FAIL.
    - Si hay nulos → advertencia (pasa con warning).
    """
    need = ["location", "date", "population"]
    miss = _columns_missing(leer_datos, need)
    if miss:
        return AssetCheckResult(passed=False, description=f"Faltan columnas: {miss}")

    subset = leer_datos[need]
    nnull = int(subset.isnull().any(axis=1).sum())
    return AssetCheckResult(
        passed=True,  # siempre pasa, pero deja la advertencia
        description=f" {nnull} filas con nulos en claves (advertencia)"
    )


@asset_check(asset="leer_datos")
def unique_loc_date(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Valida que la combinación (location, date) sea única.
    - Si faltan columnas → FAIL.
    - Si hay duplicados → FAIL.
    """
    need = ["location", "date"]
    miss = _columns_missing(leer_datos, need)
    if miss:
        return AssetCheckResult(passed=False, description=f"Faltan columnas: {miss}")

    dup = int(leer_datos.duplicated(subset=need).sum())
    return AssetCheckResult(passed=bool(dup == 0),
                            description=f"duplicados (location,date): {dup}")


@asset_check(asset="leer_datos")
def population_positive(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Comprueba que la población (`population`) sea mayor a cero.
    - Si falta columna → FAIL.
    - Si hay valores <= 0 → FAIL.
    """
    if "population" not in leer_datos.columns:
        return AssetCheckResult(passed=False, description="Falta columna 'population'")

    pop = pd.to_numeric(leer_datos["population"], errors="coerce")
    nonpos = int((pop <= 0).sum())
    return AssetCheckResult(passed=bool(nonpos == 0),
                            description=f"population<=0: {nonpos}")


@asset_check(asset="leer_datos")
def new_cases_nonnegative(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """
    Valida que la columna `new_cases` no tenga valores negativos.
    - Si falta columna → FAIL.
    - Si hay valores < 0 → FAIL.
    """
    if "new_cases" not in leer_datos.columns:
        return AssetCheckResult(passed=False, description="Falta columna 'new_cases'")

    vals = pd.to_numeric(leer_datos["new_cases"], errors="coerce")
    neg = int((vals < 0).sum())
    return AssetCheckResult(passed=bool(neg == 0),
                            description=f"new_cases<0: {neg}")


# ----------------------------------------------------------------------
# -------------------- CHECKS DE SALIDA --------------------------------
# ----------------------------------------------------------------------

@asset_check(asset="metrica_incidencia_7d")
def incidencia_en_rango(metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    """
    Verifica que la métrica de incidencia a 7 días esté en el rango [0,2000].
    - Si falta columna → FAIL.
    - Si hay valores fuera de rango → FAIL.
    """
    if "incidencia_7d" not in metrica_incidencia_7d.columns:
        return AssetCheckResult(passed=False, description="Falta 'incidencia_7d'")

    serie = pd.to_numeric(metrica_incidencia_7d["incidencia_7d"], errors="coerce")
    fuera = int(((serie < 0) | (serie > 2000)).sum())
    return AssetCheckResult(passed=bool(fuera == 0),
                            description=f"valores fuera de [0,2000]: {fuera}")


@asset_check(asset="metrica_factor_crec_7d")
def factor_crec_valido(metrica_factor_crec_7d: pd.DataFrame) -> AssetCheckResult:
    """
    Verifica que el factor de crecimiento 7d sea válido:
    - Debe ser > 0.
    - No debe ser NaN ni infinito.
    Los registros inválidos no bloquean ejecución → se reportan como advertencia.
    """
    if "factor_crec_7d" not in metrica_factor_crec_7d.columns:
        return AssetCheckResult(passed=False, description="Falta 'factor_crec_7d'")

    serie = pd.to_numeric(metrica_factor_crec_7d["factor_crec_7d"], errors="coerce")
    invalidos = int(serie.isna().sum() +
                    (~(serie.replace([float("inf"), -float("inf")], pd.NA) > 0)).sum())

    return AssetCheckResult(
        passed=True,  # warning, no bloquea
        description=f"⚠️ {invalidos} registros inválidos (NaN/inf/<=0 en ventanas tempranas)"
    )


# ----------------------------------------------------------------------
# -------------------- REGISTRO DE TODOS LOS CHECKS --------------------
# ----------------------------------------------------------------------

def get_all_checks():
    """
    Devuelve la lista de todos los checks definidos en este módulo.
    Útil para registrarlos fácilmente en definitions.py.
    """
    return [
        max_date_not_future,
        keys_not_null,
        unique_loc_date,
        population_positive,
        new_cases_nonnegative,
        incidencia_en_rango,
        factor_crec_valido,
    ]
