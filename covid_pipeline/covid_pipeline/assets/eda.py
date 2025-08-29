import pandas as pd
from dagster import asset

@asset
def tabla_perfilado(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
    Asset de Dagster que genera un perfilado básico del dataset de COVID-19.

    Este asset consume el DataFrame producido por `leer_datos`, y construye un 
    resumen con estadísticas descriptivas clave para validar y explorar los datos:

    1. **Columnas y tipos de datos (`tipos`)**  
       - Extrae la lista de columnas del DataFrame con su tipo (`dtype`).
       - Permite verificar la coherencia de los tipos leídos.

    2. **Valores mínimos y máximos de `new_cases` (`min_max`)**  
       - Calcula el mínimo y máximo de casos nuevos reportados.
       - Útil para detectar valores atípicos extremos o errores de carga.

    3. **Porcentaje de valores nulos (`nulos`)**  
       - Calcula el porcentaje de celdas vacías en `new_cases` y `people_vaccinated`.
       - Ayuda a dimensionar problemas de calidad de datos.

    4. **Rango de fechas (`fechas`)**  
       - Encuentra la fecha mínima y máxima disponible en la columna `date`.
       - Permite conocer la cobertura temporal del dataset.

    El resultado final se concatena en un único DataFrame (`perfil`) y se guarda 
    como `tabla_perfilado.csv` en la raíz del proyecto, para su inspección manual
    y entrega como artefacto del reporte.

    Returns:
        pd.DataFrame: Tabla con el perfilado básico de los datos.
    """
    df = leer_datos

    # --- Columnas y tipos de datos ---
    tipos = pd.DataFrame(
        df.dtypes, columns=["dtype"]
    ).reset_index().rename(columns={"index": "columna"})

    # --- Min y max de casos diarios ---
    min_max = pd.DataFrame({
        "columna": ["new_cases_min", "new_cases_max"],
        "valor": [df["new_cases"].min(skipna=True), df["new_cases"].max(skipna=True)]
    })

    # --- Porcentaje de nulos ---
    nulos = pd.DataFrame({
        "columna": ["pct_null_new_cases", "pct_null_people_vaccinated"],
        "valor": [
            df["new_cases"].isna().mean() * 100,
            df["people_vaccinated"].isna().mean() * 100
        ]
    })

    # --- Rango de fechas ---
    fechas = pd.DataFrame({
        "columna": ["fecha_min", "fecha_max"],
        "valor": [
            pd.to_datetime(df["date"], errors="coerce").min(),
            pd.to_datetime(df["date"], errors="coerce").max()
        ]
    })

    # --- Unir todos los resultados ---
    perfil = pd.concat([tipos, min_max, nulos, fechas], ignore_index=True)

    # --- Guardar como CSV (artefacto ligero para el repositorio) ---
    perfil.to_csv("tabla_perfilado.csv", index=False)

    return perfil
