import pandas as pd
from dagster import asset

@asset
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la incidencia acumulada de COVID-19 a 7 días por cada 100.000 habitantes.

    Fórmulas:
        - incidencia_diaria = (new_cases / population) * 100.000
        - incidencia_7d = promedio móvil de 7 días de la incidencia_diaria, 
          calculado por cada país.

    Entradas:
        datos_procesados (pd.DataFrame):
            Debe contener columnas ["date", "location", "new_cases", "population"].

    Salidas:
        pd.DataFrame con columnas:
            - date: fecha
            - location: país
            - incidencia_7d: incidencia acumulada semanal por 100.000 hab.
    """
    df = datos_procesados.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["location", "date"])

    # Evitar división por cero en población
    df["incidencia_diaria"] = (df["new_cases"] / df["population"].replace({0: pd.NA})) * 100000

    # Promedio móvil de 7 días (requiere al menos 7 observaciones)
    df["incidencia_7d"] = (
        df.groupby("location")["incidencia_diaria"]
          .transform(lambda s: s.rolling(7, min_periods=7).mean())
    )

    # Retornar solo filas donde la incidencia está calculada
    out = df.loc[df["incidencia_7d"].notna(), ["date", "location", "incidencia_7d"]]
    return out.reset_index(drop=True)


@asset(deps=["datos_procesados"])
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el factor de crecimiento semanal (7 días).

    Fórmulas:
        - casos_semana = suma de casos nuevos en la ventana móvil de 7 días.
        - casos_semana_prev = casos de la semana previa (shift 7 días).
        - factor_crec_7d = casos_semana / casos_semana_prev.

    Entradas:
        datos_procesados (pd.DataFrame):
            Debe contener columnas ["date", "location", "new_cases"].

    Salidas:
        pd.DataFrame con columnas:
            - semana_fin: fecha de cierre de la semana móvil
            - location: país
            - casos_semana: número total de casos en esa semana
            - factor_crec_7d: ratio de crecimiento respecto a la semana previa
              (valores >1 indican crecimiento, <1 decrecimiento).
    """
    df = datos_procesados.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Casos semanales acumulados (ventana móvil de 7 días)
    df["casos_semana"] = df.groupby("location")["new_cases"].transform(
        lambda x: x.rolling(7, min_periods=1).sum()
    )

    # Casos de la semana anterior (desplazado 7 días)
    df["casos_semana_prev"] = df.groupby("location")["casos_semana"].shift(7)

    # Factor de crecimiento (con división segura)
    df["factor_crec_7d"] = df["casos_semana"] / df["casos_semana_prev"]
    df["factor_crec_7d"] = df["factor_crec_7d"].replace([float("inf"), -float("inf")], pd.NA)

    # Preparar salida
    out = df[["date", "location", "casos_semana", "factor_crec_7d"]].dropna()
    out = out.rename(columns={"date": "semana_fin"})  # la fecha corresponde al fin de la ventana
    return out.reset_index(drop=True)
