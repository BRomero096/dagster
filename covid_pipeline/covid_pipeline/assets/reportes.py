import os
import pandas as pd
from dagster import asset

@asset
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame,
):
    """
    Asset de Dagster que exporta los resultados finales del pipeline a un archivo Excel.

    Entradas:
        - datos_procesados (pd.DataFrame):
            Dataset limpio y filtrado (Ecuador + país comparativo) listo para análisis.
        - metrica_incidencia_7d (pd.DataFrame):
            Resultado de la métrica de incidencia acumulada a 7 días por 100k.
        - metrica_factor_crec_7d (pd.DataFrame):
            Resultado de la métrica de factor de crecimiento semanal (7 días).

    Salida:
        - Crea el archivo `reporte_covid.xlsx` (ligero) con tres hojas:
            * "datos_procesados"
            * "incidencia_7d"
            * "factor_crec_7d"

    Notas:
        - Este archivo puede versionarse en el repo (artefacto final de entrega).
        - No incluye datos crudos pesados, solo resultados procesados y métricas.
    """
    # (Opcional) Si quieres guardar siempre en una carpeta específica:
    # os.makedirs("reportes", exist_ok=True)
    # out_path = os.path.join("reportes", "reporte_covid.xlsx")
    # En ese caso, actualiza el ExcelWriter a `out_path` y ajusta tu .gitignore para permitirlo.

    with pd.ExcelWriter("reporte_covid.xlsx") as writer:
        datos_procesados.to_excel(writer, sheet_name="datos_procesados", index=False)
        metrica_incidencia_7d.to_excel(writer, sheet_name="incidencia_7d", index=False)
        metrica_factor_crec_7d.to_excel(writer, sheet_name="factor_crec_7d", index=False)
