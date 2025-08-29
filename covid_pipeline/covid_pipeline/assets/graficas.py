import os
import matplotlib.pyplot as plt
import pandas as pd
from dagster import asset, MetadataValue

# NOTA: No configuramos estilos ni colores a propósito.
# (Requisito: usar matplotlib simple, sin seaborn y sin especificar colores.)

def _ensure_dir(path: str):
    """
    Crea un directorio si no existe.
    Se usa para garantizar que la carpeta `reportes/plots/` esté disponible
    antes de guardar las imágenes.
    """
    os.makedirs(path, exist_ok=True)


def _save_lineplot(df: pd.DataFrame, x: str, y: str, hue: str, title: str, out_path: str):
    """
    Función auxiliar que genera un gráfico de líneas a partir de un DataFrame.

    Parámetros:
        df (pd.DataFrame): Datos a graficar.
        x (str): Columna que irá en el eje X (ej. fechas).
        y (str): Columna que irá en el eje Y (ej. incidencia).
        hue (str): Columna para separar por grupos (ej. país/location).
        title (str): Título del gráfico.
        out_path (str): Ruta donde se guardará el gráfico en formato PNG.

    - Se ordenan los datos por `hue` y `x` para evitar saltos en las líneas.
    - Se dibuja una curva por cada valor distinto en `hue`.
    - Se guarda el gráfico como imagen PNG en `out_path`.
    """
    df = df.sort_values([hue, x])
    plt.figure()  # un gráfico por figura
    for loc, g in df.groupby(hue, dropna=False):
        plt.plot(g[x], g[y], label=str(loc))
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


@asset
def grafica_incidencia_7d(context, metrica_incidencia_7d: pd.DataFrame) -> str:
    """
    Asset de Dagster que genera un gráfico de líneas de la incidencia COVID-19 a 7 días.

    - Input: DataFrame `metrica_incidencia_7d` (con columnas: `date`, `location`, `incidencia_7d`).
    - Proceso:
        1. Convierte la columna `date` a tipo datetime.
        2. Usa `_save_lineplot` para graficar `incidencia_7d` contra `date`.
        3. Traza una línea por cada país (`location`).
    - Output: Ruta del archivo PNG generado en `reportes/plots/incidencia_7d.png`.

    Además, adjunta metadatos a Dagster:
        - `png`: ruta al archivo.
        - `preview`: vista previa del gráfico en la UI de Dagster.
    """
    _ensure_dir("reportes/plots")
    out_path = "reportes/plots/incidencia_7d.png"

    df = metrica_incidencia_7d.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    _save_lineplot(
        df=df,
        x="date",
        y="incidencia_7d",
        hue="location",
        title="Incidencia 7 días por 100k (Ecuador vs país comparativo)",
        out_path=out_path,
    )

    context.add_output_metadata({
        "png": MetadataValue.path(out_path),
        "preview": MetadataValue.md(f"![incidencia_7d]({out_path})")
    })
    return out_path


@asset
def grafica_factor_crec_7d(context, metrica_factor_crec_7d: pd.DataFrame) -> str:
    """
    Asset de Dagster que genera un gráfico de líneas del factor de crecimiento semanal (7 días).

    - Input: DataFrame `metrica_factor_crec_7d` 
      (espera columnas: `semana_fin`, `location`, `factor_crec_7d`).
    - Proceso:
        1. Convierte `semana_fin` (o `date` como fallback) a datetime.
        2. Usa `_save_lineplot` para graficar `factor_crec_7d` contra la fecha.
        3. Dibuja una línea por país.
    - Output: Ruta del archivo PNG generado en `reportes/plots/factor_crec_7d.png`.

    Al igual que el anterior, se añade metadata para que Dagster muestre
    el preview de la imagen directamente en su interfaz web.
    """
    _ensure_dir("reportes/plots")
    out_path = "reportes/plots/factor_crec_7d.png"

    df = metrica_factor_crec_7d.copy()
    if "semana_fin" in df.columns:
        df["semana_fin"] = pd.to_datetime(df["semana_fin"], errors="coerce")
        x_col = "semana_fin"
    else:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        x_col = "date"

    _save_lineplot(
        df=df,
        x=x_col,
        y="factor_crec_7d",
        hue="location",
        title="Factor de crecimiento 7 días (Ecuador vs país comparativo)",
        out_path=out_path,
    )

    context.add_output_metadata({
        "png": MetadataValue.path(out_path),
        "preview": MetadataValue.md(f"![factor_crec_7d]({out_path})")
    })
    return out_path
