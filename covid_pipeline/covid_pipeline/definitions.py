from dagster import Definitions, load_assets_from_modules
from .assets import datos, metricas, reportes, checks, eda, graficas

# -------------------------------------------------------------------
# Registro central de assets y checks del proyecto Dagster
# -------------------------------------------------------------------

# Carga automáticamente todos los assets definidos en los módulos listados.
# Cada módulo (datos.py, metricas.py, reportes.py, eda.py, graficas.py) contiene
# funciones decoradas con @asset que representan transformaciones del pipeline.
all_assets = load_assets_from_modules([datos, metricas, reportes, eda, graficas])

# Recupera la lista de asset checks definidos en checks.py
# (funciones decoradas con @asset_check para validar calidad de datos).
all_checks = checks.get_all_checks()

# Construye el objeto raíz Definitions, que Dagster utiliza para orquestar el pipeline.
# Aquí se declaran:
#   - assets: todos los datasets y transformaciones
#   - asset_checks: validaciones de calidad asociadas a cada asset
defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
)
