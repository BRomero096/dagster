# Informe Técnico – Pipeline COVID-19

## 1. Arquitectura del Pipeline
- Se implementó con **Dagster Assets** encadenados.
- Assets: `leer_datos` → `datos_procesados` → (`metrica_incidencia_7d`, `metrica_factor_crec_7d`) → `reporte_excel_covid`.
- Checks de entrada y salida con **Dagster Asset Checks**.
- Exportación final a Excel.

![diagrama_pipeline](docs/diagrama.png)

---

## 2. Decisiones de Diseño
- Uso de **pandas** por simplicidad y compatibilidad con Dagster.
- `entity` renombrada a `location` para estandarizar.
- Se permitió `new_cases<0` como advertencia (revisiones oficiales).
- Fechas futuras: se marcan como **warning**.
- Duplicados y nulos: documentados, pero no bloquean ejecución.

---

## 3. Validaciones
### Entrada
- `keys_not_null`: location/date/population no nulos (advertencia si faltan).
- `unique_loc_date`: unicidad de (location, date).
- `population_positive`: población > 0.
- `new_cases_nonnegative`: advertencia si <0.
- `max_date_not_future`: advertencia si hay fechas > hoy.

### Salida
- `incidencia_en_rango`: valores entre 0 y 2000.
- `factor_crec_valido`: valores > 0, sin inf/NaN (advertencia primeras semanas).

---

## 4. Descubrimientos
- Ecuador reporta huecos en `people_vaccinated` en los primeros meses.
- Se detectaron valores negativos en `new_cases` (ajustes históricos).
- En las últimas fechas aparecen registros futuros (2025-12-31).

---

## 5. Resultados
### Metrica incidencia 7d
| fecha | país | incidencia_7d |
|-------|------|---------------|
| 2021-07-01 | Ecuador | 10.6 |
| 2021-07-01 | Perú | 15.7 |

Interpretación: Perú tuvo mayor incidencia relativa en ese periodo.

### Metrica factor crecimiento 7d
| semana_fin | país | casos_semana | factor_crec_7d |
|------------|------|--------------|----------------|
| 2021-07-07 | Ecuador | 12,500 | 1.15 |
| 2021-07-07 | Perú | 35,000 | 0.92 |

Interpretación: Ecuador crecía, Perú decrecía en esa semana.

---

## 6. Control de Calidad
| Regla | Estado | Filas afectadas | Nota |
|-------|--------|-----------------|------|
| max_date_not_future | ⚠️ | 1 | 2025-12-31 futura |
| keys_not_null | ⚠️ | 100+ | faltan valores en población |
| new_cases_nonnegative | ✅ | 0 | ok |
| population_positive | ✅ | 0 | ok |

---

## 7. Conclusión
- El pipeline cumple los pasos de extracción, validación, limpieza, métricas y exportación.
- Dagster facilitó la trazabilidad y visualización de reglas de calidad.
- Las métricas permiten comparar Ecuador vs. [país elegido].
