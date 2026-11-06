# Evidencia — Disponibilidad de librerías en Databricks serverless (insumo de ADR-010, Req. 2)

- **Qué demuestra:** qué librerías geoespaciales necesarias para la máscara tierra/agua y
  para leer el shapefile WPI (`world_port_index`) están **realmente** preinstaladas en el
  entorno de cómputo *serverless* del workspace. Es el insumo crítico para decidir el método
  de clasificación tierra/agua de la capa plata (ADR-010: `global-land-mask` vs. geometría WPI).
- **Cómo se generó:** se subió un notebook de prueba al workspace del usuario y se ejecutó un
  *run* de trabajo en **serverless** (sin clúster, `jobs/runs/submit` con `notebook_task`), que
  intenta `import` de cada módulo y reporta su versión vía `dbutils.notebook.exit(...)`. No es una
  inspección de documentación: es una ejecución real contra el entorno serverless del proyecto.
- **Fecha:** 2026-11-05
- **Entorno:** cómputo serverless del workspace OceanWatch; `run_id` de prueba terminado con
  `life_cycle_state = TERMINATED`, `result_state = SUCCESS`.
- **Estado:** Verificado (ejecutado)

## Resultados reales (salida literal del notebook serverless)

```json
{
  "python": "3.11.10",
  "libs": {
    "global-land-mask": {"disponible": false, "error": "ModuleNotFoundError: No module named 'global_land_mask'"},
    "geopandas":        {"disponible": false, "error": "ModuleNotFoundError: No module named 'geopandas'"},
    "pyshp":            {"disponible": false, "error": "ModuleNotFoundError: No module named 'shapefile'"}
  }
}
```

| Librería | Módulo importado | ¿Preinstalada en serverless? | Versión |
|---|---|---|---|
| `global-land-mask` | `global_land_mask` | **No** | n/a (`ModuleNotFoundError`) |
| `geopandas` | `geopandas` | **No** | n/a (`ModuleNotFoundError`) |
| `pyshp` | `shapefile` | **No** | n/a (`ModuleNotFoundError`) |

- **Versión de Python del entorno serverless:** 3.11.10.

## Implicación para la decisión de método tierra/agua (ADR-010)

**Ninguna** de las tres librerías viene preinstalada en el entorno serverless. Por tanto, cualquier
método que dependa de ellas exige instalación explícita en tiempo de ejecución (p. ej.
`%pip install` a nivel de notebook o dependencias de entorno serverless declaradas en el *job*).
Esto condiciona el ADR-010:

1. **`global-land-mask`:** requeriría instalarlo (`pip`) en cada ejecución serverless. Aporta una
   máscara tierra/agua global rasterizada, sin dependencias geoespaciales pesadas, pero añade una
   dependencia externa que debe quedar pinneada por reproducibilidad.
2. **Geometría WPI (`geopandas`/`pyshp`):** usar el `world_port_index` como fuente de proximidad a
   puerto también requeriría instalar `geopandas` (dependencias nativas GEOS/GDAL) o `pyshp`
   (lector puro de shapefile, más ligero). `geopandas` es la más costosa de instalar en serverless.
3. **Alternativa sin dependencias externas:** evaluar en ADR-010 un método que no requiera instalar
   nada (p. ej. lógica geométrica propia o un recurso estático precargado), dado que el entorno
   serverless parte sin estas librerías.

> Decisión concreta del método se tomará en ADR-010 (paso posterior). Este documento solo registra
> el hecho verificado: el entorno serverless no trae las tres librerías de fábrica.

## Reproducibilidad

- Prueba ejecutada mediante un `notebook_task` en `jobs/runs/submit` **sin** especificación de
  clúster (lo que fuerza cómputo serverless), contra las credenciales de `databricks.key`
  (fuera del repo). El notebook de prueba se eliminó del workspace tras la ejecución.
