# Evidencia — Confirmación de consumo SOLO-ORO

- **Qué demuestra:** que **ningún** visual del dashboard consulta `bronze` o `silver`;
  los 4 datasets leen exclusivamente tablas de `oceanwatch.gold`.
- **Cómo se generó:** inspección del archivo versionado
  `resources/dashboards/oceanwatch_oro.lvdash.json` (búsqueda de referencias de tablas).
- **Fecha:** 2026-11-06.
- **IDs reales:** dashboard `01f1da0367841f3db6c6ddcb8edaa849`.
- **Estado:** **Verificado (ejecutado)**.

## Tablas referenciadas por dataset

| Dataset | Visual | Tabla (FROM) | Capa |
|---|---|---|---|
| `ds_matriz_diaria` | V1 mapa intensidad | `oceanwatch.gold.matriz_trafico_diaria` | **oro** |
| `ds_ranking_puertos` | V2 ranking puertos | `oceanwatch.gold.presencia_puertos` | **oro** |
| `ds_buques_puerto` | V3 buques en puerto | `oceanwatch.gold.presencia_puertos` | **oro** |
| `ds_anomalias` | V4 anomalías del día | `oceanwatch.gold.anomalias_velocidad` | **oro** |

## Verificación

- Búsqueda de `oceanwatch.gold.` en el `.lvdash.json`: **4** coincidencias (una por `FROM`).
- Búsqueda de `bronze` o `silver` en el `.lvdash.json`: **0** coincidencias.

Conclusión: el tablero cumple la regla de rendimiento D4/ADR-017 — consume capa **oro**
pre-agregada y no toca las capas crudas. La única agregación costosa (colapsar horas para
el mapa) se materializó una vez en oro (`gold.matriz_trafico_diaria`, ver
`vista_apoyo_matriz_diaria.md`), no en tiempo de render.
