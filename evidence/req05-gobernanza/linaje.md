# Evidencia — Linaje de datos verificado por API (tabla y columna)

- **Qué demuestra:** que Unity Catalog mantiene el **linaje real** del pipeline OceanWatch
  (bronce → plata → oro) a nivel de **tabla** y de **columna**, consultable por API y
  utilizable para análisis de impacto y trazabilidad.
- **Cómo se generó:** llamadas **GET** a la API de linaje de Databricks
  (`/api/2.0/lineage-tracking/table-lineage` y `/api/2.0/lineage-tracking/column-lineage`)
  con el token del workspace. Se usa **GET con query params**; `POST` responde
  `404 ENDPOINT_NOT_FOUND` en este workspace.
- **Fecha:** 2026-11-05
- **Origen:** API REST `lineage-tracking` de Databricks (workspace serverless).
- **Estado:** Verificado por API (ejecutado). El screenshot de Catalog Explorer queda
  **PENDIENTE DE CAPTURA** (requiere navegador autenticado; no es automatizable).

## 1. Linaje de TABLA (JSON real)

### `GET /api/2.0/lineage-tracking/table-lineage?table_name=oceanwatch.silver.ais_clean`

Nodo central de la capa plata: aguas arriba el crudo de bronce y la tabla de apoyo
`silver.celda_en_agua`; aguas abajo las 4 tablas de oro.

```json
{
  "upstreams": [
    { "tableInfo": { "name": "celda_en_agua", "catalog_name": "oceanwatch", "schema_name": "silver", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:38:31.0" } },
    { "tableInfo": { "name": "ais_raw",       "catalog_name": "oceanwatch", "schema_name": "bronze", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:37:46.0" } }
  ],
  "downstreams": [
    { "tableInfo": { "name": "anomalias_velocidad",  "catalog_name": "oceanwatch", "schema_name": "gold", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:40:43.0" } },
    { "tableInfo": { "name": "matriz_trafico",       "catalog_name": "oceanwatch", "schema_name": "gold", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:39:17.0" } },
    { "tableInfo": { "name": "presencia_puertos",    "catalog_name": "oceanwatch", "schema_name": "gold", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:39:12.0" } },
    { "tableInfo": { "name": "trayectorias_diarias", "catalog_name": "oceanwatch", "schema_name": "gold", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:39:08.0" } }
  ]
}
```

### `GET .../table-lineage?table_name=oceanwatch.bronze.ais_raw`

Confirma el origen de la ingesta (volumen `bronze.landing`) y los descendientes de la
capa plata/calidad/cuarentena.

```json
{
  "upstreams": [
    { "fileInfo": { "path": "/Volumes/oceanwatch/bronze/landing/csv/", "securable_name": "oceanwatch.bronze.landing", "securable_type": "VOLUME", "has_permission": true, "lineage_timestamp": "2026-11-06 03:46:03.0" } }
  ],
  "downstreams": [
    { "tableInfo": { "name": "metricas_calidad", "catalog_name": "oceanwatch", "schema_name": "quality",    "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:38:17.0" } },
    { "tableInfo": { "name": "ais_rejected",     "catalog_name": "oceanwatch", "schema_name": "quarantine", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:38:06.0" } },
    { "tableInfo": { "name": "ais_clean",        "catalog_name": "oceanwatch", "schema_name": "silver",     "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:37:46.0" } }
  ]
}
```

### Descendientes de oro (upstream = `silver.ais_clean`)

Las 4 tablas de oro confirman `silver.ais_clean` como fuente (JSON real):

- `gold.trayectorias_diarias` → upstream `silver.ais_clean` (`2026-11-06 16:39:08.0`).
- `gold.matriz_trafico` → upstream `silver.ais_clean`; downstream `gold.matriz_trafico_diaria`.
- `gold.presencia_puertos` → upstreams `silver.ais_clean` **y** `gold.world_port_index`.
- `gold.anomalias_velocidad` → upstream `silver.ais_clean` (`2026-11-06 16:40:43.0`).

## 2. Linaje de COLUMNA (JSON real)

### `GET /api/2.0/lineage-tracking/column-lineage?table_name=oceanwatch.gold.trayectorias_diarias&column_name=MMSI`

El identificador de buque `MMSI` de la tabla de oro proviene directamente de la columna
`MMSI` de `silver.ais_clean`:

```json
{
  "upstream_cols": [
    { "name": "MMSI", "catalog_name": "oceanwatch", "schema_name": "silver", "table_name": "ais_clean", "table_type": "TABLE", "lineage_timestamp": "2026-11-06 16:39:08.0" }
  ]
}
```

## 3. Uso para análisis de impacto

El linaje responde dos preguntas de gobernanza sin inspeccionar el código:

- **Impacto aguas abajo:** si cambia `silver.ais_clean`, los descendientes directos
  afectados son las 4 tablas de oro (`trayectorias_diarias`, `matriz_trafico`,
  `presencia_puertos`, `anomalias_velocidad`) y, transitivamente, `gold.matriz_trafico_diaria`
  y el dashboard que la consume.
- **Trazabilidad aguas arriba:** una métrica de oro se rastrea hasta su origen
  (`silver.ais_clean` ← `bronze.ais_raw` ← volumen `bronze.landing`), incluso a nivel de
  columna (`gold.trayectorias_diarias.MMSI` ← `silver.ais_clean.MMSI`).

## 4. Procedimiento del screenshot de Catalog Explorer (PENDIENTE DE CAPTURA)

El grafo visual complementa la evidencia por API, pero **no es automatizable** (requiere
navegador autenticado). Procedimiento exacto para capturarlo:

1. Abrir el workspace Databricks e ir a **Catalog** (Catalog Explorer) en la barra lateral.
2. Navegar: catálogo **`oceanwatch`** → esquema **`silver`** → tabla **`ais_clean`**.
3. Abrir la pestaña **Lineage / Linaje** de la tabla.
4. Expandir el grafo **upstream** (bronce `ais_raw`, `silver.celda_en_agua`) y
   **downstream** (las 4 tablas de oro) para que coincida con el JSON de la sección 1.
5. Capturar la pantalla del grafo y guardarla como
   `evidence/req05-gobernanza/lineage_catalog_explorer.png`, actualizando esta sección a
   "Verificado (capturado)".

> **Distinción de estado:** las secciones 1 y 2 están **Verificadas por API (ejecutado)**;
> el screenshot de esta sección está **PENDIENTE DE CAPTURA**.
