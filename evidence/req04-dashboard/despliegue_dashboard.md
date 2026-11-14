# Evidencia — Despliegue REAL del dashboard AI/BI (create + publish)

- **Qué demuestra:** que el dashboard existe en el workspace, tiene warehouse asignado,
  está **publicado** y accesible (no un archivo local). Incluye ID y URL **reales**.
- **Cómo se generó:** llamadas REST a la Lakeview API
  (`POST /api/2.0/lakeview/dashboards` + `POST .../published` + `GET`), leyendo el
  `.lvdash.json` versionado `resources/dashboards/oceanwatch_oro.lvdash.json` como
  `serialized_dashboard`. Script temporal `_fase7_deploy.py` (fuera del repo, eliminado al
  terminar), credenciales desde `databricks.key` (fuera del repo).
- **Fecha:** 2026-11-06.
- **Estado:** **Verificado (ejecutado)**.

## IDs y URL reales

| Campo | Valor real |
|---|---|
| `dashboard_id` | **`01f1da0367841f3db6c6ddcb8edaa849`** |
| `display_name` | `OceanWatch - Consola de trafico (oro)` |
| `path` | `/Users/jd.torresa1@uniandes.edu.co/OceanWatch - Consola de trafico (oro).lvdash.json` |
| `warehouse_id` | `377600528fed7c47` (serverless) |
| `lifecycle_state` | `ACTIVE` |
| `etag` (al crear) | `2878504591` |
| `embed_credentials` | `true` |
| `create_time` | `2026-11-06T16:59:18.236Z` |
| `revision_create_time` (publish) | `2026-11-06T16:59:24.771Z` |
| **URL publicada** | `https://dbc-a568bc49-bdae.cloud.databricks.com/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849/published` |
| **URL borrador (editor)** | `https://dbc-a568bc49-bdae.cloud.databricks.com/sql/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849` |

## 1. Create — `POST /api/2.0/lakeview/dashboards`

Cuerpo: `display_name`, `warehouse_id=377600528fed7c47`,
`parent_path=/Users/jd.torresa1@uniandes.edu.co`, `serialized_dashboard=<.lvdash.json>`.

Respuesta real (HTTP **200**):

```json
{
  "dashboard_id": "01f1da0367841f3db6c6ddcb8edaa849",
  "display_name": "OceanWatch - Consola de trafico (oro)",
  "path": "/Users/jd.torresa1@uniandes.edu.co/OceanWatch - Consola de trafico (oro).lvdash.json",
  "create_time": "2026-11-06T16:59:18.236Z",
  "update_time": "2026-11-06T16:59:18.354Z",
  "warehouse_id": "377600528fed7c47",
  "etag": "2878504591",
  "lifecycle_state": "ACTIVE",
  "parent_path": "/Users/jd.torresa1@uniandes.edu.co"
}
```

## 2. Publish — `POST /api/2.0/lakeview/dashboards/<id>/published`

Cuerpo: `embed_credentials=true`, `warehouse_id=377600528fed7c47`.

Respuesta real (HTTP **200**):

```json
{
  "display_name": "OceanWatch - Consola de trafico (oro)",
  "warehouse_id": "377600528fed7c47",
  "embed_credentials": true,
  "revision_create_time": "2026-11-06T16:59:24.771Z"
}
```

## 3. Verificación — `GET` borrador y publicado

`GET /api/2.0/lakeview/dashboards/01f1da0367841f3db6c6ddcb8edaa849` (HTTP 200):

```json
{
  "dashboard_id": "01f1da0367841f3db6c6ddcb8edaa849",
  "display_name": "OceanWatch - Consola de trafico (oro)",
  "path": "/Users/jd.torresa1@uniandes.edu.co/OceanWatch - Consola de trafico (oro).lvdash.json",
  "warehouse_id": "377600528fed7c47",
  "lifecycle_state": "ACTIVE"
}
```

`GET /api/2.0/lakeview/dashboards/01f1da0367841f3db6c6ddcb8edaa849/published` (HTTP 200):

```json
{
  "display_name": "OceanWatch - Consola de trafico (oro)",
  "warehouse_id": "377600528fed7c47",
  "embed_credentials": true,
  "revision_create_time": "2026-11-06T16:59:24.771Z"
}
```

## Dashboard único

`GET /api/2.0/lakeview/dashboards?page_size=100` lista **un solo** dashboard de este
proyecto: `01f1da0367841f3db6c6ddcb8edaa849 | OceanWatch - Consola de trafico (oro) |
ACTIVE`. (También aparece un `New Dashboard 2026-10-31 11:21:08` preexistente, ajeno a esta
fase y no creado por este trabajo; no se toca.) No se crearon dashboards de prueba.

## Definición como código

La fuente de verdad de la definición es
`resources/dashboards/oceanwatch_oro.lvdash.json` (4 datasets + 1 página con filtro de
fecha y 7 widgets). SIN secretos ni `dashboard_id` embebido. El despliegue lo hace un script
temporal que lee ese archivo.
