# Pendientes del equipo (acciones manuales) — OceanWatch Analytics

Lista explícita de lo que el **equipo humano** debe hacer manualmente. **Nada de esto es software
pendiente:** todo el pipeline, el Job, el modelo, el dashboard y la gobernanza están
**ejecutados y verificados** (ver `traceability_matrix.md` y `autoevaluacion_rubrica.md`). Lo que
queda son **capturas de interfaz** que requieren un navegador autenticado y no se pueden
automatizar desde la API.

Cuenta de trabajo: `jd.torresa1@uniandes.edu.co`. Workspace:
`https://dbc-a568bc49-bdae.cloud.databricks.com`. SQL warehouse serverless: `377600528fed7c47`
(arranca en frío en ~10–20 s la primera vez).

---

## 1. Capturas del dashboard (4 visuales + parámetro de fecha)

- **Qué demuestra:** evidencia visual de que el dashboard AI/BI renderiza con datos reales de la
  capa oro. Las 4 consultas ya están **verificadas (ejecutadas)** por API/SQL.
- **Referencia detallada:** `evidence/req04-dashboard/capturas_PENDIENTE.md`.
- **Dashboard:** `01f1da0367841f3db6c6ddcb8edaa849` (ACTIVE/publicado).

**Procedimiento exacto:**

1. Iniciar sesión en el workspace con `jd.torresa1@uniandes.edu.co`.
2. Abrir la URL publicada:
   `https://dbc-a568bc49-bdae.cloud.databricks.com/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849/published`
3. Confirmar que el warehouse `377600528fed7c47` está activo y que el filtro de **fecha** arranca
   en `2023-06-03`.
4. Capturar (guardar como PNG en `evidence/req04-dashboard/`):
   - `01_dashboard_completo_20230603.png` — página completa con los 4 visuales y datos.
   - `02_v1_mapa_intensidad.png` — V1 mapa de intensidad por celda.
   - `03_v2_ranking_puertos.png` — V2 barras (NEW YORK CITY arriba ese día).
   - `04_v3_buques_en_puerto.png` — V3 tabla de buques en puerto.
   - `05_v4_anomalias_tabla_y_mapa.png` — V4 tabla + mapa de anomalías.
   - `06_filtro_fecha_20230602.png` — filtro cambiado a `2023-06-02`: V4 queda vacío (esperado).
   - `07_url_barra_navegador.png` — barra del navegador con el `dashboard_id`.
5. Actualizar `capturas_PENDIENTE.md` a "Verificado (capturado)".

---

## 2. Captura del linaje en Catalog Explorer

- **Qué demuestra:** el grafo visual de linaje en la UI. El linaje ya está **verificado por API**
  (tabla y columna); la rúbrica valora además la captura de la interfaz.
- **Referencia detallada:** `evidence/req05-gobernanza/linaje.md` §4.

**Procedimiento exacto:**

1. En el workspace, abrir **Catalog** (Catalog Explorer) en la barra lateral.
2. Navegar: catálogo `oceanwatch` → esquema `silver` → tabla `ais_clean` (o
   `gold.trayectorias_diarias`).
3. Abrir la pestaña **Lineage / Linaje**.
4. Expandir upstream (`bronze.ais_raw`, `silver.celda_en_agua`) y downstream (las 4 tablas de oro)
   para que coincida con el JSON de `linaje.md` §1.
5. Guardar como `evidence/req05-gobernanza/lineage_catalog_explorer.png` y actualizar `linaje.md`
   §4 a "Verificado (capturado)".

---

## Nada más queda pendiente de software

El resto del proyecto está completo: ingesta, capas/calidad, orquestación, dashboard (definición
+ despliegue + publicación), gobernanza (grants + linaje por API + comentarios) y ML (modelo en
UC + tarea en el Job). `python -m pytest -q` → **109 passed, 1 skipped**.

> **Nota de fechas:** algunos timestamps de linaje en `evidence/req05-gobernanza/linaje.md`
> aparecen como `2026-11-06` por el huso UTC del servidor; el trabajo se realizó el
> **2026-11-05** (hora local). Es la misma sesión; la diferencia es solo de zona horaria.
