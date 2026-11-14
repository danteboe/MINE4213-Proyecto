# ADR-017: Consola de visualización con Databricks AI/BI Dashboards sobre la capa oro

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 4 (15%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
El Req. 4 pide una **consola de visualización** sobre el Lakehouse que consuma el modelo
dimensional de negocio (la capa **oro** `oceanwatch.gold`), con al menos 4 visuales y un
parámetro de fecha, desplegada **de verdad** en el workspace. Restricciones del entorno:
cómputo **solo serverless** (warehouse `377600528fed7c47`), Unity Catalog, resistencia a
datos nuevos (podrían entrar más días de AIS), reproducibilidad y **definición como código**
en el repo. El corpus actual son 3 días (2023-06-01/02/03; las anomalías solo existen el
06-03, test set de ADR-016). Hay que decidir: (a) herramienta de BI, (b) vía de despliegue,
(c) cómo alimentar eficientemente el mapa de intensidad sin recomputar sobre tablas grandes,
(d) cómo implementar el parámetro de fecha.

## 2. Criterios de decisión
- **Integración nativa con el Lakehouse / Unity Catalog** (sin exportar datos a otra herramienta).
- **Definición como código** versionable en el repo y despliegue reproducible (CI/CD-friendly).
- **Coste/viabilidad serverless** (el consumo analítico debe leer oro pre-agregada, no recomputar).
- **Resistencia a datos nuevos** (el parámetro de fecha debe funcionar si entran más días).
- **ID/URL reales verificables** (nada inventado; dashboard único y publicado).

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| **A. Databricks AI/BI Dashboards (Lakeview) + REST API (ELEGIDA)** | Dashboard nativo `.lvdash.json` versionado en el repo, desplegado con `POST /api/2.0/lakeview/dashboards` + `.../published` sobre el warehouse serverless | Nativo de UC (sin exportar datos); definición como código; despliegue reproducible por API; coherente con la automatización CLI/REST ya usada en fases 2–6; ID/URL reales | El `.lvdash.json` es un esquema propietario que hay que escribir a mano | Dashboard real `01f1da0367841f3db6c6ddcb8edaa849` publicado (`evidence/req04-dashboard/despliegue_dashboard.md`); [API CRUD de dashboards AI/BI](https://learn.microsoft.com/en-us/azure/databricks/dashboards/tutorials/dashboard-crud-api) (acceso 2026-11-05) |
| B. Power BI sobre el warehouse | Conector Power BI → SQL warehouse | Herramienta ampliamente conocida | Externa al Lakehouse; requiere licencia/servicio aparte; no es "definición como código" en el repo; rompe la coherencia solo-Databricks del proyecto | El requisito y el enunciado del equipo piden **dashboard de Databricks**, no Power BI |
| C. AI/BI vía recurso `dashboards` de Databricks Asset Bundles (DAB) | Declarar el dashboard como recurso del bundle | Integrado al `databricks.yml` existente | El contenido del `.lvdash.json` no se interpola y el flujo DAB para dashboards tiene fricción documentada para CI/CD; añade piezas sin capturar mejor el ID/URL | Hilo de la comunidad [Deploy dashboard with asset bundle](https://community.databricks.com/t5/data-engineering/deploy-dashboard-with-asset-bundle/td-p/146681) (acceso 2026-11-05) |

## 4. Decisión
Se implementa la consola como un **Databricks AI/BI Dashboard (Lakeview)** cuya definición
`.lvdash.json` vive versionada en `resources/dashboards/oceanwatch_oro.lvdash.json` y se
**despliega vía REST API** (`create` + `published`) sobre el warehouse serverless
`377600528fed7c47`. El tablero consume **exclusivamente** la capa **oro**; para el mapa de
intensidad se materializa una **vista de apoyo pre-agregada en oro**
`gold.matriz_trafico_diaria`; y el filtrado temporal se hace con **un único parámetro de
dashboard `fecha` de tipo `DATE`** cableado a los 4 datasets.

## 5. Justificación basada en evidencia
- **Herramienta AI/BI (vs Power BI).** El requisito pide un dashboard de **Databricks** sobre
  la capa oro; AI/BI es nativo de Unity Catalog, no exporta datos y permite definición como
  código. Verificado desplegado y publicado: dashboard `01f1da0367841f3db6c6ddcb8edaa849`,
  `lifecycle_state=ACTIVE`, `warehouse_id=377600528fed7c47`
  (`evidence/req04-dashboard/despliegue_dashboard.md`).
- **REST (vs DAB, D1).** El proyecto ya conduce Databricks con CLI/REST desde scripts fuera del
  repo (patrón de fases 2–6); REST da la vía más directa para capturar `dashboard_id`/URL
  reales y dejar **un único** dashboard, evitando la fricción documentada del recurso
  `dashboards` de DAB ([community](https://community.databricks.com/t5/data-engineering/deploy-dashboard-with-asset-bundle/td-p/146681)).
  El `.lvdash.json` versionado es la fuente de verdad; el script temporal solo lo lee y lo publica.
- **Vista de apoyo en oro (D2).** `gold.matriz_trafico` tiene granularidad celda×**hora**
  (2.319.365 filas; 324k–371k celdas/día). El mapa necesita 1 fila por celda-día; recomputar
  ese `GROUP BY` en cada render es caro. Se materializó **una vez** en oro
  `gold.matriz_trafico_diaria` (**1.027.635** filas, verificado) — el dashboard lee ya
  agregado (`evidence/req04-dashboard/vista_apoyo_matriz_diaria.md`).
- **Rendimiento solo-oro (D4).** Los 4 datasets leen solo `oceanwatch.gold.*`; 0 referencias a
  bronze/silver (`silver.ais_clean` tiene 25,8 M filas y **no** se toca en consumo). Verificado
  por inspección del `.lvdash.json` (`evidence/req04-dashboard/confirmacion_solo_oro.md`). El
  filtro `:fecha` entra en el `WHERE` antes de agregar.
- **Parámetro de fecha (D3).** Un parámetro `fecha` tipo `DATE` (default `2023-06-03`) en los 4
  datasets, enlazado por un widget filtro; sintaxis de *named parameter marker* `:fecha`
  ([parámetros de dashboard](https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters),
  acceso 2026-11-05). Verificado que cambia los 4 visuales y que V4 solo tiene datos el 06-03
  (`evidence/req04-dashboard/validacion_parametro_fecha.md`). Resistente a datos nuevos: si
  entran más días de AIS, el parámetro los selecciona sin cambiar el diseño.
- **Definición de "anomalía del día".** `es_anomalia = true` del `_batch_date = :fecha`, salida
  del modelo campeón IsolationForest (`contamination=0.01`, `random_state=42`) sobre
  `[z_sog, delta_sog, hora_sin, hora_cos]`, con línea base interpretable `|z_sog| > 3` por
  `VesselType` (**umbral = 3.0**, `conf/config.yaml` `ml.umbral_zscore_base`). Definido en
  [ADR-016](ADR-016-modelo-anomalias-sog-mlflow.md) §4–§5.

## 6. Patrón de diseño / ingeniería de datos aplicado
- **Serving layer / BI semantic layer sobre Gold** del Medallion: el consumo analítico lee la
  capa dimensional de negocio, nunca las crudas.
- **Dashboard-as-code**: la definición declarativa (`.lvdash.json`) versionada es la fuente de
  verdad; el despliegue es reproducible por API.
- **Materialized pre-aggregation (summary table)**: `gold.matriz_trafico_diaria` precalcula la
  agregación costosa una sola vez para el render del mapa.
- **Parametrización por *named markers***: un solo parámetro `:fecha` compartido por los datasets.

## 7. Consecuencias
- **Positivas:** consola real, publicada y verificable (4 visuales, parámetro de fecha);
  consumo 100 % oro pre-agregada; definición versionada y re-desplegable; coherente con la
  automatización del repo; resistente a nuevos días de datos.
- **Negativas / deuda:** el `.lvdash.json` es un esquema propietario mantenido a mano (cambios
  de encoding exigen cuidado); la métrica secundaria `buques` del mapa sobrecuenta buques
  presentes en varias horas (documentado, es métrica de actividad, no de unicidad); las
  **capturas de pantalla** quedan PENDIENTES del equipo (no automatizables sin navegador
  autenticado, `evidence/req04-dashboard/capturas_PENDIENTE.md`).
- **Refresco (decisión).** Los dashboards AI/BI se refrescan **solos** al abrirse/visualizarse
  (consultan el warehouse en vivo) y admiten *schedule* propio. **No** se añade un nodo
  `refresh_dashboard` al Job `oceanwatch_pipeline_entrega2`: el DAG lo listaba como nodo futuro,
  pero como el tablero lee las tablas de oro en vivo cada vez que se abre, un refresco acoplado
  al Job no aporta y añadiría una dependencia frágil. Si en el futuro se quisiera *warm cache*
  tras cada corrida del pipeline, se puede programar el refresco propio del dashboard; queda
  fuera del alcance de esta entrega.

## 8. Cómo se verifica / reversibilidad
- **Existe y está publicado:** `GET /api/2.0/lakeview/dashboards/01f1da0367841f3db6c6ddcb8edaa849`
  → `lifecycle_state=ACTIVE`, `warehouse_id=377600528fed7c47`; `GET .../published` → 200 con
  `revision_create_time` (`evidence/req04-dashboard/despliegue_dashboard.md`).
- **URL publicada:** `https://dbc-a568bc49-bdae.cloud.databricks.com/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849/published`.
- **Los 4 visuales retornan filas reales:** `evidence/req04-dashboard/consultas_visuales.md`
  (V1 top 39.470; V2 NEW YORK CITY 92; V3 MMSI con 1.347 posiciones; V4 67.828 anomalías 06-03).
- **Parámetro de fecha:** `evidence/req04-dashboard/validacion_parametro_fecha.md`.
- **Reversibilidad:** `DELETE /api/2.0/lakeview/dashboards/<id>/published` (despublica) y
  `DELETE /api/2.0/lakeview/dashboards/<id>` (a la papelera, recuperable); `DROP TABLE
  oceanwatch.gold.matriz_trafico_diaria` elimina la vista de apoyo. El resto del oro no depende
  del dashboard.

## 9. Referencias
- API CRUD de dashboards AI/BI (create/publish/get): https://learn.microsoft.com/en-us/azure/databricks/dashboards/tutorials/dashboard-crud-api (acceso 2026-11-05).
- Parámetros de dashboard (named markers `:keyword`, tipo Date): https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters (acceso 2026-11-05).
- Tipos de visualización AI/BI: https://docs.databricks.com/aws/en/dashboards/manage/visualizations/types (acceso 2026-11-05).
- Mapas (point/symbol map; requieren lat/lon): https://learn.microsoft.com/en-gb/azure/databricks/dashboards/manage/visualizations/maps (acceso 2026-11-05).
- DAB dashboards (alternativa C descartada): https://community.databricks.com/t5/data-engineering/deploy-dashboard-with-asset-bundle/td-p/146681 (acceso 2026-11-05).
- [ADR-016](ADR-016-modelo-anomalias-sog-mlflow.md) (definición de "anomalía del día"), [ADR-001](ADR-001-arquitectura-medallion-serverless.md) (grilla lat/lon 2 decimales, medallion), [ADR-012](ADR-012-modelo-oro.md) (modelo dimensional de oro).
- Evidencia: `evidence/req04-dashboard/` (`consultas_visuales.md`, `vista_apoyo_matriz_diaria.md`, `despliegue_dashboard.md`, `validacion_parametro_fecha.md`, `confirmacion_solo_oro.md`, `capturas_PENDIENTE.md`).

> *Content was rephrased for compliance with licensing restrictions.*
