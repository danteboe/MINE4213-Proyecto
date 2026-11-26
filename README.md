# OceanWatch Analytics · MINE 4213

OceanWatch procesa mensajes AIS de NOAA para consultar el tráfico, la presencia de buques en puertos y las anomalías de velocidad. La Entrega 2 corre en el workspace institucional de Databricks con un **clúster clásico de Unity Catalog**. Reutiliza el esquema AIS, la grilla espacial y el diagnóstico de calidad de la Entrega 1.

## Entrega 2

**Equipo 9** · Alejandro Abril, David Fernando Caro y Juan David Torres Albarracín.

| Para | Abrir |
|---|---|
| Encontrar un Job, volumen, notebook, tabla, clúster, tablero o experimento | [Directorio de recursos](docs/directorio_recursos.md) |
| Revisar los ocho criterios del enunciado | [Matriz de trazabilidad](docs/traceability_matrix.md) y [estado de entrega](docs/autoevaluacion_rubrica.md) |
| Entender y justificar el diseño | [Arquitectura Spark](docs/architecture/medallion-overview.md), [DAG](docs/architecture/fase4-dag-orquestacion.md) y [decisiones](docs/adr/) |
| Cargar archivos, reproducir una corrida o diagnosticar fallos | [Runbook](docs/runbook.md) |
| Verificar resultados y preparar la presentación | [Evidencia institucional](evidence/institucional/) y [guía de sustentación](docs/guia_sustentacion.md) |

[Detector de archivos](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466) → [Job Entrega 2](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) → [Tablero AI/BI](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published).

El DAG ejecuta **preparación → ingesta → plata → conciliación → oro → ML**. Auto Loader usa un checkpoint persistente y un sink Delta append-only. Plata aplica reglas, conserva los rechazos con causas y deduplica por buque y timestamp. Los pings del día que pasa conciliación alimentan los agregados y la inferencia. El IsolationForest queda versionado en Unity Catalog mediante MLflow. El SQL warehouse institucional sirve las consultas del tablero.

El backfill institucional inicial cubrió el **1–8 de junio de 2023** y concilió **69.454.003 posiciones** en bronce. La prueba posterior con el CSV2 original del **29 de junio de 2026** activó el pipeline desde el detector y concilió **10.139.168 posiciones** con diferencia cero. La inferencia publicó **95.461 anomalías** con el modelo campeón versión 1. [Evidencia de carga y resultados](evidence/institucional/llegada_archivos.md).

| Recurso | Ubicación |
|---|---|
| Tablas y modelo institucionales | `clase14.equipo9.oceanwatch_<capa>_<objeto>` |
| Landing y checkpoint | `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9` |
| Bandeja de archivos nuevos | `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9/landing/inbox/` |
| Configuración | [conf/config.yaml](conf/config.yaml), sección `institucional` |
| Despliegue | [databricks.yml](databricks.yml) y [directorio de implementaciones](docs/directorio_recursos.md#notebooks-y-código-desplegado) |

La cobertura de gobernanza es parcial. Unity Catalog conserva linaje, comentarios y permisos del grupo `mine-group9`. La segregación efectiva de capas y roles requiere esquemas y grants de la administración del curso. [Contrato de oro](docs/data_contract_gold.md) · [Gobernanza verificada](evidence/institucional/gobernanza.md).

### Incorporar un archivo nuevo

En **Catalog → clase10 → equipo9 → checkpoints**, abra `oceanwatch_equipo9/landing/inbox` y cargue el archivo de NOAA tal como lo descargó. Se admiten `ais-AAAA-MM-DD.csv.zst`, `AIS_AAAA_MM_DD.zip` y `AIS_AAAA_MM_DD.csv`. Para un lote adicional en el formato ZIP o CSV original use un sufijo, por ejemplo `AIS_2023_06_10_parte2.csv`. Cada nombre identifica una entrega inmutable.

El detector consulta la bandeja **a la hora y a la media hora**. Si encuentra archivos pendientes, lanza el DAG completo. Databricks arranca el clúster clásico si está apagado. El detector usa el warehouse SQL para nombres y confirmaciones. El trigger nativo **File arrival** queda pausado hasta que el administrador habilite los eventos de archivos en AWS. En Windows, el script también permite cargar el archivo.

```powershell
.\tools\subir_ais.ps1 -Archivo 'C:\Descargas\ais-2026-06-29.csv.zst' -Perfil oceanwatch
```

El DAG toma las fechas de los nombres, convierte los CSV2 comprimidos al esquema AIS y usa Auto Loader para incorporar los CSV a bronce. Reconstruye plata, oro y las anomalías de los días afectados con el campeón vigente. El tablero abre en la última fecha disponible en oro. [Operación y activación del trigger](docs/runbook.md).

### Reproducir

Estas comprobaciones usan el [perfil OAuth institucional](docs/runbook.md). El Job de verificación crea un sandbox y mide poda leyendo la plata disponible.

```sh
databricks bundle validate --strict -p oceanwatch
databricks bundle run -p oceanwatch oceanwatch_verificacion --params fecha_proceso=2023-06-01,medir_poda=true
```

El [runbook](docs/runbook.md#despliegue) detalla el despliegue inicial, el entrenamiento y el backfill. El repositorio excluye tokens, perfiles de autenticación y archivos `*.key`.

### Fuentes

- AIS diarios, Marine Cadastre / NOAA. [ZIP históricos de 2023](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/) · [CSV2 Zstandard de 2026](https://www.fisheries.noaa.gov/inport/item/80362).
- [World Port Index de NGA](https://msi.nga.mil/Publications/WPI). El CSV versionado extrae 7 campos y 3.669 registros del shapefile de la Entrega 1. `INDEX_NO` se normaliza a entero. El ZIP original queda fuera del repositorio.
- Global Land Mask 1.0.0, máscara ráster derivada de GLOBE. [Código y datos](https://github.com/toddkarin/global-land-mask).
- Canales y ríos navegables de NWN, instantánea del 18 de enero de 2023, y lagos de Natural Earth 5.0.0. El [ADR-020](docs/adr/ADR-020-procesamiento-incremental-y-calidad.md) documenta la proyección hidrográfica reproducible. [NTAD de BTS](https://rosap.ntl.bts.gov/view/dot/56264) y [Natural Earth](https://www.naturalearthdata.com/downloads/10m-physical-vectors/).

Los ADR-001 a 018, notebooks de fase y carpetas `req01-*` a `req06-*` conservan el prototipo en Free Edition. El directorio y la evidencia institucional describen el despliegue de la Parte 2.

---

## Entrega 1 (preservada)

Explorar, responder y almacenar de forma óptima el tráfico marítimo AIS de los días 1 al 7 de
junio de 2023 (fuente oficial: [Marine Cadastre / NOAA](https://hub.marinecadastre.gov/pages/vesseltraffic)).

Corpus procesado: 60.5M posiciones · 31,871 buques únicos · 7 días.

### Estructura del notebook "entrega1_oceanwatch.ipynb"

> Los pesos de la tabla siguiente son los de la rúbrica de **Entrega 1** (no los de Entrega 2).

| Sección | Contenido | Requisito (Entrega 1) |
|---|---|---|
| 0 | Configuración e imports (parámetros centralizados, estilo visual) | — |
| 1 | Ingesta documentada: descarga con reintentos + verificación de integridad (Content-Length, SHA-256, zip válido), descompresión en Volume, lectura con esquema explícito | Req. 1 (15%) |
| 2 | Exploración y perfilamiento: posiciones/día, buques únicos, distribución por tipo y tamaño, patrones temporales y espaciales | Req. 2 (20%) |
| 3 | Calidad de datos: detección y cuantificación de problemas (nulos, coordenadas, velocidades, rumbos, MMSI, dimensiones, duplicados) | Req. 2 (20%) |
| 4 | Preguntas de negocio (5) con Spark eficiente y planes de ejecución (".explain") | Req. 3 (25%) |
| 5 | Almacenamiento óptimo: Parquet plano vs. Delta particionado vs. Delta + "OPTIMIZE ZORDER", con evidencia (bytes, archivos leídos) | Req. 4 (25%) |
| 6 | Gobernanza: Unity Catalog (catálogo/esquema/volume) + comentarios y metadatos en tablas y columnas | Req. 5 (10%) |

### Cómo ejecutar (Entrega 1)

1. Importar "entrega1_oceanwatch.ipynb" en el workspace de Databricks (Workspace → Import).
2. Ajustar, si hace falta, los parámetros de la primera celda de código:
   - "CATALOG = oceanwatch", "SCHEMA = entrega1", "VOLUME = landing".
3. Ejecutar las celdas en orden. El notebook:
   - crea catálogo / esquema / volume en Unity Catalog,
   - descarga y descomprime los 7 días desde NOAA (idempotente),
   - lee con esquema explícito y ejecuta perfilamiento, calidad, preguntas de negocio,
     almacenamiento óptimo y gobernanza.

### Decisiones técnicas (Entrega 1)

- Esquema explícito (no "inferSchema"): control de tipos y menos pasadas de lectura sobre ~60M filas.
- Agregar con Spark → "toPandas()" → graficar; muestreo para distribuciones finas. Se evita traer
  el dataset completo al driver.
- Grilla espacial por redondeo lat/lon (alternativa aceptada a H3), justificada en el notebook.
- Almacenamiento: propósito de consulta = consulta diaria del operador por fecha y zona marítima;
  se compara Parquet plano vs. Delta particionado por "date" + "ZORDER (LAT, LON)", con evidencia de
  bytes y archivos leídos por la consulta objetivo (partition pruning + data skipping).
  - **Cruce con World Port Index (WPI):** redondeo de coordenadas de puertos y del tráfico AIS a dos
    decimales, "left join" sobre esa grilla espacial y top-10 de celdas con mayor tráfico junto con el
    puerto correspondiente.
    > **Corrección Entrega 2:** el WPI es un **shapefile** (`world_port_index/WPI.shp`), no un CSV de
    > Kaggle como decía el README original de Entrega 1. En Entrega 2 se lee el shapefile.

### Resultados clave (Entrega 1)

- Perfilamiento: 60.5M posiciones, 31,871 buques, 7 días completos. Tráfico dominado por Towing
  (16.7M) y Pleasure craft (14.5M); atributos estáticos con muchos nulos (Draft 64%, IMO 43%).
- Calidad: sentinelas AIS esperados (Heading=511 en 55.4%, COG=360 en 17.0%, SOG≥102.3 en 0.26%),
  dimensiones no físicas (Width/Length ≤0 ~3.5%), MMSI anómalos (0.08%), duplicados mínimos (<0.01%).
  Coordenadas sin valores fuera de rango.
- Preguntas de negocio: el conteo aproximado (HLL++) subestima ~5.6% en promedio (cardinalidad
  baja → se prefiere el exacto); Towing lidera el tráfico pero Cargo/Tanker son los más veloces; 39.8%
  de los buques transmiten los 7 días y 18.8% son visitantes de un solo día.
- Almacenamiento (evidencia): para la consulta objetivo (día + zona), Parquet plano lee 69/69
  archivos; Delta particionado por "date" lee 1 (partition pruning) y baja de 2,218 MB a 1,457 MB;
  "OPTIMIZE ZORDER (LAT, LON)" reduce a 1,357 MB. Layout recomendado: Delta particionado por "date" +
  ZORDER (LAT, LON).

### Artefactos en Unity Catalog (Entrega 1, parámetros por defecto)

- "oceanwatch.entrega1.vessel_type_catalog" — códigos VesselType → categoría legible.
- "oceanwatch.entrega1.data_quality_report" — diagnóstico de calidad (insumo Entrega 2).
- "oceanwatch.entrega1.positions_delta" — posiciones saneadas, particionadas por "date" y optimizadas.
- Volume "oceanwatch.entrega1.landing" — zips, CSV, Parquet baseline y manifiesto de ingesta.

> El esquema `oceanwatch.entrega1` se **preserva** sin modificaciones en Entrega 2 (ver ADR-002).

---

## Bitácora

La bitácora cronológica completa (con plantilla de entrada y las sesiones de Entrega 2) está en
[`docs/bitacora.md`](docs/bitacora.md). La tabla histórica de aportes de Entrega 1 se conserva aquí:

| Fecha | Integrante(s) | Aporte |
|---|---|---|
| Semana 5 | Todo el equipo | Ingesta y esquema explícito |
| Semana 5 | Todo el equipo | Perfilamiento y calidad de datos |
| Semana 6 | Todo el equipo | Preguntas de negocio |
| Semana 7 | Todo el equipo | Almacenamiento óptimo y gobernanza |

## Equipo

- Alejandro Abril - 202224328
- David Fernando Caro - 202222073
- Juan David Torres Albarracín - 202317608

## Fuentes

- AIS Data — Marine Cadastre / NOAA: https://hub.marinecadastre.gov/pages/vesseltraffic
- World Port Index (puertos): shapefile `world_port_index/WPI.shp` (NGA, dominio público).
