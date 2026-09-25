# MINE4213 · Proyecto — OceanWatch Analytics

Proyecto del curso MINE 4213 · Soluciones Intensivas en Datos.
OceanWatch Analytics: el lakehouse del tráfico marítimo — análisis de datos AIS (NOAA) sobre
Databricks Free Edition.

## Entrega 1

Explorar, responder y almacenar de forma óptima el tráfico marítimo AIS de los días 1 al 7 de
junio de 2023 (fuente oficial: [Marine Cadastre / NOAA](https://hub.marinecadastre.gov/pages/vesseltraffic)).

Corpus procesado: 60.5M posiciones · 31,871 buques únicos · 7 días.

### Contenido del repositorio

```
MINE4213-Proyecto/
├── entrega1_oceanwatch.ipynb   # Notebook principal (Databricks) — Entrega 1 completa
└── README.md                   # Este archivo (documentación + bitácora)
```

### Estructura del notebook "entrega1_oceanwatch.ipynb"

| Sección | Contenido | Requisito |
|---|---|---|
| 0 | Configuración e imports (parámetros centralizados, estilo visual) | — |
| 1 | Ingesta documentada: descarga con reintentos + verificación de integridad (Content-Length, SHA-256, zip válido), descompresión en Volume, lectura con esquema explícito | Req. 1 (15%) |
| 2 | Exploración y perfilamiento: posiciones/día, buques únicos, distribución por tipo y tamaño, patrones temporales y espaciales | Req. 2 (20%) |
| 3 | Calidad de datos: detección y cuantificación de problemas (nulos, coordenadas, velocidades, rumbos, MMSI, dimensiones, duplicados) | Req. 2 (20%) |
| 4 | Preguntas de negocio (5) con Spark eficiente y planes de ejecución (".explain") | Req. 3 (25%) |
| 5 | Almacenamiento óptimo: Parquet plano vs. Delta particionado vs. Delta + "OPTIMIZE ZORDER", con evidencia (bytes, archivos leídos) | Req. 4 (25%) |
| 6 | Gobernanza: Unity Catalog (catálogo/esquema/volume) + comentarios y metadatos en tablas y columnas | Req. 5 (10%) |

### Cómo ejecutar

1. Importar "entrega1_oceanwatch.ipynb" en el workspace de Databricks (Workspace → Import).
2. Ajustar, si hace falta, los parámetros de la primera celda de código:
   - "CATALOG = oceanwatch", "SCHEMA = entrega1", "VOLUME = landing".
3. Ejecutar las celdas en orden. El notebook:
   - crea catálogo / esquema / volume en Unity Catalog,
   - descarga y descomprime los 7 días desde NOAA (idempotente),
   - lee con esquema explícito y ejecuta perfilamiento, calidad, preguntas de negocio,
     almacenamiento óptimo y gobernanza.
4. (Opcional, para la pregunta 4.d) Subir el World Port Index a
   "/Volumes/oceanwatch/entrega1/landing/wpi/WPI.csv" para el cruce de celdas con puertos.

### Decisiones técnicas

- Esquema explícito (no "inferSchema"): control de tipos y menos pasadas de lectura sobre ~60M filas.
- Agregar con Spark → "toPandas()" → graficar; muestreo para distribuciones finas. Se evita traer
  el dataset completo al driver.
- Grilla espacial por redondeo lat/lon (alternativa aceptada a H3), justificada en el notebook.
- Almacenamiento: propósito de consulta = consulta diaria del operador por fecha y zona marítima;
  se compara Parquet plano vs. Delta particionado por "date" + "ZORDER (LAT, LON)", con evidencia de
  bytes y archivos leídos por la consulta objetivo (partition pruning + data skipping).

### Resultados clave

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

### Artefactos en Unity Catalog (parámetros por defecto)

- "oceanwatch.entrega1.vessel_type_catalog" — códigos VesselType → categoría legible.
- "oceanwatch.entrega1.data_quality_report" — diagnóstico de calidad (insumo Entrega 2).
- "oceanwatch.entrega1.positions_delta" — posiciones saneadas, particionadas por "date" y optimizadas.
- Volume "oceanwatch.entrega1.landing" — zips, CSV, Parquet baseline y manifiesto de ingesta.

## Bitácora

Registro del aporte por clase (Req. 6).

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
- World Port Index (puertos): disponible desde la misma página del dataset.
