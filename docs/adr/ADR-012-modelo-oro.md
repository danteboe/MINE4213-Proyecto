# ADR-012: Modelo dimensional de la capa oro (trayectorias, presencia en puertos, matriz de tráfico)

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 2 (Capas de calidad, 15%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

La capa oro (FEAT-004) materializa el modelo de negocio sobre el lakehouse AIS. Se
alimenta **SOLO** de `oceanwatch.silver.ais_clean` (25.857.101 filas reales, verificadas
en FEAT-003): el oro NO limpia, agrega. Hay que decidir, con granularidad y clave
EXPLÍCITAS: (a) cómo incorporar el World Port Index (WPI) como dimensión de referencia
sin romper serverless; (b) cómo modelar la trayectoria diaria por buque y su distancia
recorrida; (c) qué significa geoespacialmente que un buque esté 'en puerto' y cuánta
permanencia exigir; (d) cómo cuadricular el espacio para la matriz de tráfico. Las
restricciones son: cómputo **solo serverless** (sin all-purpose cluster; `geopandas`,
`pyshp` y `global-land-mask` NO preinstaladas, verificado en
`evidence/req02-capas-calidad/disponibilidad_librerias_serverless.md`), UTC por defecto
(ADR-004), continuidad con la grilla por redondeo lat/lon de Entrega 1 (ADR-001) y la
necesidad de que las tablas resistan datos nuevos del feed AIS.

## 2. Criterios de decisión

- Separación de responsabilidades: el oro agrega, no limpia (hereda las garantías de plata).
- Granularidad y clave declaradas sin ambigüedad por tabla (reproducibilidad, versionado).
- Compatibilidad serverless: sin dependencias nativas pesadas; preferir expresiones nativas
  a UDFs escalares (una UDF fila-a-fila sobre 25.9M filas provocó `INTERNAL_ERROR` en FEAT-003).
- Continuidad con ADR-001 (grilla por redondeo lat/lon, NO H3) salvo evidencia fuerte.
- Coste y reproducibilidad de la carga del WPI (64.8 MB `.dbf`, 78 campos, 3.669 puertos).
- Parámetros de negocio (radio/celda de 'en puerto', permanencia) justificados y configurables.

## 3. Alternativas consideradas

### 3.1 Carga del World Port Index

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `.dbf` -> CSV local (dbfread) + Volume + `read_files` | Convertir el `.dbf` a CSV en el entorno local con `dbfread` (puro Python) y cargar el CSV como tabla de referencia | Sin dependencias nativas en serverless; reproducible; 3.669 filas triviales | Un paso local previo (conversión) | `disponibilidad_librerias_serverless.md` (geopandas/pyshp NO preinstaladas); `evidence/.../capa_oro.md` (3.669 filas cargadas) |
| B. Leer el shapefile en serverless con `%pip install pyshp` | Instalar `pyshp` en el notebook y leer `WPI.shp`/`.dbf` en Spark | Un solo entorno (serverless) | Añade dependencia pinneada + `%pip` en cada run; `pyshp`/`geopandas` arrastran coste de instalación; innecesario para 3.669 filas | [NGA World Port Index](https://msi.nga.mil/Publications/WPI) (acceso 2026-11-05) |
| C. `geopandas` en serverless | Leer el shapefile completo con geometría | Geometría lista para joins espaciales | `geopandas` arrastra GEOS/GDAL (instalación costosa en serverless); no se necesita la geometría, solo lat/lon de cada puerto | `disponibilidad_librerias_serverless.md` |

### 3.2 Regla 'en puerto': radio fijo (haversine) vs. celda de grilla

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Celda de grilla compartida | 'En puerto' = la posición y el puerto caen en la misma celda lat/lon redondeada a 2 decimales | *Join* por igualdad (barato a 25.9M filas); coherente con ADR-001 y con `matriz_trafico`; determinista | Celda ~1.1 km de lado (no un círculo exacto); cerca del polo la celda se estrecha en longitud | `evidence/.../capa_oro.md` (349 puertos cruzados, muestras coherentes: WARRENTON, POINT RICHMOND) |
| B. Radio fijo con haversine | 'En puerto' = `haversine_km(pos, puerto) <= R` | Radio circular exacto, independiente de la grilla | Requiere *cross join* o *range join* (caro a 25.9M × 3.669); elegir R es otro parámetro; sin índice espacial nativo en serverless se degrada | [haversine great-circle](https://en.wikipedia.org/wiki/Haversine_formula) (acceso 2026-11-05) |

### 3.3 Grilla de la matriz de tráfico: redondeo lat/lon vs. H3

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Redondeo lat/lon a 2 decimales | Celda = `(round(LAT,2), round(LON,2))` | Función pura determinista; sin dependencias nativas; ya validada en Entrega 1; *group by* nativo | Área de celda varía con la latitud (celdas más estrechas cerca del polo) | ADR-001 (Delta + ZORDER + grilla por redondeo superó a Parquet plano); `evidence/.../capa_oro.md` |
| B. H3 (índice hexagonal) | Indexar cada posición a una celda H3 de resolución fija | Área casi uniforme; vecindad hexagonal | Requiere librería H3 (nativa) + UDF en serverless; UDF escalar a 25.9M filas dio `INTERNAL_ERROR` en FEAT-003; sin beneficio demostrado para "consulta diaria por fecha + zona" | [Uber H3](https://h3geo.org/) (acceso 2026-11-05); findings FEAT-003 (UDF escalar no viable) |

### 3.4 Distancia recorrida: haversine vs. euclídea en grados

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Haversine (great-circle, R=6371 km) | Distancia esférica entre posiciones consecutivas | Error < 0.5% frente al elipsoide; km reales | Algo más de aritmética (trigonometría) | `src/oceanwatch/quality/geo.py` (`haversine_km`), `tests/test_geo.py` (LAX-JFK ~3983 km) |
| B. Distancia euclídea en grados | `sqrt(dLat^2 + dLon^2)` | Trivial | No es distancia física; distorsión brutal por latitud; inútil para km | [haversine great-circle](https://en.wikipedia.org/wiki/Haversine_formula) (acceso 2026-11-05) |

## 4. Decisión

Se adopta un **modelo dimensional (star-schema-lite)** en `oceanwatch.gold` con una
dimensión de referencia y tres tablas de hechos, todas alimentadas SOLO de
`oceanwatch.silver.ais_clean`:

- **`gold.world_port_index`** (dimensión): el WPI se convierte de `.dbf` a CSV
  **localmente con `dbfread`** (alternativa 3.1.A) y se carga vía Volume + `read_files`;
  3.669 puertos con los 7 campos clave.
- **`gold.trayectorias_diarias`** (hecho): granularidad **1 fila por `(MMSI, dia)`**;
  distancia recorrida = suma de **haversine** (alternativa 3.4.A) entre posiciones
  consecutivas (`LAG` por `(MMSI, dia)` ordenado por `BaseDateTime`).
- **`gold.presencia_puertos`** (hecho): granularidad **1 fila por `(MMSI, INDEX_NO, dia)`**;
  'en puerto' = **compartir celda de grilla a 2 decimales** con un puerto del WPI
  (alternativa 3.2.A, *Reference Data Join*), con **permanencia mínima de 3 posiciones**
  en la celda-puerto ese día.
- **`gold.matriz_trafico`** (hecho): granularidad **1 fila por `(celda_lat, celda_lon, hora)`**;
  grilla por **redondeo lat/lon a 2 decimales** (alternativa 3.3.A, ratifica ADR-001) y
  hora por `date_trunc('hour', BaseDateTime)` (UTC, ADR-004).

## 5. Justificación basada en evidencia

El entorno serverless no trae `geopandas`/`pyshp`
(`disponibilidad_librerias_serverless.md`): convertir el `.dbf` localmente con `dbfread`
elimina toda dependencia nativa y es trivial para 3.669 filas; la carga real produjo
**3.669** puertos (`capa_oro.md`). Usar la **celda de grilla** para 'en puerto' convierte
el cruce AIS↔WPI en un *join* por igualdad (barato a 25.9M filas) y es coherente con la
grilla de `matriz_trafico` y con ADR-001; la ejecución real cruzó **349** puertos con
muestras geográficamente plausibles (WARRENTON, POINT RICHMOND, NEW YORK CITY). El radio
fijo (3.2.B) exigiría un *range join* costoso sin índice espacial nativo. Se mantiene el
**redondeo lat/lon** frente a **H3** porque H3 requiere librería nativa + UDF, y FEAT-003
demostró que una UDF escalar a 25.9M filas provoca `INTERNAL_ERROR` en serverless; H3 no
aporta beneficio para la consulta objetivo (fecha + zona) y ADR-001 ya evidenció que la
grilla por redondeo es suficiente. El **haversine** (R=6371 km) es la distancia física
correcta (error < 0.5%), ya probada en `tests/test_geo.py` (LAX-JFK ≈ 3983 km). La
**permanencia de 3 posiciones** evita contar un tránsito de 1-2 pings como presencia en
puerto; es un parámetro configurable (`gold.presencia_puertos_param`). Todas las
transformaciones usan **expresiones nativas** (sin UDF escalar), por la fricción de
FEAT-003. Conteos reales citados: silver = 25.857.101; trayectorias = 61.768 (26.266
buques × 3 días); presencia = 9.153 (349 puertos, 4.592 buques); matriz = 2.319.365
(726.513 celdas × 72 horas). Ver `evidence/req02-capas-calidad/capa_oro.md`.

## 6. Patrón de diseño / ingeniería de datos aplicado

**Modelo dimensional / star-schema-lite** (una dimensión de referencia
`world_port_index` + tres tablas de hechos con granularidad declarada) combinado con
**Reference Data Join** (cruce del hecho AIS con la dimensión WPI por celda de grilla)
y **Spatial Grid Bucketing** (cuadriculado del espacio por redondeo lat/lon, ADR-001).

## 7. Consecuencias

- Positivas: granularidad y clave explícitas por tabla; consultas de negocio directas
  (trayectoria por buque/día, presencia en puertos, densidad de tráfico por celda/hora);
  carga del WPI reproducible sin dependencias nativas; linaje claro (todo deriva de plata).
- Negativas: la celda de grilla no es un radio circular exacto y su área varía con la
  latitud; `date_trunc` en UTC (los husos locales se resuelven aguas abajo).
- Deuda asumida: `distancia_total_km` hereda de plata la ausencia de un filtro de
  continuidad/velocidad entre posiciones, por lo que puede ser no física para MMSI con
  posiciones AIS que "saltan" (documentado como límite conocido en `capa_oro.md`). Un
  filtro de velocidad sería una regla de calidad adicional de plata (Fase futura), no del
  oro (el oro NO limpia). Gobernanza/evolución del esquema oro: PENDIENTE - Fase 5.

## 8. Cómo se verifica / reversibilidad

- Verificación: `SELECT count(*)` y muestras de las 4 tablas vía `_dbx_sql.py` sobre el
  SQL warehouse serverless `377600528fed7c47` (resultados en `capa_oro.md`); los módulos
  `oceanwatch.gold.*` importan sin Spark (imports perezosos) y `pytest -q` queda verde
  (incluye `tests/test_gold_puertos.py` para la decisión pura 'en puerto' por celda).
- Linaje: el `FROM` de cada CTAS (`resources/sql/fase3_materializar_oro.sql`) y de cada
  función de `src/oceanwatch/gold/` lee de `oceanwatch.silver.ais_clean` (no de bronce).
- Reversibilidad: `DROP TABLE` sobre las 4 tablas de `oceanwatch.gold`; reejecutar los
  CTAS (`CREATE OR REPLACE`) regenera el estado de forma idempotente.

## 9. Referencias

- NGA World Port Index: https://msi.nga.mil/Publications/WPI (acceso 2026-11-05).
- Fórmula de Haversine (great-circle): https://en.wikipedia.org/wiki/Haversine_formula (acceso 2026-11-05).
- Uber H3 (índice hexagonal): https://h3geo.org/ (acceso 2026-11-05).
- `read_files` (SQL) — Databricks: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files (acceso 2026-11-05).
- ADR-001 (grilla por redondeo lat/lon, NO H3), ADR-004 (UTC/serverless), ADR-011 (cuarentena).
- Evidencia interna: `evidence/req02-capas-calidad/capa_oro.md`, `.../disponibilidad_librerias_serverless.md`, `.../diagnostico_calidad_bronce.md`.
