# ADR-010: Método de máscara tierra/agua para la regla de calidad "coordenada en el agua"

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 2 (capas de calidad / compuertas de la capa plata)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

La capa plata (Req. 2) aplica un catálogo de reglas de calidad sobre los reportes AIS de
`oceanwatch.bronze.ais_raw` (25 897 493 filas, diagnóstico en
`evidence/req02-capas-calidad/diagnostico_calidad_bronce.md`). Entre las reglas de validación
física de la posición, además de "coordenada en rango" y "no centinela" (ADR-009), interesa una
regla geográfica: **¿la coordenada cae en el agua?** Una posición AIS tierra adentro *puede* ser
un error de posición (GPS defectuoso, ruido), pero la geografía costera real (puertos, canales de
navegación, ríos navegables) hace que "tierra adentro" y "tráfico marítimo válido" se solapen.

Restricciones del entorno (verificadas, `disponibilidad_librerias_serverless.md`): el cómputo es
**exclusivamente serverless** (Python 3.11.10) y **ninguna** librería geoespacial
(`global-land-mask`, `geopandas`, `pyshp`) viene preinstalada; cualquiera exige `%pip install` en
tiempo de ejecución. El paquete `src/oceanwatch` debe seguir importándose **sin** esas librerías
(imports perezosos). La solución debe ser resistente a datos AIS nuevos del feed de NOAA.

## 2. Criterios de decisión

1. **Disponibilidad real en serverless** (costo del `%pip install`; preferir dependencias ligeras).
2. **Resolución** de la máscara (capacidad de distinguir costa/agua).
3. **Costo medido** de instalación y de clasificación a escala del corpus.
4. **Tasa de falsos positivos/negativos** medida contra una verdad de referencia.
5. **Reproducibilidad** (versión pinneada) e **importabilidad sin la librería** (imports perezosos).

## 3. Alternativas consideradas

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `global-land-mask` (ráster ~1/120°) | Máscara global tierra/agua basada en datos GSHHG; `globe.is_land(lat,lon)`. `%pip install global-land-mask==1.0.0`. | Sin dependencias nativas pesadas; instala rápido; clasificación vectorizada numpy muy barata; cobertura global (resistente a feed nuevo). | No preinstalada (coste `%pip`); resolución ~0.9 km marca como "tierra" puertos/canales interiores ⇒ alta tasa de FP-tierra sobre AIS costero. | Medición real: run `614704376036187` SUCCESS; `mascara_tierra_agua.md` |
| B. `geopandas` + shapefile de costa (Natural Earth) | Punto-en-polígono contra polígonos de costa. | Precisión poligonal ajustable. | `geopandas` arrastra GEOS/GDAL nativos, el `%pip install` más costoso en serverless; mayor latencia de arranque. | `disponibilidad_librerias_serverless.md` (no preinstalada) |
| C. `pyshp` (lector puro) + punto-en-poligono propio | Leer shapefile de costa con `pyshp` e implementar ray-casting. | Dependencia pura-Python ligera; sin GEOS/GDAL. | Hay que implementar/validar el punto-en-polígono; costo por punto alto sin índice espacial; mantenimiento del recurso costa. | `disponibilidad_librerias_serverless.md` (no preinstalada) |
| D. *Baseline* bounding-box sin dependencias | Cajas delimitadoras groseras de masas de agua; cero dependencias. | Siempre disponible; import garantizado; salvaguarda si `%pip` falla. | Resolución gro(no distingue costa); alta tasa de falsos negativos de agua por construcción. | `src/oceanwatch/quality/tierra_agua.py::en_agua_bbox` |

## 4. Decisión

Se adopta **`global-land-mask==1.0.0`** (alternativa A) como método principal de la máscara
tierra/agua, instalado con `%pip install` pinneado en el notebook serverless, con un **baseline
bounding-box sin dependencias** (alternativa D) como salvaguarda de respaldo; y, a la luz de la
medición empírica, la regla `coordenada_en_agua` se clasifica como **NULIFICAR-ATRIBUTO**: se
materializa la bandera booleana `en_agua` y la fila **pasa** a la capa plata, sin enviar ningún
registro a cuarentena por esta regla.

## 5. Justificación basada en evidencia

Medición real ejecutada en serverless (run `614704376036187`, `result_state = SUCCESS`; salida
literal en `evidence/req02-capas-calidad/mascara_tierra_agua.md`):

- **Costo:** `%pip install` + import ≈ **3.65 s** (una vez por sesión); clasificación vectorizada
  de **60 000 puntos en 0.006 s**. El costo dominante es el `%pip`, no la clasificación, que
  escala sin problema a los ~25.9 M de filas del corpus. Esto descarta B (GEOS/GDAL, instalación
  más cara) y C (costo por punto sin índice espacial) por relación costo/beneficio.
- **Exactitud en verdad de referencia:** sobre 6 puntos de océano abierto y 6 de tierra adentro
  profunda, **falsos negativos de agua = 0/6** y **falsos positivos de agua = 0/6** (tasas 0.0):
  en el régimen "claramente agua / claramente tierra" el método es exacto.
- **Hallazgo sobre AIS real:** el **72.28 %** de la muestra AIS se clasifica como *tierra*. El
  cruce con las celdas AIS más densas del corpus (conteo real) muestra que son **puertos y aguas
  interiores navegables** —Houston Ship Channel (29.8,−95.1; 390 510 filas), Nueva York (40.7,−74.0),
  Puget Sound (47.7,−122.4), delta del Mississippi (29.9,−90.1)— es decir **tráfico AIS VÁLIDO**
  que la máscara de costa ~1/120° no distingue de tierra.

Por tanto, descartar las filas "en tierra" (acción `descartar_fila`) eliminaría la mayoría del
corpus legítimo. La evidencia obliga a clasificar la regla como **NULIFICAR-ATRIBUTO**: se añade
la bandera `en_agua` (True/False/NULL si la coord. es nula) y la fila se conserva; el consumidor
de la capa oro decide si filtra por la bandera. El baseline D queda como salvaguarda documentada
para el caso en que `%pip install` no sea viable en una ejecución serverless futura.

## 6. Patrón de diseño / ingeniería de datos aplicado

**Reference Data Enrichment** (enriquecimiento con dato de referencia): la validación geoespacial
cruza cada posición contra un dato de referencia externo de la geografía tierra/agua (la máscara
ráster de `global-land-mask`) para derivar un atributo de calidad (`en_agua`). Se combina con el
patrón **Data Quality Gate** de ADR-009 en su variante *nulificadora* (estilo `expect` de las
expectativas de calidad: se marca, no se descarta).

## 7. Consecuencias

- **Positivas:** regla de bajo costo y exacta en los extremos; conserva el tráfico costero/fluvial
  (no destruye datos); el módulo se importa sin la librería (imports perezosos) y es testeable sin
  Spark; versión pinneada ⇒ reproducible; baseline sin dependencias disponible siempre.
- **Negativas / deuda asumida:** la bandera `en_agua` tiene ~72 % de "tierra" sobre AIS real por la
  resolución de la máscara, de modo que **no** es un filtro directo de errores de posición; un uso
  más fino (p. ej. buffer costero o proximidad a puerto WPI) queda como mejora futura. El `%pip
  install` añade ~3.65 s de arranque por sesión serverless.

## 8. Cómo se verifica / reversibilidad

- **Local (sin Spark):** `pytest -q` incluye `tests/test_tierra_agua.py` (baseline determinista
  siempre verde; método principal con `pytest.importorskip`). `$env:PYTHONPATH='src'; python -c
  "import oceanwatch.quality.tierra_agua"` importa **sin** la librería instalada.
- **Serverless (real):** run `614704376036187` `SUCCESS`; cotejar con
  `databricks.exe jobs get-run --run-id 614704376036187 --output json` y
  `databricks.exe jobs get-run-output 641524386080059 --output json`. Métricas en
  `evidence/req02-capas-calidad/mascara_tierra_agua.md`.
- **Reversibilidad:** la acción de la regla vive en `conf['calidad']['reglas']['coordenada_en_agua']
  ['accion']`; cambiarla a `descartar_fila` revierte la decisión sin tocar código. El método
  (`metodo`/`libreria`/`version`) también es parametrizable en `conf['calidad']['coordenada_en_agua']`.

## 9. Referencias

- `evidence/req02-capas-calidad/mascara_tierra_agua.md` — medición real (run 614704376036187), 2026-11-05.
- `evidence/req02-capas-calidad/disponibilidad_librerias_serverless.md` — librerías no preinstaladas, 2026-11-05.
- `evidence/req02-capas-calidad/diagnostico_calidad_bronce.md` — diagnóstico de 25 897 493 filas, 2026-11-05.
- `global-land-mask` (PyPI): https://pypi.org/project/global-land-mask/ (acceso 2026-11-05).
- GSHHG (Global Self-consistent, Hierarchical, High-resolution Geography), base de la máscara:
  https://www.ngdc.noaa.gov/mgg/shorelines/ (acceso 2026-11-05).
- ADR-009 (catálogo de reglas de calidad), ADR-001 (arquitectura medallion / grilla lat-lon).
