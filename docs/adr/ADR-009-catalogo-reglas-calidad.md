# ADR-009: Catálogo de reglas de calidad de la capa plata y clasificación descartar-fila vs nulificar-atributo

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 2 (capas de calidad)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

La capa bronce (`oceanwatch.bronze.ais_raw`) es append-only e inmutable (ADR-006):
conserva el dato AIS tal como llega de NOAA, con todas sus anomalías. La capa plata
debe aplicar una **compuerta de calidad** que separe los registros limpios de los
defectuosos, de forma reproducible y resistente a datos nuevos del feed AIS.

El problema de diseño es doble:

1. **Qué validar y con qué umbral.** Los umbrales no pueden elegirse "a ojo": deben
   salir de medir la fuente. El diagnóstico real sobre 25.897.493 filas (3 días,
   `evidence/req02-capas-calidad/diagnostico_calidad_bronce.md`) da las distribuciones
   que los justifican.
2. **Qué hacer cuando una regla se viola.** No toda anomalía implica descartar la
   fila: un rumbo "no disponible" (centinela 511) no invalida la posición del buque.
   Hace falta una clasificación inequívoca por regla.

Restricciones: cómputo **solo serverless** (no hay all-purpose cluster); DLT/Lakeflow
Declarative no está confirmado en serverless, por lo que la plata se materializa con
Structured Streaming + `foreachBatch` replicando el comportamiento `expect` /
`expect_or_drop` (ADR-011/FEAT-003). Las reglas deben ser **funciones puras**
testeables sin Spark para poder verificarlas localmente con `pytest`.

## 2. Criterios de decisión

- **Fundamento en evidencia:** cada umbral citado contra el diagnóstico real.
- **Reproducibilidad y testeabilidad:** reglas como funciones puras verificables sin Spark.
- **Parametrización:** umbrales y acciones en `conf/config.yaml` (bloque `calidad`), no hardcodeados.
- **Resiliencia a datos nuevos:** reglas presentes aunque el corpus actual tenga 0 casos (p. ej. coordenada fuera de rango).
- **Conservación de información:** no descartar filas útiles por una anomalía en un atributo no crítico.
- **Trazabilidad:** nombre estable + versión por regla para que FEAT-002/FEAT-003 las consuman.

## 3. Alternativas consideradas

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Descartar toda fila con cualquier anomalía | Enviar a cuarentena cualquier registro que falle una sola regla (incluido Heading=511 o COG=360) | Capa plata muy "limpia"; lógica simple | Descartaría ~55.5% de filas solo por Heading=511 y ~16.9% por COG=360, **perdiendo posiciones válidas**; destruye la señal de tráfico | Heading=511=14.367.009, COG=360=4.376.798 (diagnóstico) |
| B. Nulificar atributos no críticos y descartar solo lo que invalida el registro | Clasificar cada regla: descartar-fila (cuarentena) solo cuando falla un atributo crítico (coordenada, identidad, tiempo); nulificar + bandera el atributo no crítico (rumbo/velocidad/dimensión no disponibles) | Conserva la posición del buque; separa "dato ausente" de "fila inválida"; alinea con la práctica de calidad de datos (completitud por atributo, no por fila) | Más estados por fila (banderas de nulificación); requiere documentar la clasificación | DAMA-DMBOK: dimensiones de calidad por atributo; MMSI no 9 díg.=21.960 (0.085%) frente a 42.5% IMO vacío |

Práctica de calidad de datos de referencia: las dimensiones de calidad (exactitud,
completitud, validez, consistencia) se evalúan **por atributo**, de modo que un valor
ausente en un campo no obliga a descartar el registro completo
([DAMA-DMBOK, Data Quality](https://www.dama.org/cpages/body-of-knowledge), acceso 2026-11-05).
Contenido reformulado para cumplir las restricciones de licenciamiento.

## 4. Decisión

Se adopta la **alternativa B**: un catálogo de reglas de calidad versionadas (v1),
cada una clasificada de forma inequívoca como **descartar-fila** (a cuarentena) o
**nulificar-atributo** (la fila pasa, el atributo va a `null` + bandera):

- **Descartar-fila (cuarentena):** `coordenadas_en_rango`, `coordenada_no_centinela`,
  `mmsi_bien_formado`, `mmsi_mid_valido`, `timestamp_valido_no_futuro` (y, cuando
  ADR-010 lo incorpore, coordenada en tierra). El duplicado exacto MMSI+BaseDateTime
  lo elimina `dropDuplicatesWithinWatermark` (ADR-006), contabilizado aparte.
- **Nulificar-atributo:** `sog_plausible`/`sog_es_centinela`, `heading_valido`/
  `heading_es_centinela`, `cog_valido`/`cog_es_centinela`, `dimension_fisica`
  (Length/Width/Draft), `imo_valido` (vacío es VÁLIDO).

## 5. Justificación basada en evidencia

Umbrales y clasificación justificados contra el diagnóstico de 25.897.493 filas:

- **Coordenadas (descartar):** 0 fuera de rango, 0 centinelas LAT=91/LON=181 en el
  corpus. La regla se mantiene por robustez ante feed futuro; cuando una posición no
  es físicamente localizable, la fila no sirve => cuarentena.
- **MMSI (descartar):** MMSI no de 9 dígitos=**21.960** (~0.085%). El MMSI es la
  identidad del buque (ITU-R M.585); sin una identidad bien formada el registro no es
  conciliable => cuarentena. El MID (3 primeros dígitos) debe caer en el rango ITU
  [201,775] ([ITU, Table of Maritime Identification Digits](https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx),
  acceso 2026-11-05).
- **Timestamp (descartar):** 0 futuros/nulos en el corpus; regla por robustez (el
  serverless usa UTC, ADR-004).
- **SOG (nulificar):** SOG>=102.3=**65.510** filas son el centinela "no disponible",
  no velocidad. El máximo físico plausible se fija en **80 kn**: muy por encima de
  P99.9=32.5 kn (deja pasar buques rápidos reales) y por debajo del centinela 102.3,
  que es el corte inequívoco. La velocidad ausente no invalida la posición => null + bandera.
- **Heading (nulificar):** Heading=511=**14.367.009** (~55.5%) es centinela. Descartar
  esas filas destruiría más de la mitad del corpus (alternativa A rechazada).
- **COG (nulificar):** COG=360=**4.376.798** (~16.9%) es centinela, mismo razonamiento.
- **Dimensiones (nulificar):** Length<=0=**905.785**, Width<=0=**967.816**,
  Draft<=0=**1.180.265**. Son atributos **estáticos del buque**, no de la posición;
  un valor no físico se nulifica sin descartar la observación de tráfico.
- **IMO (nulificar, vacío VÁLIDO):** IMO vacío=**11.015.974** (~42.5%). No todos los
  buques tienen IMO, así que vacío es válido; si viene, se valida el dígito verificador
  del esquema de numeración IMO ([OMI, Resolución A.1117(30)](https://www.imo.org), acceso 2026-11-05).

## 6. Patrón de diseño / ingeniería de datos aplicado

**Data Quality Gate** (compuerta de calidad) con expectativas al estilo
**expect / expect_or_drop / expect_or_fail**: cada regla es una *expectativa* nombrada
y versionada; las expectativas `expect_or_drop` corresponden a descartar-fila (cuarentena)
y las `expect` (que no abortan ni descartan) corresponden a nulificar-atributo. En
serverless sin DLT, el patrón se replica con funciones puras (este FEAT) aplicadas en
`foreachBatch` (FEAT-003). Las funciones puras habilitan además el patrón **Fail Fast**
en pruebas unitarias: la calidad se verifica localmente antes de tocar Spark.

## 7. Consecuencias

Positivas: capa plata reproducible y auditable; conservación de posiciones válidas con
atributos ausentes marcados; umbrales parametrizados y versionados reutilizables por
FEAT-002 (regla tierra/agua) y FEAT-003 (materialización); verificación local sin coste
serverless. Negativas / deuda: cada fila plata porta banderas de nulificación (más
columnas); la clasificación es una decisión de diseño que deberá revisarse si el feed
AIS futuro cambia sus centinelas; el conteo real de cuarentena y la conciliación
(`registros_bronce = plata + cuarentena + duplicados`) quedan PENDIENTES DE EJECUCIÓN
hasta FEAT-003.

## 8. Cómo se verifica / reversibilidad

- **Local (ejecutado):** `pytest -q` desde la raíz del repo ejecuta los tests de
  `tests/test_reglas.py`, `tests/test_identificadores.py` y `tests/test_geo.py` sobre
  las funciones puras (rango de coordenadas, centinelas LAT=91/LON=181, SOG 102.3,
  Heading 511, COG 360, timestamp futuro, MMSI 9 dígitos + MID, IMO check digit
  correcto vs alterado, haversine contra distancia tabulada, redondeo de celda).
  Criterio observable: 57 passed, 1 skipped (base previa 20 passed).
- **Importabilidad sin Spark (ejecutado):**
  `$env:PYTHONPATH='src'; python -c "import oceanwatch.quality.reglas, oceanwatch.quality.identificadores, oceanwatch.quality.geo; print('ok')"`.
- **Real en Databricks (PENDIENTE DE EJECUCIÓN, FEAT-003):** conteos reales de
  `oceanwatch.silver.ais_clean` y `oceanwatch.quarantine.ais_rejected` vía la SQL
  Statement Execution API y verificación de la invariante de conciliación.
- **Reversibilidad:** las reglas son funciones puras sin estado y sus umbrales viven en
  `conf/config.yaml`; revertir un umbral o una clasificación es editar el YAML/ADR y
  re-ejecutar `pytest`, sin migración de datos.

## 9. Referencias

- `evidence/req02-capas-calidad/diagnostico_calidad_bronce.md` — diagnóstico real (25.897.493 filas), acceso 2026-11-05.
- [ITU-R M.585, Table of Maritime Identification Digits (MID)](https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx) — acceso 2026-11-05.
- [OMI, Resolución A.1117(30) — IMO Ship Identification Number Scheme](https://www.imo.org) — acceso 2026-11-05.
- [DAMA-DMBOK, Data Quality Management](https://www.dama.org/cpages/body-of-knowledge) — dimensiones de calidad por atributo, acceso 2026-11-05.
- ADR-001 (grilla por redondeo lat/lon, NO H3), ADR-004 (serverless/UTC), ADR-006 (dedup bronce->plata). Documentos internos.
- `docs/data_quality_rules.md` — catálogo completo de reglas (este ADR lo formaliza).
