# Evidencia — Diagnóstico de calidad real sobre bronce (insumo de umbrales, Req. 2)

- **Qué demuestra:** las distribuciones reales de los campos AIS en `oceanwatch.bronze.ais_raw`
  (25 897 493 filas, 3 días) que **justifican con evidencia** los umbrales de las reglas de
  calidad de la capa plata (Req. 2). No son umbrales elegidos "a ojo": salen de medir la fuente.
- **Cómo se generó:** consultas de agregación vía la SQL Statement Execution API sobre el
  warehouse serverless `377600528fed7c47`, directamente sobre la tabla bronce ingerida en Fase 2.
- **Fecha:** 2026-11-05
- **Origen:** consultas sobre `oceanwatch.bronze.ais_raw`.
- **Estado:** Verificado (ejecutado)

## Resultados reales (salida literal)

| Métrica | Valor real | Implicación para la regla de calidad |
|---|---|---|
| LAT/LON fuera de rango | 0 / 0 | Ninguna fila viola [-90,90]/[-180,180] |
| LAT=91 / LON=181 (centinela) | 0 / 0 | No hay centinelas de coordenada en el corpus |
| LAT/LON nulas | 0 / 0 | La coordenada siempre viene |
| SOG min / max / avg | 0.0 / 102.3 / 2.695 | Centinela superior claro en 102.3 |
| SOG ≥ 102.3 (centinela "no disp.") | **65 510** | Tratar 102.3 como centinela, no velocidad válida |
| SOG < 0 / nula | 0 / 0 | No hay negativos ni nulos |
| SOG percentiles (P50/P90/P99/P99.9) | 0.0 / 9.0 / 20.5 / 32.5 | Umbral físico de velocidad plausible muy por encima de 32.5 kn; el corte real es el centinela 102.3 |
| Heading = 511 (centinela) | **14 367 009** (~55.5 %) | Centinela dominante; tratar 511 como "no disponible" |
| Heading fuera de [0,359] (excl. 511) | 268 | Pocas filas con rumbo inválido no-centinela |
| COG = 360 (centinela) | **4 376 798** (~16.9 %) | Centinela de rumbo sobre fondo |
| COG fuera de rango (excl. 360) | 4 | Casi nulo |
| MMSI no de 9 dígitos | **21 960** (~0.085 %) | Regla MMSI de 9 dígitos; porcentaje bajo |
| MMSI nulo | 0 | Siempre presente |
| MMSI distintos | 26 463 | Buques únicos en 3 días |
| IMO vacío | **11 015 974** (~42.5 %) | La regla debe ACEPTAR vacío (no todos tienen IMO) |
| IMO con prefijo `IMO` | 14 881 519 | Forma válida esperada `IMO`+dígitos |
| IMO "otro" (ni vacío ni prefijo) | **0** | No hay formas inesperadas |
| BaseDateTime min / max | 2023-06-01T00:00:00Z / 2023-06-03T23:59:59Z | Dentro del corpus esperado |
| BaseDateTime futuros / nulos | 0 / 0 | Ninguno; la regla "no futuro" no descarta nada en este corpus |
| Length ≤ 0 | **905 785** | Dimensión no física |
| Width ≤ 0 | **967 816** | Dimensión no física |
| Draft ≤ 0 | **1 180 265** | Dimensión no física |
| `_rescued_data` no nulo | **0** | El esquema explícito de 17 campos casó perfecto; no hubo rescatados en el corpus real |

## Decisiones de umbral que esta evidencia habilita (para los ADRs de Fase 3)

1. **Coordenadas en rango:** LAT∈[-90,90], LON∈[-180,180]; excluir centinelas LAT=91/LON=181
   (0 casos aquí, pero la regla debe existir por robustez ante feed futuro).
2. **SOG plausible:** marcar SOG≥102.3 como centinela ("no disponible"); el máximo físico
   plausible se fija muy por encima de P99.9=32.5 kn (p. ej. 60–80 kn según tipo), porque entre
   32.5 y 102.3 hay señales reales de buques rápidos; 102.3 es el corte centinela inequívoco.
3. **Heading/COG:** 511 y 360 son centinelas "no disponible", no rumbos; no se descarta la fila
   por ellos, se marca el atributo como desconocido.
4. **MMSI:** exigir 9 dígitos (y MID válido); ~0.085 % no cumple → a cuarentena.
5. **IMO:** aceptar vacío; si viene, validar patrón `IMO`+dígitos (0 formas raras observadas).
6. **Timestamp:** válido y no futuro (0 descartes en este corpus, regla por robustez).
7. **Dimensiones:** Length/Width/Draft ≤0 son no físicas (cientos de miles de casos); decidir si
   se descartan o se nulifican como atributo (son estáticas del buque, no de la posición).

## Nota de consistencia con Entrega 1

Los porcentajes de centinelas coinciden con el diagnóstico de la Entrega 1 (Heading=511 ~55 %,
COG=360 ~17 %, SOG centinela ~0.25 %), lo que valida que el corpus ingerido es el esperado.
