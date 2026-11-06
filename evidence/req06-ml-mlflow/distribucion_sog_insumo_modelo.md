# Evidencia — Distribución real de SOG (insumo del modelo de anomalías, Req. 6)

- **Qué demuestra:** la señal real de velocidad (SOG) en la capa plata que justifica el caso de
  ML elegido (detección de anomalías de velocidad, ADR-005) y la decisión de modelar la anomalía
  **relativa al tipo de buque**. Datos reales, no supuestos.
- **Cómo se generó:** agregaciones sobre `oceanwatch.silver.ais_clean` (excluyendo centinela SOG)
  vía SQL Statement Execution API, warehouse serverless `377600528fed7c47`.
- **Fecha:** 2026-11-05
- **Estado:** Verificado (ejecutado)

## SOG global (filas con SOG válido = no centinela)

- Filas con SOG válido: **25 791 600** (de 25 857 101 en plata; solo 65 501 son centinela "no disponible").
- avg = 2.443 kn · stddev = 4.731 kn
- Percentiles P50 / P95 / P99 / P99.9 = **0.0 / 12.4 / 20.5 / 32.5** kn

## SOG por tipo de buque (top 12 por volumen)

| VesselType | n | SOG avg | SOG std | SOG max | P99 |
|---|---|---|---|---|---|
| 31 (towing) | 6 967 749 | 1.73 | 3.01 | 78.8 | 10.9 |
| 37 (pleasure) | 6 239 432 | 1.49 | 4.41 | 102.2 | 24.2 |
| 60 (passenger) | 2 073 142 | 4.22 | 7.05 | 102.2 | 28.9 |
| 30 (fishing) | 1 763 062 | 2.29 | 3.60 | 63.9 | 13.8 |
| 36 (sailing) | 1 653 085 | 1.66 | 2.75 | 102.2 | 8.9 |
| 70 (cargo) | 1 622 024 | 6.43 | 6.40 | 102.2 | 19.6 |
| 90 (other) | 1 593 610 | 1.71 | 4.06 | 102.2 | 18.7 |
| 52 (tug) | 816 539 | 1.81 | 3.19 | 84.6 | 11.3 |
| 80 (tanker) | 777 136 | 5.85 | 6.03 | 69.0 | 16.6 |
| 57 | 579 653 | 1.92 | 2.98 | 89.2 | 10.2 |
| 0 (no definido) | 278 929 | 1.55 | 3.84 | 102.2 | 19.0 |
| 33 (dredging) | 162 584 | 1.22 | 3.64 | 55.6 | 21.3 |

## Implicaciones para el modelo (para el ADR del caso ML)

1. **La normalidad de velocidad depende del tipo de buque:** cargo (70) y tanker (80) navegan a
   6+ kn de media; towing (31) y pleasure (37) a ~1.5. Una misma velocidad (ej. 25 kn) es normal
   en cargo pero anómala en towing. => features o normalización por `VesselType`.
2. **Hay cola de valores extremos reales** (máximos 55-102 kn tras excluir el centinela 102.3):
   candidatos a anomalía. El modelo simple (ej. z-score por tipo, IsolationForest, o umbral
   estadístico por grupo) debe señalarlos.
3. **Volumen suficiente** (25.8M filas) para una partición temporal train/test sin fuga de datos
   (entrenar con 2023-06-01/02, evaluar 06-03, o split temporal análogo — definir en el ADR).
4. El objetivo NO es maximizar precisión (rúbrica): mantener el modelo simple y reproducible,
   trackeado en MLflow y registrado en Unity Catalog.
