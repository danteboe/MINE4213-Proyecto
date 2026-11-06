# ADR-005: Elección del caso de ML — detección de anomalías de velocidad (SOG)

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 6 (10%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La rúbrica (Req. 6) pide un componente de Machine Learning versionado con MLflow, sin exigir
maximizar precisión. El usuario decidió explícitamente **detección de anomalías de velocidad
(SOG)** y aplazar el pronóstico de arribos. Hay que fijar y justificar el caso de ML de modo
que reutilice el pipeline medallion, sea interpretable y reproducible, y encaje en serverless.

## 2. Criterios de decisión
- Disponibilidad de la señal en bronze/silver (SOG ya está en el esquema de 17 campos).
- Interpretabilidad del resultado (anomalías explicables al operador).
- Reproducibilidad con MLflow (tracking + registro de modelo).
- Coste en serverless (entrenamiento acotado, sin dependencias nativas pesadas).
- Alineación con el alcance decidido por el usuario.

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Anomalías de SOG | Umbral estadístico/IQR o IsolationForest por tipo de buque sobre SOG | Reusa SOG limpio y las reglas de calidad; simple, interpretable, reproducible | No "predice" el futuro, solo detecta atípicos | Entrega 1: sentinelas `SOG ≥ 102.3` en 0.26% + colas de SOG |
| B. Pronóstico de arribos por día/puerto | Series temporales de llegadas por puerto | Valor de negocio prospectivo | Mayor complejidad; el usuario lo APLAZÓ | Decisión del usuario (aplazar pronóstico) |
| C. Clasificación de tipo de buque | Clasificar `VesselType` a partir de cinemática | Etiqueta existente | Objetivo distinto; nulos altos en estáticos (Draft 64%, IMO 43%) | Entrega 1: perfilado de nulos |

## 4. Decisión
Se implementa **detección de anomalías de velocidad (SOG)** como caso de ML, registrada y
versionada con MLflow; el pronóstico de arribos a puerto **no** se implementa en esta entrega
(queda documentado y aplazado).

## 5. Justificación basada en evidencia
El perfilado de Entrega 1 aporta evidencia concreta sobre SOG: valores sentinela
`SOG ≥ 102.3` en el 0.26% de las posiciones y colas de velocidad ya caracterizadas, lo que
hace de SOG una señal lista y limpia para detección de anomalías sin ingeniería adicional
costosa. La reproducibilidad y el versionado se apoyan en
[MLflow tracking de Databricks](https://docs.databricks.com/aws/en/mlflow/) (acceso
2026-11-05), que permite registrar métricas, parámetros y el modelo. Como la rúbrica no exige
maximizar precisión, un detector interpretable (umbral estadístico/IQR o IsolationForest por
tipo de buque) es suficiente y barato en serverless, frente al pronóstico de arribos
(alternativa B), que el usuario aplazó, o la clasificación de tipo (C), penalizada por los
altos nulos en atributos estáticos observados en Entrega 1.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Anomaly Detection no supervisado** apoyado en un **Feature Store ligero + Model Registry
(MLflow)** para versionar features y modelo.

## 7. Consecuencias
- Positivas: alcance ML acotado, interpretable y alineado con el corpus; reutiliza SOG y las
  reglas de calidad de silver/gold; reproducible con MLflow.
- Negativas: se renuncia (temporalmente) al pronóstico de arribos.
- Deuda asumida: el pronóstico queda como posible extensión futura, fuera de esta entrega.

## 8. Cómo se verifica / reversibilidad
Al ejecutar (Fase posterior): un experimento MLflow con métricas y un modelo registrado sobre
las anomalías de SOG. Reversible archivando el experimento y el modelo registrado. Estado
actual: **Diseñado (no ejecutado)**.

## 9. Referencias
- MLflow en Databricks: https://docs.databricks.com/aws/en/mlflow/ (acceso 2026-11-05).
- Notebook `entrega1_oceanwatch.ipynb` (perfilado de SOG: sentinelas y colas).
- ADR-001 (arquitectura Medallion, capa `ml`).
