# ADR-NNN: <título>

- **Estado:** Propuesto | Aceptado | Reemplazado por ADR-XXX
- **Fecha:** AAAA-MM-DD
- **Requisito(s) de la rúbrica:** Req. N (peso %)
- **Autores:** <nombres>

## 1. Contexto y problema
<Qué situación obliga a decidir. Qué restricciones aplican (serverless, Unity Catalog, resistencia a datos nuevos).>

## 2. Criterios de decisión
<Lista de criterios ponderables: idempotencia, evolución de esquema, coste serverless, reproducibilidad, etc.>

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A | ... | ... | ... | <URL o referencia> |
| B | ... | ... | ... | <URL o referencia> |
<mínimo 2 alternativas>

## 4. Decisión
<La opción elegida, en una frase inequívoca. Sin "X o Y".>

## 5. Justificación basada en evidencia
<Por qué, citando las URLs/evidencia de la sección 3 y el inventario verificado del entorno.>

## 6. Patrón de diseño / ingeniería de datos aplicado
<Nombrar el patrón explícitamente (p. ej. Medallion, Idempotent Consumer, Trigger-once batch).>

## 7. Consecuencias
<Positivas y negativas; qué queda habilitado y qué deuda se asume.>

## 8. Cómo se verifica / reversibilidad
<Comando o criterio observable para comprobarla; cómo revertir si fuera necesario.>

## 9. Referencias
<URLs con fecha de acceso 2026-11-05, documentos internos, ADRs relacionados.>
