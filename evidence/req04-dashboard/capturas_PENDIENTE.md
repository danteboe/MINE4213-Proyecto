# Capturas de pantalla del dashboard — PENDIENTE DE CAPTURA POR EL EQUIPO

- **Qué demuestra:** evidencia visual del dashboard AI/BI renderizando con datos reales.
  Las consultas ya están **Verificadas (ejecutadas)** (ver `consultas_visuales.md`), pero
  las capturas de la interfaz las debe tomar una persona del equipo con sesión iniciada en
  el workspace (no se pueden automatizar desde la API sin navegador autenticado).
- **Cómo se genera:** abrir las URLs reales abajo con la cuenta
  `jd.torresa1@uniandes.edu.co` y tomar las capturas indicadas.
- **Fecha de preparación:** 2026-11-06.
- **IDs reales:** dashboard `01f1da0367841f3db6c6ddcb8edaa849`; warehouse `377600528fed7c47`.
- **Estado:** **PENDIENTE DE CAPTURA POR EL EQUIPO**.

## URLs reales

- **Publicado (vista de consumo):**
  `https://dbc-a568bc49-bdae.cloud.databricks.com/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849/published`
- **Borrador (editor):**
  `https://dbc-a568bc49-bdae.cloud.databricks.com/sql/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849`

## Procedimiento

1. Iniciar sesión en `https://dbc-a568bc49-bdae.cloud.databricks.com` con
   `jd.torresa1@uniandes.edu.co`.
2. Abrir la URL **publicada**. Confirmar que el warehouse serverless
   `377600528fed7c47` está activo (arranca en frío en ~10–20 s la primera vez).
3. Verificar que el filtro de **fecha** arranca en `2023-06-03` (default) y que los 4
   visuales muestran datos.

## Capturas requeridas (guardar como PNG en esta carpeta)

| Archivo sugerido | Qué debe mostrar |
|---|---|
| `01_dashboard_completo_20230603.png` | Página completa con `fecha = 2023-06-03`: título, filtro de fecha y los 4 visuales con datos. |
| `02_v1_mapa_intensidad.png` | Visual 1 (mapa de intensidad por celda) con puntos coloreados por intensidad; verificar la concentración en Seattle (47.63,-122.39), San Diego y zona de Houston. |
| `03_v2_ranking_puertos.png` | Visual 2 (barras) con NEW YORK CITY arriba (ese día). |
| `04_v3_buques_en_puerto.png` | Visual 3 (tabla) con MMSI, puerto, posiciones y primera/última posición. |
| `05_v4_anomalias_tabla_y_mapa.png` | Visual 4 (tabla + mapa de posiciones anómalas) con `fecha = 2023-06-03`. |
| `06_filtro_fecha_20230602.png` | El **filtro de fecha** cambiado a `2023-06-02`: V1–V3 cambian de datos y **V4 queda vacío** (comportamiento esperado del corpus, ver `validacion_parametro_fecha.md`). |
| `07_url_barra_navegador.png` | La barra del navegador mostrando la URL publicada con el `dashboard_id` real. |

> Nota: si el render nativo de mapa no estuviera habilitado en el workspace, los visuales
> V1/V4 aparecerán como *scatter* lat×lon con color por intensidad/`z_sog`; eso cumple
> igualmente el requisito (ver supuesto D5 en ADR-017). Documentar en la captura cuál quedó
> activo.
