"""Helpers PUROS de la Fase 4 (Orquestación, Req. 3) — sin PySpark.

Este paquete contiene la lógica que los notebooks orquestadores de tarea del Job de
Databricks (``notebooks/fase4_tarea_*.py``) reutilizan, pero expresada como funciones
puras y testeables SIN Spark (imports de PySpark perezosos o inexistentes):

- :func:`validar_fecha_proceso`: valida el parámetro ``fecha_proceso`` del Job
  reutilizando ``oceanwatch.ingest.paths._validar_fecha`` (formato ``YYYY-MM-DD`` y
  fecha de calendario real).
- :func:`fila_auditoria`: arma el ``dict`` de una fila de la tabla de auditoría
  ``oceanwatch.quality.ejecuciones_job`` (una por corrida+tarea).
- :func:`base_parameters`: construye el ``dict`` de ``base_parameters`` que el Job
  propaga a cada ``notebook_task`` (``fecha_proceso`` + widgets de identidad).
- :func:`tabla_auditoria_fqn`: nombre totalmente calificado de la tabla de auditoría.
- :func:`merge_auditoria_sql`: SQL ``MERGE`` idempotente por ``(job_run_id, task_key)``
  para insertar/actualizar una fila de auditoría; los notebooks lo ejecutan con
  ``spark.sql``.

La escritura pesada en serverless SIEMPRE es vía ``spark.sql`` (CTAS/MERGE), NUNCA
``df.write.saveAsTable`` (fricción serverless real: ``INTERNAL_ERROR`` sobre 25.9M
filas en Spark Connect; ver cabeceras de ``resources/sql/fase3_*.sql``).
"""

from __future__ import annotations

import json
from typing import Any, Optional

from oceanwatch.common.config import fqn
from oceanwatch.ingest.paths import _validar_fecha

# Claves EXACTAS de las columnas de oceanwatch.quality.ejecuciones_job
# (ver resources/sql/fase4_auditoria.sql). El orden importa para legibilidad, no
# para el MERGE (que usa nombres).
COLUMNAS_AUDITORIA: tuple[str, ...] = (
    "job_run_id",
    "job_id",
    "task_key",
    "fecha_proceso",
    "estado",
    "trigger",
    "ts_inicio",
    "ts_fin",
    "filas_clave",
    "detalle",
)


def validar_fecha_proceso(fecha: str) -> str:
    """Valida el parámetro ``fecha_proceso`` del Job (``YYYY-MM-DD``, fecha real).

    Reutiliza ``oceanwatch.ingest.paths._validar_fecha`` para no duplicar la regla de
    validación de fechas del proyecto (continuidad con la ingesta).

    Args:
        fecha: valor del widget/parámetro ``fecha_proceso``.

    Returns:
        La misma cadena ``fecha`` si es válida.

    Raises:
        ValueError: si el formato no es ``YYYY-MM-DD`` o no es una fecha de calendario.
    """
    return _validar_fecha(fecha)


def tabla_auditoria_fqn(cfg: dict[str, Any]) -> str:
    """Nombre totalmente calificado de la tabla de auditoría (``quality.ejecuciones_job``)."""
    aud = cfg["orquestacion"]["tabla_auditoria"]
    return fqn(cfg, aud["esquema"], aud["objeto"])


def fila_auditoria(
    job_run_id: int,
    job_id: int,
    task_key: str,
    fecha_proceso: Optional[str],
    estado: str,
    ts_inicio: str,
    ts_fin: str,
    filas_clave: Optional[dict[str, Any]] = None,
    detalle: str = "",
) -> dict[str, Any]:
    """Arma el ``dict`` de una fila de auditoría de ejecución del Job.

    Una fila por corrida+tarea. ``filas_clave`` (conteos relevantes de la tarea) se
    serializa a JSON para almacenarse como ``STRING`` en la tabla (columna ``filas_clave``),
    de modo que cada tarea reporte lo suyo sin cambiar el esquema.

    Args:
        job_run_id: id de la corrida del Job ({{job.run_id}}).
        job_id: id del Job ({{job.id}}).
        task_key: nombre de la tarea ({{task.name}}): ingesta/plata_calidad/conciliacion/oro.
        fecha_proceso: Fecha ISO o None cuando la corrida aún no ha identificado un día.
        estado: estado de la tarea (p. ej. ``SUCCESS``, ``FAILED``).
        ts_inicio: timestamp ISO de inicio de la tarea.
        ts_fin: timestamp ISO de fin de la tarea.
        filas_clave: conteos u otras métricas de la tarea; se serializa a JSON.
        detalle: texto libre (diagnóstico, invariante, etc.).

    Returns:
        ``dict`` con exactamente las claves de :data:`COLUMNAS_AUDITORIA`.
    """
    if not isinstance(job_run_id, int) or isinstance(job_run_id, bool):
        raise TypeError("job_run_id debe ser int.")
    if not isinstance(job_id, int) or isinstance(job_id, bool):
        raise TypeError("job_id debe ser int.")
    if not task_key:
        raise ValueError("task_key no puede estar vacío.")
    if fecha_proceso is not None:
        validar_fecha_proceso(fecha_proceso)
    return {
        "job_run_id": int(job_run_id),
        "job_id": int(job_id),
        "task_key": str(task_key),
        "fecha_proceso": fecha_proceso,
        "estado": str(estado),
        "trigger": "",  # lo completa el notebook (manual vs scheduled); vacío por defecto.
        "ts_inicio": str(ts_inicio),
        "ts_fin": str(ts_fin),
        "filas_clave": json.dumps(filas_clave or {}, ensure_ascii=False, sort_keys=True),
        "detalle": str(detalle),
    }


def base_parameters(cfg: dict[str, Any], fecha_proceso: Optional[str] = None) -> dict[str, str]:
    """Construye el ``base_parameters`` que el Job propaga a cada ``notebook_task``.

    Los valores son plantillas de parámetros/variables de Databricks que el Job
    sustituye en tiempo de ejecución: ``fecha_proceso`` desde el parámetro del Job y
    la identidad de la corrida/tarea para la auditoría.

    Args:
        cfg: configuración cargada.
        fecha_proceso: si se pasa una fecha explícita (validada), se usa como valor
            literal; si es ``None`` se usa la plantilla ``{{job.parameters.fecha_proceso}}``.

    Returns:
        ``dict`` con las claves de widget que leen los notebooks de tarea.
    """
    if fecha_proceso is None:
        valor_fecha = "{{job.parameters.fecha_proceso}}"
    else:
        valor_fecha = validar_fecha_proceso(fecha_proceso)
    return {
        "fecha_proceso": valor_fecha,
        "job_run_id": "{{job.run_id}}",
        "job_id": "{{job.id}}",
        "task_key": "{{task.name}}",
    }


def _sql_lit(valor: Any) -> str:
    """Serializa un valor Python a un literal SQL seguro (comillas escapadas).

    Solo se usa para construir el ``MERGE`` de auditoría a partir de valores internos
    del Job (ids numéricos, task_key, fecha, estado, texto), no de entrada de usuario.
    """
    if valor is None:
        return "NULL"
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, int):
        return str(valor)
    texto = str(valor).replace("'", "''")
    return f"'{texto}'"


def merge_auditoria_sql(tabla: str, fila: dict[str, Any]) -> str:
    """Genera el ``MERGE`` idempotente por ``(job_run_id, task_key)`` para una fila.

    Idempotencia (ADR-014): un reintento de la misma tarea en la misma corrida NO crea
    una fila nueva, actualiza la existente. ``_registrado`` se fija a
    ``current_timestamp()`` y ``ts_inicio``/``ts_fin`` a ``TIMESTAMP`` por ``CAST``.

    Args:
        tabla: nombre totalmente calificado de la tabla de auditoría.
        fila: ``dict`` producido por :func:`fila_auditoria` (claves de :data:`COLUMNAS_AUDITORIA`).

    Returns:
        Sentencia SQL ``MERGE INTO ...`` lista para ``spark.sql``.
    """
    faltan = [c for c in COLUMNAS_AUDITORIA if c not in fila]
    if faltan:
        raise KeyError(f"Faltan columnas en la fila de auditoría: {faltan}")
    ts_inicio = f"CAST({_sql_lit(fila['ts_inicio'])} AS TIMESTAMP)"
    ts_fin = f"CAST({_sql_lit(fila['ts_fin'])} AS TIMESTAMP)"
    return (
        f"MERGE INTO {tabla} AS t\n"
        f"USING (SELECT {_sql_lit(fila['job_run_id'])} AS job_run_id, "
        f"{_sql_lit(fila['task_key'])} AS task_key) AS s\n"
        f"ON t.job_run_id = s.job_run_id AND t.task_key = s.task_key\n"
        f"WHEN MATCHED THEN UPDATE SET\n"
        f"  job_id = {_sql_lit(fila['job_id'])},\n"
        f"  fecha_proceso = {_sql_lit(fila['fecha_proceso'])},\n"
        f"  estado = {_sql_lit(fila['estado'])},\n"
        f"  trigger = {_sql_lit(fila['trigger'])},\n"
        f"  ts_inicio = {ts_inicio},\n"
        f"  ts_fin = {ts_fin},\n"
        f"  filas_clave = {_sql_lit(fila['filas_clave'])},\n"
        f"  detalle = {_sql_lit(fila['detalle'])},\n"
        f"  _registrado = current_timestamp()\n"
        f"WHEN NOT MATCHED THEN INSERT (\n"
        f"  job_run_id, job_id, task_key, fecha_proceso, estado, trigger,\n"
        f"  ts_inicio, ts_fin, filas_clave, detalle, _registrado\n"
        f") VALUES (\n"
        f"  {_sql_lit(fila['job_run_id'])}, {_sql_lit(fila['job_id'])}, "
        f"{_sql_lit(fila['task_key'])}, {_sql_lit(fila['fecha_proceso'])}, "
        f"{_sql_lit(fila['estado'])}, {_sql_lit(fila['trigger'])},\n"
        f"  {ts_inicio}, {ts_fin}, {_sql_lit(fila['filas_clave'])}, "
        f"{_sql_lit(fila['detalle'])}, current_timestamp()\n"
        f")"
    )
