"""Tests de los helpers puros de orquestación (Fase 4, Req. 3) — sin Spark.

Cubren la validación del parámetro ``fecha_proceso``, el armado de la fila de
auditoría, la construcción de ``base_parameters`` y el ``MERGE`` idempotente de
auditoría por ``(job_run_id, task_key)``.
"""

from __future__ import annotations

import json

import pytest

import oceanwatch.orquestacion as orq


def test_validar_fecha_proceso_ok():
    assert orq.validar_fecha_proceso("2023-06-03") == "2023-06-03"


@pytest.mark.parametrize("mala", ["2023-6-3", "03-06-2023", "2023/06/03", "", "2023-13-01"])
def test_validar_fecha_proceso_rechaza_invalida(mala):
    with pytest.raises(ValueError):
        orq.validar_fecha_proceso(mala)


def test_tabla_auditoria_fqn(cfg):
    assert orq.tabla_auditoria_fqn(cfg) == "oceanwatch.quality.ejecuciones_job"


def test_fila_auditoria_claves_exactas():
    fila = orq.fila_auditoria(
        job_run_id=123, job_id=45, task_key="ingesta", fecha_proceso="2023-06-03",
        estado="SUCCESS", ts_inicio="2026-11-05T00:00:00+00:00",
        ts_fin="2026-11-05T00:05:00+00:00", filas_clave={"filas": 10}, detalle="ok",
    )
    assert set(fila) == set(orq.COLUMNAS_AUDITORIA)
    assert fila["job_run_id"] == 123
    assert fila["task_key"] == "ingesta"
    # filas_clave se serializa a JSON (STRING en la tabla).
    assert json.loads(fila["filas_clave"]) == {"filas": 10}


def test_fila_auditoria_valida_fecha():
    with pytest.raises(ValueError):
        orq.fila_auditoria(
            job_run_id=1, job_id=1, task_key="oro", fecha_proceso="fecha-mala",
            estado="SUCCESS", ts_inicio="x", ts_fin="y",
        )


def test_fila_auditoria_rechaza_task_key_vacio():
    with pytest.raises(ValueError):
        orq.fila_auditoria(
            job_run_id=1, job_id=1, task_key="", fecha_proceso="2023-06-03",
            estado="SUCCESS", ts_inicio="x", ts_fin="y",
        )


def test_base_parameters_plantilla(cfg):
    bp = orq.base_parameters(cfg)
    assert bp["fecha_proceso"] == "{{job.parameters.fecha_proceso}}"
    assert bp["job_run_id"] == "{{job.run_id}}"
    assert bp["job_id"] == "{{job.id}}"
    assert bp["task_key"] == "{{task.name}}"


def test_base_parameters_fecha_literal(cfg):
    bp = orq.base_parameters(cfg, fecha_proceso="2023-06-01")
    assert bp["fecha_proceso"] == "2023-06-01"


def test_base_parameters_fecha_literal_invalida(cfg):
    with pytest.raises(ValueError):
        orq.base_parameters(cfg, fecha_proceso="2023-99-01")


def test_merge_auditoria_sql_estructura():
    fila = orq.fila_auditoria(
        job_run_id=777, job_id=42, task_key="conciliacion", fecha_proceso="2023-06-03",
        estado="SUCCESS", ts_inicio="2026-11-05T00:00:00+00:00",
        ts_fin="2026-11-05T00:01:00+00:00", filas_clave={"bronce": 100}, detalle="cuadra",
    )
    sql = orq.merge_auditoria_sql("oceanwatch.quality.ejecuciones_job", fila)
    assert sql.startswith("MERGE INTO oceanwatch.quality.ejecuciones_job")
    assert "t.job_run_id = s.job_run_id AND t.task_key = s.task_key" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "777" in sql and "conciliacion" in sql


def test_merge_auditoria_sql_escapa_comillas():
    fila = orq.fila_auditoria(
        job_run_id=1, job_id=1, task_key="oro", fecha_proceso="2023-06-03",
        estado="SUCCESS", ts_inicio="t", ts_fin="t", detalle="no 'cuadra' aún",
    )
    sql = orq.merge_auditoria_sql("oceanwatch.quality.ejecuciones_job", fila)
    # La comilla simple se duplica para no romper el literal SQL.
    assert "no ''cuadra'' aún" in sql


def test_merge_auditoria_sql_falta_columna():
    with pytest.raises(KeyError):
        orq.merge_auditoria_sql("t", {"job_run_id": 1})
