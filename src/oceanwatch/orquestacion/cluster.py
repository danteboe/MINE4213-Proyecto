"""DAG clásico de OceanWatch. Cada transformación recibe solo los días de su corrida."""

from __future__ import annotations

import csv
import hashlib
import json
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from oceanwatch.common.config import default_config_path, fqn
from oceanwatch.ingest import autoloader, download, paths
from oceanwatch.ingest.entradas import extraer_csv, fecha_archivo, normalizar_noaa_csv2, validar_csv
from oceanwatch.ingest.schema import ais_struct_type, columnas_ais
from oceanwatch.orquestacion import fila_auditoria, tabla_auditoria_fqn, validar_fecha_proceso


def dias_corrida(cfg, fecha, entrenar, desde=""):
    """La primera corrida prepara train y validación antes del día solicitado."""
    fecha = validar_fecha_proceso(fecha)
    inicio = date.fromisoformat(validar_fecha_proceso(desde or fecha))
    intervalo = (date.fromisoformat(fecha) - inicio).days + 1
    if not 1 <= intervalo <= 31:
        raise ValueError("El intervalo inclusivo de backfill debe tener entre 1 y 31 días")
    solicitados = [(inicio + timedelta(days=i)).isoformat() for i in range(intervalo)]
    if entrenar not in {"true", "false"}:
        raise ValueError("entrenar debe valer true o false")
    if entrenar == "false":
        return solicitados
    ml = cfg["ml_cluster"]
    if not max(ml["train_dates"]) < ml["validation_date"] <= fecha:
        raise ValueError("La fecha debe ser igual o posterior a validation_date")
    return sorted(set([*ml["train_dates"], ml["validation_date"], *solicitados]))


def _registro_landing(cfg, carpeta):
    return Path(cfg["ruta_volumen"], "control_landing", carpeta)


def ruta_entrada(cfg):
    return cfg.get("ruta_entrada") or f"{cfg['ruta_volumen']}/landing/inbox"


def manifiesto_corrida(cfg, job_run_id):
    return _registro_landing(cfg, "corridas") / f"{job_run_id}.json"


def dia_col(spark, tabla, fecha, columna="_batch_date"):
    return spark.table(tabla).where(F.col(columna) == F.lit(fecha).cast("date"))


def reemplazar_dia(df, tabla, fecha, comentario, columna="_batch_date", particionar=False):
    """Publica una fecha con un único commit, incluso si el resultado está vacío."""
    spark = df.sparkSession
    if not spark.catalog.tableExists(tabla):
        esquema = ", ".join(f"`{c.name}` {c.dataType.simpleString()}" for c in df.schema)
        particiones = f"PARTITIONED BY ({columna})" if particionar else ""
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {tabla} ({esquema}) USING DELTA {particiones}"
        )
        spark.sql(f"COMMENT ON TABLE {tabla} IS :comentario", args={"comentario": comentario})
    (
        df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"{columna} = DATE '{validar_fecha_proceso(fecha)}'")
        .saveAsTable(tabla)
    )


def preparar(spark, dbutils, cfg):
    tabla = autoloader.tabla_bronze_fqn(cfg)
    esquema = ", ".join(f"`{c.name}` {c.dataType.simpleString()}" for c in ais_struct_type(cfg))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabla} (
          {esquema}, _rescued_data STRING, _ingest_timestamp TIMESTAMP,
          _source_file STRING, _batch_date DATE NOT NULL
        ) USING DELTA PARTITIONED BY (_batch_date)
        COMMENT 'Archivos AIS de NOAA interpretados con esquema explícito y metadatos de origen.'
        TBLPROPERTIES ('delta.appendOnly' = 'true')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabla_auditoria_fqn(cfg)} (
          job_run_id BIGINT, job_id BIGINT, task_key STRING, fecha_proceso STRING,
          estado STRING, trigger STRING, ts_inicio TIMESTAMP, ts_fin TIMESTAMP,
          filas_clave STRING, detalle STRING, _registrado TIMESTAMP
        ) USING DELTA COMMENT 'Estado por corrida y tarea y métricas operativas en JSON.'
    """)
    wpi = fqn(cfg, "gold", cfg["gold"]["tabla_wpi"])
    if not spark.catalog.tableExists(wpi):
        csv_path = default_config_path().parents[1] / "resources" / "world_port_index.csv"
        with csv_path.open(encoding="utf-8", newline="") as archivo:
            registros = [
                (
                    int(r["INDEX_NO"]), r["PORT_NAME"], r["COUNTRY"],
                    float(r["LATITUDE"]), float(r["LONGITUDE"]), r["HARBORSIZE"], r["HARBORTYPE"],
                )
                for r in csv.DictReader(archivo)
            ]
        spark.createDataFrame(
            registros, "INDEX_NO INT, PORT_NAME STRING, COUNTRY STRING, LATITUDE DOUBLE, "
            "LONGITUDE DOUBLE, HARBORSIZE STRING, HARBORTYPE STRING",
        ).coalesce(1).write.format("delta").mode("error").saveAsTable(wpi)
        spark.sql(f"COMMENT ON TABLE {wpi} IS 'World Port Index, referencia NGA proyectada.'")
    dbutils.fs.mkdirs(paths.landing_csv_root(cfg))
    dbutils.fs.mkdirs(ruta_entrada(cfg))
    for carpeta in ("confirmados", "corridas"):
        dbutils.fs.mkdirs(str(_registro_landing(cfg, carpeta)))
    return {"bronze": tabla, "puertos": wpi}


def _copiar_csv(dbutils, fuente, destino):
    dbutils.fs.mkdirs(str(destino.parent))
    if not dbutils.fs.cp(fuente, str(destino)):
        raise OSError(f"No se publicó {destino}")


def publicar_csv(dbutils, cfg, fecha):
    """Infla en disco local y publica solo CSV completos y con ruta estable."""
    destino = Path(paths.landing_csv_dir(cfg, fecha)) / f"AIS_{fecha.replace('-', '_')}.csv"
    if destino.is_file() and destino.stat().st_size > 0:
        return {"csv_bytes": destino.stat().st_size, "reutilizado": True}

    with tempfile.TemporaryDirectory(prefix="oceanwatch-") as tmp:
        zip_path = Path(tmp) / "noaa.zip"
        csv_path = Path(tmp) / destino.name
        download.descargar_zip(paths.url_zip_noaa(cfg, fecha), str(zip_path))
        with zip_path.open("rb") as archivo:
            sha256 = hashlib.file_digest(archivo, "sha256").hexdigest()
        extraer_csv(zip_path, csv_path)
        validar_csv(csv_path, columnas_ais(cfg))
        _copiar_csv(dbutils, csv_path.as_uri(), destino)
        if destino.stat().st_size != csv_path.stat().st_size:
            dbutils.fs.rm(str(destino))
            raise OSError(f"CSV incompleto para {fecha}")
        return {"zip_sha256": sha256, "csv_bytes": csv_path.stat().st_size, "reutilizado": False}


def _sha256(ruta):
    with ruta.open("rb") as archivo:
        return hashlib.file_digest(archivo, "sha256").digest()


def _publicar_entrada(dbutils, cfg, entrada, fecha):
    es_zst = entrada.name.endswith(".csv.zst")
    nombre_csv = entrada.stem if es_zst else entrada.stem + ".csv"
    destino = Path(paths.landing_csv_dir(cfg, fecha), nombre_csv)
    columnas = columnas_ais(cfg)
    with tempfile.TemporaryDirectory(prefix="oceanwatch-entrada-") as tmp:
        fuente = entrada
        if es_zst:
            fuente = Path(tmp, destino.name)
            normalizar_noaa_csv2(entrada, fuente, columnas)
        elif entrada.suffix == ".zip":
            fuente = Path(tmp, destino.name)
            extraer_csv(entrada, fuente)
        validar_csv(fuente, columnas)
        tamano = fuente.stat().st_size
        if destino.exists():
            if destino.stat().st_size != tamano or _sha256(destino) != _sha256(fuente):
                raise ValueError(f"Ya existe otro contenido en {destino.name}. Use un sufijo nuevo para un lote adicional.")
            return
        ubicacion = str(fuente) if fuente == entrada else fuente.as_uri()
        _copiar_csv(dbutils, ubicacion, destino)
        if destino.stat().st_size != tamano:
            dbutils.fs.rm(str(destino))
            raise OSError(f"CSV publicado incompleto: {destino.name}")


def confirmar_entradas(dbutils, cfg, lote):
    """La recepción se cierra solo después de publicar oro e inferencia."""
    for nombre in lote["entradas"]:
        marca = _registro_landing(cfg, "confirmados") / nombre
        if not dbutils.fs.put(str(marca), "publicado\n", True):
            raise OSError(f"No se pudo confirmar {nombre}")


def ingerir(spark, dbutils, cfg, fecha, desde, entrenar, job_run_id):
    if not fecha and (desde or entrenar != "false"):
        raise ValueError("fecha_inicio y entrenar requieren fecha_proceso explícita")
    solicitadas = dias_corrida(cfg, fecha, entrenar, desde) if fecha else []
    confirmados = {i.name for i in dbutils.fs.ls(str(_registro_landing(cfg, "confirmados")))}
    inbox = ruta_entrada(cfg)
    entradas = {
        i.name: fecha_archivo(i.name)
        for i in sorted(dbutils.fs.ls(inbox), key=lambda info: info.name)
        if i.name.endswith((".zip", ".csv", ".csv.zst")) and i.name not in confirmados
    }
    for nombre, dia in entradas.items():
        _publicar_entrada(dbutils, cfg, Path(inbox, nombre), dia)
    dias_entrada = set(entradas.values())
    archivos = {dia: publicar_csv(dbutils, cfg, dia) for dia in solicitadas}
    fechas = sorted(dias_entrada.union(solicitadas))
    lote = {
        "fechas": fechas,
        "fechas_ml": sorted(dias_entrada.union(dia for dia in solicitadas if dia >= (desde or fecha))),
        "entradas": list(entradas),
    }
    manifiesto = manifiesto_corrida(cfg, job_run_id)
    if not dbutils.fs.put(str(manifiesto), json.dumps(lote), True):
        raise OSError(f"No se guardó el manifiesto de la corrida {job_run_id}")
    if not fechas:
        return {**lote, "filas_entrada": 0}
    query = autoloader.ejecutar_ingesta(spark, cfg)
    tabla = autoloader.tabla_bronze_fqn(cfg)
    for fecha in fechas:
        if not dia_col(spark, tabla, fecha).take(1):
            raise RuntimeError(f"El stream terminó sin publicar datos de {fecha}")
    return {
        **lote,
        "archivos": archivos,
        "filas_entrada": sum(p["numInputRows"] for p in query.recentProgress),
        "query_id": str(query.id),
        "query_run_id": str(query.runId),
    }


def registrar(spark, cfg, contexto, inicio, estado, resultados=None, detalle=""):
    fila = fila_auditoria(
        **{k: contexto[k] for k in ("job_run_id", "job_id", "task_key", "fecha_proceso")},
        estado=estado,
        ts_inicio=inicio,
        ts_fin=datetime.now(timezone.utc).isoformat(),
        filas_clave=resultados,
        detalle=detalle[:2000],
    )
    fila["trigger"] = contexto["trigger"]
    tabla = tabla_auditoria_fqn(cfg)
    cols = tuple(fila)
    expresiones = [
        f"CAST(:{c} AS TIMESTAMP) AS {c}" if c in {"ts_inicio", "ts_fin"} else f":{c} AS {c}"
        for c in cols
    ]
    spark.sql(
        f"""
        MERGE INTO {tabla} t
        USING (SELECT {', '.join(expresiones)}, current_timestamp() AS _registrado) s
        ON t.job_run_id = s.job_run_id AND t.task_key = s.task_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        args=fila,
    )
