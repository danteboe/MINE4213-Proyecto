"""Materialización de la capa PLATA: plata + cuarentena + métricas de calidad.

Replica el patrón de ``src/oceanwatch/ingest/autoloader.py`` (``foreachBatch`` +
idempotencia ``txnAppId``/``txnVersion``) pero para el salto bronce->plata:

1. Lee ``oceanwatch.bronze.ais_raw`` (stream o batch).
2. Deduplica REUTILIZANDO ``oceanwatch.ingest.dedup.deduplicar`` (ADR-006; NO se reescribe).
3. Aplica ``oceanwatch.quality.compuertas.aplicar_compuertas`` (nulifica atributos +
   banderas y construye ``causas`` array<string> de reglas de descarte violadas).
4. SEPARA por ``size(causas)=0``:
   - PASAN  -> ``oceanwatch.silver.ais_clean`` (Delta).
   - RECHAZADOS -> ``oceanwatch.quarantine.ais_rejected`` (registro original COMPLETO +
     ``causas`` + ``_batch_date`` + ``_fecha_cuarentena``). La cuarentena es un
     *side-output* (patrón Dead-letter/Quarantine, ADR-011); NO detiene el flujo.
5. Calcula y escribe métricas por lote/día y por regla en
   ``oceanwatch.quality.metricas_calidad`` (evaluados, pasados, rechazados, % rechazo, alerta).

Imports de PySpark SIEMPRE perezosos: el módulo se importa sin Spark.
"""

from __future__ import annotations

from typing import Any

from oceanwatch.common.config import fqn
from oceanwatch.ingest.dedup import deduplicar
from oceanwatch.ingest.schema import columnas_ais, columnas_auditoria
from oceanwatch.quality.compuertas import aplicar_compuertas


def tabla_bronze_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla bronce de origen (``oceanwatch.bronze.ais_raw``)."""
    return fqn(cfg, "bronze", cfg["ingesta"]["tabla_bronze"])


def tabla_silver_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla plata limpia (``oceanwatch.silver.ais_clean``)."""
    return fqn(cfg, "silver", cfg["silver"]["tabla_clean"])


def tabla_cuarentena_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla de cuarentena (``oceanwatch.quarantine.ais_rejected``)."""
    return fqn(cfg, "quarantine", cfg["quarantine"]["tabla_rejected"])


def tabla_metricas_fqn(cfg: dict[str, Any]) -> str:
    """FQN de las métricas de calidad (``oceanwatch.quality.metricas_calidad``)."""
    return fqn(cfg, "quality", cfg["quality_plata"]["tabla_metricas_calidad"])


def columnas_originales(cfg: dict[str, Any]) -> list[str]:
    """Las 17 columnas AIS + las 4 de auditoría (el registro original COMPLETO)."""
    return columnas_ais(cfg) + columnas_auditoria()


def separar_pasan_rechazados(df_compuertas: Any, cfg: dict[str, Any]):
    """Separa el DataFrame ya pasado por compuertas en (pasan, rechazados).

    ``pasan`` = ``size(causas)=0``; ``rechazados`` = ``size(causas)>0``. Una fila con
    varias causas cuenta UNA sola vez en cuarentena (es una sola fila con un array).
    """
    from pyspark.sql import functions as F

    col_causas = cfg["quarantine"]["columna_causas"]
    pasan = df_compuertas.filter(F.size(F.col(col_causas)) == 0)
    rechazados = df_compuertas.filter(F.size(F.col(col_causas)) > 0)
    return pasan, rechazados


def construir_cuarentena(df_rechazados: Any, cfg: dict[str, Any]):
    """Proyecta la cuarentena: registro ORIGINAL completo + causas + fechas.

    Mantiene las 17+4 columnas originales (bronce), añade ``causas`` (array<string>),
    conserva ``_batch_date`` y añade ``_fecha_cuarentena = current_timestamp()``.
    """
    from pyspark.sql import functions as F

    col_causas = cfg["quarantine"]["columna_causas"]
    col_fecha = cfg["quarantine"]["columna_fecha_cuarentena"]
    columnas = columnas_originales(cfg) + [col_causas]
    return df_rechazados.select(*columnas).withColumn(col_fecha, F.current_timestamp())


def construir_plata(df_pasan: Any, cfg: dict[str, Any]):
    """Proyecta la plata: columnas originales (ya nulificadas) + banderas.

    Descarta la columna ``causas`` (vacía por construcción en las filas que pasan) y
    conserva los atributos (ya nulificados por ``aplicar_compuertas``) más las banderas.
    """
    col_causas = cfg["quarantine"]["columna_causas"]
    banderas = list(cfg["silver"]["banderas"].values())
    columnas = [c for c in df_pasan.columns if c != col_causas]
    # Garantiza que las banderas estén presentes (ya añadidas por aplicar_compuertas).
    for b in banderas:
        if b not in columnas:
            columnas.append(b)
    return df_pasan.select(*columnas)


def calcular_metricas(df_compuertas: Any, cfg: dict[str, Any]):
    """Calcula métricas por día (``_batch_date``) y por regla de DESCARTE.

    Para cada combinación (día, regla) devuelve una fila con:
    - ``evaluados``: filas del día (todas se evalúan contra toda regla).
    - ``rechazados``: filas del día cuya ``causas`` contiene esa regla.
    - ``pasados`` = evaluados - rechazados.
    - ``pct_rechazo`` = rechazados / evaluados.
    - ``alerta``: ``pct_rechazo >= umbral`` (``cfg['quality_plata']['umbral_alerta_pct_rechazo']``).

    Devuelve un DataFrame listo para escribir. OJO: ``rechazados`` aquí es por regla
    (una fila con 2 causas cuenta en las 2 reglas); el conteo de FILAS en cuarentena
    (una por fila, no por causa) se calcula aparte en ``conteos_cuarentena``.
    """
    from pyspark.sql import functions as F

    col_causas = cfg["quarantine"]["columna_causas"]
    umbral = float(cfg["quality_plata"]["umbral_alerta_pct_rechazo"])
    reglas_descarte = [
        r["nombre"]
        for r in cfg["calidad"]["reglas"].values()
        if r["accion"] == "descartar_fila"
    ]

    base = df_compuertas.select("_batch_date", col_causas)
    # Evaluados por día.
    evaluados = base.groupBy("_batch_date").agg(F.count("*").alias("evaluados"))

    filas_regla = []
    for nombre in reglas_descarte:
        rech = (
            base.filter(F.array_contains(F.col(col_causas), F.lit(nombre)))
            .groupBy("_batch_date")
            .agg(F.count("*").alias("rechazados"))
            .withColumn("regla", F.lit(nombre))
        )
        filas_regla.append(rech)

    if filas_regla:
        rechazados = filas_regla[0]
        for extra in filas_regla[1:]:
            rechazados = rechazados.unionByName(extra)
    else:  # pragma: no cover - siempre hay reglas de descarte en el catálogo
        return evaluados

    # Producto día x regla para que toda regla tenga fila incluso con 0 rechazos.
    dias = evaluados.select("_batch_date")
    reglas_df = rechazados.select("regla").distinct()
    esqueleto = dias.crossJoin(reglas_df)

    metricas = (
        esqueleto.join(evaluados, on="_batch_date", how="left")
        .join(rechazados, on=["_batch_date", "regla"], how="left")
        .withColumn("rechazados", F.coalesce(F.col("rechazados"), F.lit(0)))
        .withColumn("pasados", F.col("evaluados") - F.col("rechazados"))
        .withColumn(
            "pct_rechazo",
            F.when(F.col("evaluados") > 0, F.col("rechazados") / F.col("evaluados")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn("alerta", F.col("pct_rechazo") >= F.lit(umbral))
        .withColumn("umbral_alerta", F.lit(umbral))
        .withColumn("_registrado", F.current_timestamp())
    )
    return metricas.select(
        "_batch_date",
        "regla",
        "evaluados",
        "pasados",
        "rechazados",
        "pct_rechazo",
        "alerta",
        "umbral_alerta",
        "_registrado",
    )


def _escribir_batch(batch_df: Any, batch_id: int, cfg: dict[str, Any]) -> None:
    """Escribe un micro-batch: compuertas -> plata + cuarentena, idempotente.

    Idempotencia exactly-once con ``txnAppId``/``txnVersion`` (ADR-004), un ``txnAppId``
    distinto por tabla destino para que sus versiones no colisionen.
    """
    # NO se usa cache() en serverless (ver materializar_batch).
    df_c = aplicar_compuertas(batch_df, cfg)
    pasan, rechazados = separar_pasan_rechazados(df_c, cfg)

    (
        construir_plata(pasan, cfg)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", "oceanwatch_silver_ais_clean")
        .option("txnVersion", batch_id)
        .saveAsTable(tabla_silver_fqn(cfg))
    )
    (
        construir_cuarentena(rechazados, cfg)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", "oceanwatch_quarantine_ais_rejected")
        .option("txnVersion", batch_id)
        .saveAsTable(tabla_cuarentena_fqn(cfg))
    )


def materializar_batch(spark: Any, cfg: dict[str, Any]):
    """Materializa la plata en modo BATCH (idempotente por overwrite).

    Lee bronce completo, deduplica (ADR-006), aplica compuertas, separa y SOBRESCRIBE
    (``overwrite``) plata, cuarentena y métricas. El modo batch simplifica la
    conciliación (un único recálculo determinista sobre todo el corpus) y es
    idempotente por reejecución. Devuelve un ``dict`` con los conteos reales.
    """
    from pyspark.sql import functions as F

    bronze = spark.table(tabla_bronze_fqn(cfg))
    dedup = deduplicar(bronze, cfg)
    # NOTA (serverless): NO se usa df.cache()/persist(): el cómputo serverless (Spark
    # Connect GC) no soporta cache() con el cliente actual (CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC).
    # Se escribe cada tabla por separado (Spark recomputa el plan) y los conteos se leen
    # de las tablas Delta ya materializadas, no del DataFrame intermedio.
    df_c = aplicar_compuertas(dedup, cfg)

    pasan, rechazados = separar_pasan_rechazados(df_c, cfg)
    plata = construir_plata(pasan, cfg)
    cuarentena = construir_cuarentena(rechazados, cfg)
    metricas = calcular_metricas(df_c, cfg)

    (
        plata.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_silver_fqn(cfg))
    )
    (
        cuarentena.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_cuarentena_fqn(cfg))
    )
    (
        metricas.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_metricas_fqn(cfg))
    )

    n_plata = spark.table(tabla_silver_fqn(cfg)).count()
    n_cuarentena = spark.table(tabla_cuarentena_fqn(cfg)).count()
    conteos = {
        "plata": n_plata,
        "cuarentena": n_cuarentena,
        # evaluados tras dedup = plata + cuarentena (toda fila deduplicada va a una u otra).
        "evaluados_dedup": n_plata + n_cuarentena,
    }
    return conteos


def materializar_stream(spark: Any, cfg: dict[str, Any]):
    """Materializa la plata en STREAMING (``foreachBatch`` + ``Trigger.availableNow``).

    Replica ``autoloader.escribir_bronze``: lee bronce como stream, deduplica con
    watermark (ADR-006), y en cada micro-batch aplica compuertas y escribe plata +
    cuarentena de forma idempotente. Las métricas se calculan al final en batch sobre
    las tablas ya escritas (más simple y determinista que agregarlas por micro-batch).

    Devuelve el ``StreamingQuery`` ya terminado (``availableNow`` consume y para).
    """
    checkpoint = _checkpoint_plata(cfg)
    stream = spark.readStream.table(tabla_bronze_fqn(cfg))
    dedup = deduplicar(stream, cfg)
    query = (
        dedup.writeStream.option("checkpointLocation", checkpoint)
        .foreachBatch(lambda bdf, bid: _escribir_batch(bdf, bid, cfg))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return query


def tabla_celda_en_agua_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla de referencia celda->en_agua (``oceanwatch.silver.celda_en_agua``).

    Tabla de referencia (Reference Data Enrichment, ADR-010) con una fila por celda de
    grilla distinta (``round(LAT,2)``, ``round(LON,2)`` — ADR-001) y su bandera
    ``en_agua`` (True=agua, False=tierra). Se puebla clasificando las celdas DISTINTAS
    (no las 25.9M filas) con ``global_land_mask`` FUERA de Spark y se usa para enriquecer
    la plata por join de celda. El nombre es parametrizable en ``cfg['silver']``.
    """
    return fqn(cfg, "silver", cfg["silver"].get("tabla_celda_en_agua", "celda_en_agua"))


def clasificar_celdas_en_agua(celdas: list[tuple[float, float]]) -> list[tuple[float, float, bool]]:
    """Clasifica celdas (lat, lon) como agua/tierra con ``global_land_mask`` (sin Spark).

    Vectorizado sobre arrays numpy (``globe.is_land``); ``en_agua = not is_land``. Puro,
    testeable sin Spark y SIN ``pandas_udf``. Es el núcleo del enfoque que SÍ funciona en
    serverless: se clasifican las CELDAS DISTINTAS de la grilla (cientos de miles), no las
    ~25.9M filas de la plata, evitando la fricción del ``pandas_udf`` de ``global_land_mask``
    sobre todo el corpus (ver nota en ``_columna_en_agua`` y ADR-010 §8).

    Args:
        celdas: lista de tuplas ``(celda_lat, celda_lon)`` (ya redondeadas a la grilla).

    Returns:
        lista de tuplas ``(celda_lat, celda_lon, en_agua)`` con ``en_agua`` booleano.

    Raises:
        MaskNoDisponibleError: si ``global_land_mask`` no está instalada (import perezoso).
    """
    import numpy as np

    from oceanwatch.quality.tierra_agua import MaskNoDisponibleError

    try:
        from global_land_mask import globe
    except ImportError as exc:  # pragma: no cover - depende del entorno
        raise MaskNoDisponibleError(
            "global_land_mask no está instalada. Ejecuta "
            "`%pip install global-land-mask==1.0.0` (serverless) o `pip install` (local)."
        ) from exc

    if not celdas:
        return []
    lat = np.array([float(c[0]) for c in celdas], dtype="float64")
    lon = np.array([float(c[1]) for c in celdas], dtype="float64")
    es_tierra = np.asarray(globe.is_land(lat, lon), dtype=bool)
    en_agua = ~es_tierra
    return [(float(lat[i]), float(lon[i]), bool(en_agua[i])) for i in range(len(celdas))]


def enriquecer_en_agua_por_celda(spark: Any, cfg: dict[str, Any]) -> dict[str, Any]:
    """Materializa ``en_agua`` en la plata por CELDA distinta (enfoque sin ``pandas_udf``).

    Enfoque robusto (ADR-010 §8, verificado en serverless 2026-11-05) que reemplaza al
    ``pandas_udf`` de ``global_land_mask`` (que falló sobre las 25.9M filas en Spark
    Connect):

    1. Obtiene las CELDAS DISTINTAS de la plata (``round(LAT,2)``, ``round(LON,2)`` —
       ADR-001, consistente con ``geo.celda_grilla``). Son cientos de miles, no millones.
    2. Las trae al driver (``toPandas``/``collect``; el conjunto es pequeño) y las
       clasifica con ``clasificar_celdas_en_agua`` (vectorizado numpy, SIN ``pandas_udf``).
    3. Escribe/reescribe la tabla de referencia ``silver.celda_en_agua``.
    4. Enriquece la plata con ``MERGE`` por join de celda (solo UPDATE de ``en_agua``).

    La bandera ``en_agua`` es NULIFICAR-ATRIBUTO (informativa, ADR-010): NO descarta filas,
    NO entra en ``causas`` ni en la invariante de conciliación; solo se POBLA. Idempotente.

    Returns:
        dict con ``ok`` (bool), ``celdas`` (nº de celdas distintas), ``en_agua`` /
        ``en_tierra`` / ``nulos`` (conteos de FILAS de la plata) si tuvo éxito, o ``error``.
    """
    from pyspark.sql import functions as F

    bandera = cfg["silver"]["banderas"]["en_agua"]
    tabla = tabla_silver_fqn(cfg)
    ref = tabla_celda_en_agua_fqn(cfg)
    dec = int(cfg["gold"]["decimales_grilla"])
    try:
        # (1) Celdas distintas de la plata (coords no nulas; en plata siempre lo son).
        celdas_df = (
            spark.table(tabla)
            .where(F.col("LAT").isNotNull() & F.col("LON").isNotNull())
            .select(
                F.round(F.col("LAT"), dec).alias("celda_lat"),
                F.round(F.col("LON"), dec).alias("celda_lon"),
            )
            .distinct()
        )
        celdas = [(r["celda_lat"], r["celda_lon"]) for r in celdas_df.collect()]

        # (2) Clasificación vectorizada SIN pandas_udf.
        clasificadas = clasificar_celdas_en_agua(celdas)

        # (3) Tabla de referencia celda -> en_agua.
        esquema = "celda_lat double, celda_lon double, en_agua boolean"
        ref_df = spark.createDataFrame(clasificadas, schema=esquema)
        (
            ref_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(ref)
        )

        # (4) Enriquecer la plata por join de celda (MERGE INTO, solo UPDATE de en_agua).
        spark.sql(
            f"""
            MERGE INTO {tabla} AS c
            USING {ref} AS r
            ON round(c.LAT, {dec}) = r.celda_lat AND round(c.LON, {dec}) = r.celda_lon
            WHEN MATCHED THEN UPDATE SET c.{bandera} = r.en_agua
            """
        )

        agg = spark.table(tabla).agg(
            F.sum(F.col(bandera).cast("int")).alias("en_agua"),
            F.sum((~F.col(bandera)).cast("int")).alias("en_tierra"),
            F.sum(F.col(bandera).isNull().cast("int")).alias("nulos"),
        ).collect()[0]
        return {
            "ok": True,
            "celdas": len(clasificadas),
            "en_agua": int(agg["en_agua"] or 0),
            "en_tierra": int(agg["en_tierra"] or 0),
            "nulos": int(agg["nulos"] or 0),
        }
    except Exception as exc:  # noqa: BLE001 - la bandera es informativa; no bloquea el flujo
        return {"ok": False, "error": repr(exc)}


def enriquecer_en_agua(spark: Any, cfg: dict[str, Any]) -> dict[str, Any]:
    """Enriquece la bandera ``en_agua`` de la plata con la máscara tierra/agua (ADR-010).

    .. deprecated::
        Camino basado en el ``pandas_udf`` de ``global_land_mask`` sobre las 25.9M filas.
        En el cómputo serverless (Spark Connect) ese ``pandas_udf`` FALLÓ y dejó ``en_agua``
        en NULL. El camino que SÍ materializa la bandera es
        ``enriquecer_en_agua_por_celda`` (clasifica las celdas DISTINTAS fuera de Spark y
        enriquece la plata por ``MERGE`` de celda). Se conserva esta función por
        trazabilidad del intento original; prefiera ``enriquecer_en_agua_por_celda``.

    Paso SEPARADO del núcleo de materialización: recalcula solo la columna booleana
    ``en_agua`` (informativa, NULIFICAR-ATRIBUTO) con el ``pandas_udf`` de
    ``global_land_mask`` y reescribe la tabla plata. Si la máscara falla (p. ej. límite
    del cómputo serverless con el UDF sobre 25.9M filas) NO corrompe la plata ya
    materializada: devuelve ``{'ok': False, 'error': ...}`` y deja ``en_agua`` como estaba
    (null). La invariante de conciliación NO depende de esta columna.

    Returns:
        dict con ``ok`` (bool), ``en_agua`` / ``en_tierra`` / ``nulos`` (conteos) si tuvo
        éxito, o ``error`` con el mensaje si falló.
    """
    from pyspark.sql import functions as F

    from oceanwatch.quality.compuertas import _columna_en_agua

    bandera = cfg["silver"]["banderas"]["en_agua"]
    tabla = tabla_silver_fqn(cfg)
    try:
        plata = spark.table(tabla)
        enriquecida = plata.withColumn(bandera, _columna_en_agua(cfg))
        (
            enriquecida.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabla)
        )
        agg = spark.table(tabla).agg(
            F.sum(F.col(bandera).cast("int")).alias("en_agua"),
            F.sum((~F.col(bandera)).cast("int")).alias("en_tierra"),
            F.sum(F.col(bandera).isNull().cast("int")).alias("nulos"),
        ).collect()[0]
        return {
            "ok": True,
            "en_agua": int(agg["en_agua"] or 0),
            "en_tierra": int(agg["en_tierra"] or 0),
            "nulos": int(agg["nulos"] or 0),
        }
    except Exception as exc:  # noqa: BLE001 - la bandera es informativa; no bloquea el flujo
        return {"ok": False, "error": repr(exc)}


def _checkpoint_plata(cfg: dict[str, Any]) -> str:
    """Ruta de checkpoint del stream de plata bajo el Volume de checkpoints."""
    from oceanwatch.common.config import catalog_name, schema

    catalogo = catalog_name(cfg)
    bronze = schema(cfg, "bronze")
    checkpoints = cfg["volumenes"]["checkpoints"]
    return f"/Volumes/{catalogo}/{bronze}/{checkpoints}/silver/ais_clean/_checkpoint"
