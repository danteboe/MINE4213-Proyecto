"""Compuertas de calidad de la capa plata (expresiones de columna, imports PySpark perezosos).

Aplica el catálogo de reglas de FEAT-001 (``reglas.py``, ``identificadores.py``) y la
regla tierra/agua de FEAT-002 (``tierra_agua.py``, ADR-010) sobre el DataFrame AIS
completo, construyendo:

1. **Atributos NULIFICADOS + banderas** (acción ``nulificar_atributo`` del catálogo):
   la fila PASA, pero el atributo no fiable se pone a ``null`` y se levanta una bandera
   booleana. Reglas nulificadoras (ADR-009/010):
   - ``Heading=511`` -> ``Heading=null`` + ``heading_desconocido=True``.
   - ``COG=360``      -> ``COG=null``     + ``cog_desconocido=True``.
   - ``SOG>=102.3``   -> ``SOG=null``     + ``sog_no_disponible=True``.
   - ``Length/Width/Draft<=0`` -> atributo ``null`` + bandera ``*_no_fisica=True``.
   - ``IMO`` con patrón inválido -> ``IMO=null`` + ``imo_invalido=True`` (vacío es VÁLIDO).
   - ``coordenada_en_agua`` -> bandera ``en_agua`` (True=agua, False=tierra); NO descarta.

2. **Columna ``causas`` (``array<string>``)** con los NOMBRES de las reglas de
   **DESCARTE** violadas (acción ``descartar_fila``). Una fila con ``size(causas)=0``
   pasa a plata; con ``size(causas)>0`` va a cuarentena (ADR-011). Reglas de descarte:
   - ``coordenadas_en_rango``        : LAT/LON fuera de [-90,90]/[-180,180] o nulas.
   - ``coordenada_no_centinela``     : LAT=91 o LON=181 (centinela AIS "no disponible").
   - ``mmsi_bien_formado``           : MMSI que no tiene exactamente 9 dígitos.
   - ``mmsi_mid_valido``             : MID (3 primeros dígitos) fuera del rango ITU [201,775].
   - ``timestamp_valido_no_futuro``  : ``BaseDateTime`` nulo o futuro.

La clasificación descartar/nulificar es la de ADR-009/010 (parametrizada en
``cfg['calidad']['reglas']``). Se usan EXPRESIONES NATIVAS de Spark (``pyspark.sql.functions``),
no UDFs, por rendimiento sobre las 25.9M filas: la lógica escalar de FEAT-001 se
reproduce fielmente como columnas (mismos umbrales leídos de ``cfg['calidad']``).

Todos los imports de PySpark son perezosos: el módulo se importa sin Spark.
"""

from __future__ import annotations

from typing import Any


# --------------------------------------------------------------------------- #
# Predicados de columna (válido = True). Reproducen las funciones puras de
# FEAT-001 con expresiones nativas. El import de pyspark.sql.functions es perezoso.
# --------------------------------------------------------------------------- #
def _coordenadas_en_rango(cfg: dict[str, Any]):
    """Column bool: LAT∈[lat_min,lat_max] y LON∈[lon_min,lon_max] y no nulas."""
    from pyspark.sql import functions as F

    c = cfg["calidad"]["coordenadas"]
    lat, lon = F.col("LAT"), F.col("LON")
    return (
        lat.isNotNull()
        & lon.isNotNull()
        & (lat >= F.lit(c["lat_min"]))
        & (lat <= F.lit(c["lat_max"]))
        & (lon >= F.lit(c["lon_min"]))
        & (lon <= F.lit(c["lon_max"]))
    )


def _coordenada_es_centinela(cfg: dict[str, Any]):
    """Column bool: LAT=lat_centinela (91) o LON=lon_centinela (181)."""
    from pyspark.sql import functions as F

    c = cfg["calidad"]["coordenadas"]
    return (F.col("LAT") == F.lit(c["lat_centinela"])) | (
        F.col("LON") == F.lit(c["lon_centinela"])
    )


def _mmsi_texto():
    """Column: MMSI como texto de solo dígitos (null si no es dígitos puros).

    Reproduce ``identificadores._normalizar_mmsi``: el MMSI de bronce es ``long``,
    así que ``cast('string')`` da un texto de dígitos sin punto decimal.
    """
    from pyspark.sql import functions as F

    texto = F.col("MMSI").cast("string")
    return F.when(texto.rlike(r"^\d+$"), texto).otherwise(F.lit(None))


def _mmsi_bien_formado(cfg: dict[str, Any]):
    """Column bool: MMSI tiene exactamente ``digitos`` (9) dígitos."""
    from pyspark.sql import functions as F

    digitos = int(cfg["calidad"]["mmsi"]["digitos"])
    texto = _mmsi_texto()
    return texto.isNotNull() & (F.length(texto) == F.lit(digitos))


def _mmsi_mid_valido(cfg: dict[str, Any]):
    """Column bool: MID (3 primeros dígitos) en [mid_min,mid_max]=[201,775] (ITU).

    Exige MMSI bien formado (9 dígitos) primero, igual que la función pura.
    """
    from pyspark.sql import functions as F

    m = cfg["calidad"]["mmsi"]
    mid_min, mid_max = int(m["mid_min"]), int(m["mid_max"])
    texto = _mmsi_texto()
    mid = F.substring(texto, 1, 3).cast("int")
    return (
        texto.isNotNull()
        & (F.length(texto) == F.lit(9))
        & (mid >= F.lit(mid_min))
        & (mid <= F.lit(mid_max))
    )


def _timestamp_valido_no_futuro():
    """Column bool: ``BaseDateTime`` no nulo y no futuro (<= ``current_timestamp``).

    El serverless usa UTC por defecto (ADR-004); ``current_timestamp`` es el ahora UTC.
    """
    from pyspark.sql import functions as F

    ts = F.col("BaseDateTime")
    return ts.isNotNull() & (ts <= F.current_timestamp())


def _imo_valido(cfg: dict[str, Any]):
    """Column bool: IMO válido (vacío/null VÁLIDO; si viene, check digit correcto).

    Reproduce ``identificadores.imo_valido``:
    - null o cadena en blanco => True (VÁLIDO; ~42.5% del corpus no trae IMO).
    - 'IMO'+7 dígitos o 7 dígitos => True si el 7.º dígito es el verificador
      (suma de los 6 primeros por pesos 7,6,5,4,3,2; unidad de la suma).
    - cualquier otra forma => False (nulificar).
    """
    from pyspark.sql import functions as F

    bruto = F.col("IMO").cast("string")
    texto = F.trim(bruto)
    vacio = bruto.isNull() | (texto == F.lit(""))

    # Extrae los 7 dígitos de 'IMO#######' o de 7 dígitos puros; null si no encaja.
    siete = F.when(texto.rlike(r"(?i)^IMO\d{7}$"), F.regexp_extract(texto, r"(?i)^IMO(\d{7})$", 1))
    siete = siete.when(texto.rlike(r"^\d{7}$"), texto).otherwise(F.lit(None))

    pesos = (7, 6, 5, 4, 3, 2)
    suma = None
    for i, peso in enumerate(pesos):
        termino = F.substring(siete, i + 1, 1).cast("int") * F.lit(peso)
        suma = termino if suma is None else (suma + termino)
    verificador = suma % F.lit(10)
    septimo = F.substring(siete, 7, 1).cast("int")
    check_ok = siete.isNotNull() & (septimo == verificador)
    return vacio | check_ok


# --------------------------------------------------------------------------- #
# Compuerta principal
# --------------------------------------------------------------------------- #
def aplicar_compuertas(df: Any, cfg: dict[str, Any], incluir_en_agua: bool = False):
    """Aplica nulificación + banderas y construye la columna ``causas`` de descarte.

    Args:
        df: DataFrame de bronce (17 columnas AIS + auditoría), ya deduplicado.
        cfg: configuración (``cfg['calidad']`` con umbrales/acciones; ``cfg['silver']['banderas']``).
        incluir_en_agua: si ``True`` calcula la bandera ``en_agua`` con la máscara
            tierra/agua (``pandas_udf`` de ``global_land_mask``); si ``False`` (por
            defecto) la deja como columna booleana ``null``. La bandera ``en_agua`` es
            NULIFICAR-ATRIBUTO (informativa, ADR-010): NO entra en ``causas`` ni en la
            invariante de conciliación, así que el núcleo de compuertas (reglas de
            descarte + nulificación numérica, TODO expresiones nativas) se materializa
            sin depender de la máscara. En serverless la enriquecemos en un paso aparte
            para que un fallo del ``pandas_udf`` nunca bloquee silver/cuarentena.

    Returns:
        El DataFrame enriquecido con:
        - atributos nulificados (Heading/COG/SOG/Length/Width/Draft/IMO) según reglas,
        - banderas booleanas (nombres de ``cfg['silver']['banderas']``),
        - ``en_agua`` (True=agua, False=tierra, null=coord nula) via UDF de ``tierra_agua``,
        - ``causas`` ``array<string>`` con los nombres de las reglas de descarte violadas
          (vacía => la fila pasa a plata).

    Nota de rendimiento: todas las reglas de descarte y de nulificación numérica se
    evalúan con expresiones NATIVAS. La única parte que no es expresable de forma nativa
    (la máscara ráster ``global_land_mask``) se calcula con una UDF vectorizada mínima,
    porque su acción es NULIFICAR (bandera ``en_agua``) y NO entra en ``causas`` ni en la
    invariante de conciliación (ADR-010).
    """
    from pyspark.sql import functions as F

    banderas = cfg["silver"]["banderas"]
    reglas = cfg["calidad"]["reglas"]
    sog_centinela = cfg["calidad"]["sog"]["centinela"]
    heading_centinela = cfg["calidad"]["heading"]["centinela"]
    cog_centinela = cfg["calidad"]["cog"]["centinela"]
    dim_min = cfg["calidad"]["dimensiones"]["min_exclusivo"]

    out = df

    # --- (1) NULIFICAR-ATRIBUTO: bandera + atributo a null (la fila PASA) -------
    # SOG>=102.3 (centinela "no disponible").
    sog_centinela_col = F.col("SOG").isNotNull() & (F.col("SOG") >= F.lit(sog_centinela))
    out = out.withColumn(banderas["sog"], F.coalesce(sog_centinela_col, F.lit(False)))
    out = out.withColumn("SOG", F.when(sog_centinela_col, F.lit(None)).otherwise(F.col("SOG")))

    # Heading=511 (centinela).
    heading_centinela_col = F.col("Heading") == F.lit(heading_centinela)
    out = out.withColumn(banderas["heading"], F.coalesce(heading_centinela_col, F.lit(False)))
    out = out.withColumn(
        "Heading", F.when(heading_centinela_col, F.lit(None)).otherwise(F.col("Heading"))
    )

    # COG=360 (centinela).
    cog_centinela_col = F.col("COG") == F.lit(cog_centinela)
    out = out.withColumn(banderas["cog"], F.coalesce(cog_centinela_col, F.lit(False)))
    out = out.withColumn("COG", F.when(cog_centinela_col, F.lit(None)).otherwise(F.col("COG")))

    # Length/Width/Draft <= 0 (dimensiones estáticas no físicas).
    for col_dim, clave_bandera in (("Length", "length"), ("Width", "width"), ("Draft", "draft")):
        no_fisica = F.col(col_dim).isNotNull() & (F.col(col_dim) <= F.lit(dim_min))
        out = out.withColumn(banderas[clave_bandera], F.coalesce(no_fisica, F.lit(False)))
        out = out.withColumn(col_dim, F.when(no_fisica, F.lit(None)).otherwise(F.col(col_dim)))

    # IMO con patrón inválido (vacío es VÁLIDO) -> null + bandera.
    imo_ok = _imo_valido(cfg)
    out = out.withColumn(banderas["imo"], ~imo_ok)
    out = out.withColumn("IMO", F.when(imo_ok, F.col("IMO")).otherwise(F.lit(None)))

    # coordenada_en_agua (ADR-010): bandera en_agua (True=agua, False=tierra). NO descarta.
    # Por defecto se deja null (columna booleana) para no acoplar el núcleo de compuertas
    # a la máscara; se enriquece en un paso aparte (enriquecer_en_agua) en serverless.
    if incluir_en_agua:
        out = out.withColumn(banderas["en_agua"], _columna_en_agua(cfg))
    else:
        out = out.withColumn(banderas["en_agua"], F.lit(None).cast("boolean"))

    # --- (2) DESCARTAR-FILA: causas array<string> con los nombres de reglas violadas
    def _causa(cond_violada, nombre_regla: str):
        """Devuelve el nombre de la regla si la condición de violación es True, si no null."""
        return F.when(cond_violada, F.lit(reglas[nombre_regla]["nombre"])).otherwise(F.lit(None))

    coord_en_rango = _coordenadas_en_rango(cfg)
    coord_centinela = _coordenada_es_centinela(cfg)
    mmsi_ok = _mmsi_bien_formado(cfg)
    mid_ok = _mmsi_mid_valido(cfg)
    ts_ok = _timestamp_valido_no_futuro()

    causas = F.array_except(
        F.array(
            _causa(~F.coalesce(coord_en_rango, F.lit(False)), "coordenadas_en_rango"),
            _causa(F.coalesce(coord_centinela, F.lit(False)), "coordenada_no_centinela"),
            _causa(~F.coalesce(mmsi_ok, F.lit(False)), "mmsi_bien_formado"),
            _causa(~F.coalesce(mid_ok, F.lit(False)), "mmsi_mid_valido"),
            _causa(~F.coalesce(ts_ok, F.lit(False)), "timestamp_valido_no_futuro"),
        ),
        F.array(F.lit(None).cast("string")),
    )
    out = out.withColumn(cfg["quarantine"]["columna_causas"], causas)
    return out


def _columna_en_agua(cfg: dict[str, Any]):
    """Column bool (True=agua): máscara tierra/agua VECTORIZADA (``pandas_udf``).

    ``global_land_mask`` no es expresable de forma nativa en Spark, pero su API
    ``globe.is_land(lat_array, lon_array)`` es VECTORIZADA sobre arrays de numpy (la
    medición de FEAT-002/ADR-010 clasificó 60.000 puntos en 0.006 s). Por eso se usa
    un ``pandas_udf`` (una llamada por partición sobre arrays) y NO una UDF escalar
    fila-a-fila: sobre 25.9M filas la UDF escalar es inviable en serverless (provocó un
    error interno). El import de ``global_land_mask`` es perezoso; el notebook serverless
    la instala con ``%pip install global-land-mask==1.0.0``. Como la regla es NULIFICAR
    (bandera informativa, no descarte) nunca entra en ``causas`` ni en la invariante.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType

    from oceanwatch.quality.tierra_agua import GLOBAL_LAND_MASK_VERSION  # noqa: F401

    @F.pandas_udf(BooleanType())
    def _en_agua_pudf(lat_s, lon_s):  # pragma: no cover - requiere pandas/numpy/lib en serverless
        import numpy as np
        import pandas as pd
        from global_land_mask import globe

        lat = pd.to_numeric(lat_s, errors="coerce")
        lon = pd.to_numeric(lon_s, errors="coerce")
        valido = lat.notna() & lon.notna()
        try:
            # globe.is_land exige valores finitos; se evalúa con coords saneadas.
            lat_f = lat.fillna(0.0).to_numpy()
            lon_f = lon.fillna(0.0).to_numpy()
            es_tierra = globe.is_land(lat_f, lon_f)  # vectorizado sobre numpy
            en_agua = ~np.asarray(es_tierra, dtype=bool)
            serie = pd.Series(en_agua, index=lat_s.index)
            return serie.where(valido, other=pd.NA).astype("boolean")
        except Exception:  # noqa: BLE001 - la bandera es informativa; nunca bloquea el flujo
            return pd.Series(pd.NA, index=lat_s.index, dtype="boolean")

    return _en_agua_pudf(F.col("LAT"), F.col("LON"))
