"""Reglas de calidad de la capa plata como funciones PURAS (sin PySpark).

Cada función implementa UNA regla del catálogo de calidad (Req. 2, ADR-009) sobre
valores escalares de Python, de modo que sea testeable con ``pytest`` sin una
sesión Spark activa. Los umbrales NO están hardcodeados: se leen de
``cfg['calidad']`` (``conf/config.yaml``) y están justificados por el diagnóstico
real de 25.897.493 filas (``evidence/req02-capas-calidad/diagnostico_calidad_bronce.md``).

Convención de retorno:
- Las reglas de validez devuelven ``bool`` (``True`` = válido / pasa la regla).
- Las reglas de centinela devuelven ``bool`` (``True`` = el valor ES centinela
  "no disponible", por tanto el atributo debe nulificarse).

Clasificación descartar-fila vs nulificar-atributo: ver ADR-009 y
``docs/data_quality_rules.md``. Aquí las funciones solo deciden validez; la
materialización (cuarentena vs null + bandera) la aplica FEAT-003.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

Numero = Optional[float]


def _a_float(valor: Any) -> Optional[float]:
    """Convierte ``valor`` a ``float`` o devuelve ``None`` si no es numérico."""
    if valor is None:
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def coordenadas_en_rango(lat: Numero, lon: Numero, cfg: dict[str, Any]) -> bool:
    """¿La coordenada cae dentro del rango físico terrestre?

    LAT∈[lat_min,lat_max]=[-90,90] y LON∈[lon_min,lon_max]=[-180,180]. En el
    corpus real 0 filas violan el rango (diagnóstico), pero la regla existe por
    robustez ante datos AIS futuros. Devuelve ``False`` ante ``None`` o no
    numérico (acción: descartar_fila).
    """
    coords = cfg["calidad"]["coordenadas"]
    flat = _a_float(lat)
    flon = _a_float(lon)
    if flat is None or flon is None:
        return False
    return (
        coords["lat_min"] <= flat <= coords["lat_max"]
        and coords["lon_min"] <= flon <= coords["lon_max"]
    )


def es_centinela_coordenada(lat: Numero, lon: Numero, cfg: dict[str, Any]) -> bool:
    """¿La coordenada usa el centinela AIS "no disponible" (LAT=91 / LON=181)?

    Devuelve ``True`` si LAT=91 o LON=181 (0 casos en el corpus, regla por
    robustez). Estas filas se tratan como coordenada ausente => descartar_fila.
    """
    coords = cfg["calidad"]["coordenadas"]
    flat = _a_float(lat)
    flon = _a_float(lon)
    return flat == coords["lat_centinela"] or flon == coords["lon_centinela"]


def coordenada_no_centinela(lat: Numero, lon: Numero, cfg: dict[str, Any]) -> bool:
    """Regla de validez de coordenada: en rango Y sin centinela.

    Combina ``coordenadas_en_rango`` y la negación de ``es_centinela_coordenada``.
    """
    return coordenadas_en_rango(lat, lon, cfg) and not es_centinela_coordenada(
        lat, lon, cfg
    )


def sog_es_centinela(sog: Numero, cfg: dict[str, Any]) -> bool:
    """¿La velocidad sobre el fondo es el centinela AIS (SOG>=102.3)?

    El centinela 102.3 = "no disponible". En el corpus 65.510 filas lo usan
    (diagnóstico). Devuelve ``True`` si SOG>=centinela => nulificar atributo.
    """
    umbral = cfg["calidad"]["sog"]["centinela"]
    f = _a_float(sog)
    if f is None:
        return False
    return f >= umbral


def sog_plausible(sog: Numero, cfg: dict[str, Any]) -> bool:
    """¿La velocidad sobre el fondo es físicamente plausible?

    Válido si 0<=SOG<=max_fisico (80 kn). El máximo físico se fija muy por encima
    de P99.9=32.5 kn (deja pasar buques rápidos reales) y por debajo del centinela
    102.3 kn, que es el corte inequívoco. SOG centinela (>=102.3) NO es plausible.
    ``None`` se considera no plausible (acción: nulificar_atributo).
    """
    sog_cfg = cfg["calidad"]["sog"]
    f = _a_float(sog)
    if f is None:
        return False
    return 0.0 <= f <= sog_cfg["max_fisico"]


def heading_es_centinela(h: Numero, cfg: dict[str, Any]) -> bool:
    """¿El rumbo de proa es el centinela AIS 511 ("no disponible")?

    En el corpus 14.367.009 filas (~55.5%) usan 511 (diagnóstico). Devuelve
    ``True`` si Heading=511 => nulificar atributo.
    """
    centinela = cfg["calidad"]["heading"]["centinela"]
    f = _a_float(h)
    if f is None:
        return False
    return f == centinela


def heading_valido(h: Numero, cfg: dict[str, Any]) -> bool:
    """¿El rumbo de proa es un ángulo válido en [0,359]?

    Válido si min<=Heading<=max=[0,359]. El centinela 511 NO es válido. ``None``
    se considera no válido (acción: nulificar_atributo).
    """
    hd = cfg["calidad"]["heading"]
    f = _a_float(h)
    if f is None:
        return False
    return hd["min"] <= f <= hd["max"]


def cog_es_centinela(c: Numero, cfg: dict[str, Any]) -> bool:
    """¿El rumbo sobre el fondo es el centinela AIS 360 ("no disponible")?

    En el corpus 4.376.798 filas (~16.9%) usan 360 (diagnóstico). Devuelve
    ``True`` si COG=360 => nulificar atributo.
    """
    centinela = cfg["calidad"]["cog"]["centinela"]
    f = _a_float(c)
    if f is None:
        return False
    return f == centinela


def cog_valido(c: Numero, cfg: dict[str, Any]) -> bool:
    """¿El rumbo sobre el fondo es un ángulo válido en [0,359]?

    Válido si min<=COG<=max=[0,359]. El centinela 360 NO es válido. ``None`` se
    considera no válido (acción: nulificar_atributo).
    """
    cog = cfg["calidad"]["cog"]
    f = _a_float(c)
    if f is None:
        return False
    return cog["min"] <= f <= cog["max"]


def timestamp_valido_no_futuro(ts: Any, ahora: datetime) -> bool:
    """¿El timestamp está presente y no es futuro respecto a ``ahora``?

    Válido si ``ts`` es un ``datetime`` no nulo y ``ts <= ahora``. En el corpus 0
    futuros/nulos (diagnóstico); la regla existe por robustez. Se compara en UTC:
    el serverless usa UTC por defecto (ADR-004). Acción: descartar_fila.
    """
    if not isinstance(ts, datetime):
        return False
    # Normaliza a comparación naive/aware homogénea evitando errores de tzinfo.
    if ts.tzinfo is not None and ahora.tzinfo is None:
        ahora = ahora.replace(tzinfo=ts.tzinfo)
    elif ts.tzinfo is None and ahora.tzinfo is not None:
        ts = ts.replace(tzinfo=ahora.tzinfo)
    return ts <= ahora


def dimension_fisica(valor: Numero) -> bool:
    """¿La dimensión estática del buque (Length/Width/Draft) es física (>0)?

    Válido si ``valor > 0``. En el corpus Length<=0=905.785, Width<=0=967.816,
    Draft<=0=1.180.265 (diagnóstico). ``None`` o <=0 no es física (acción:
    nulificar_atributo; son atributos estáticos, no de posición).
    """
    f = _a_float(valor)
    if f is None:
        return False
    return f > 0.0
