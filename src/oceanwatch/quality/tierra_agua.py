"""Regla de calidad 'coordenada EN EL AGUA' — máscara tierra/agua (Req. 2, ADR-010).

Clasifica una coordenada AIS como *agua* o *tierra* y expone el resultado como una
bandera ``en_agua``. La medición empírica sobre una muestra real (ADR-010,
evidence/req02-capas-calidad/mascara_tierra_agua.md) mostró que la máscara ráster
global ~1/120° marca como "tierra" una fracción grande del tráfico AIS costero y de
aguas interiores navegables (canales de puerto, ríos), que es tráfico VÁLIDO. Por eso
la regla es NULIFICAR-ATRIBUTO (marca la bandera, NO descarta la fila): un reporte en
tierra puede ser error de posición, pero también un buque en un canal interior que la
máscara no distingue. El consumidor decide aguas abajo.

Patrón aplicado: **Reference Data Enrichment** (validación geoespacial contra un
dato de referencia de la geografía tierra/agua).

Disponibilidad real del entorno (evidence/req02-capas-calidad/disponibilidad_librerias_serverless.md):
ninguna librería geoespacial (``global-land-mask``, ``geopandas``, ``pyshp``) viene
preinstalada en el cómputo *serverless*. Por eso este módulo ofrece DOS caminos y
está escrito para **importarse sin ninguna librería geoespacial instalada**:

1. ``en_agua_bbox(lat, lon, cfg)`` — *baseline* puro, determinista y sin dependencias:
   descarta coordenadas fuera de cajas delimitadoras (bounding boxes) de masas de agua
   groseras. Sirve de salvaguarda cuando ``%pip install`` no es viable.
2. ``en_agua(lat, lon, cfg)`` — método PRINCIPAL (ADR-010): envoltorio con import
   PEREZOSO de ``global_land_mask`` (máscara ráster global ~1/120°). Si la librería no
   está instalada lanza ``MaskNoDisponibleError`` con instrucciones de ``%pip install``;
   nunca falla en tiempo de import del módulo.

Todas las funciones son puras (sin PySpark, sin estado global mutable observable).
"""

from __future__ import annotations

from typing import Any, Optional

# Versión fijada por reproducibilidad (ADR-010). Debe coincidir con el %pip install
# del notebook de materialización (FEAT-003) y con conf['calidad']['coordenada_en_agua'].
GLOBAL_LAND_MASK_VERSION = "1.0.0"

# Cajas delimitadoras (bounding boxes) de grandes masas de agua, en grados decimales:
# (lat_min, lat_max, lon_min, lon_max). Es un recurso ESTÁTICO, grosero y sin
# dependencias, usado SOLO por el baseline en_agua_bbox como salvaguarda. No pretende
# resolución costera: define regiones donde sí hay océano abierto para el corpus AIS
# costero de EE. UU. y, de forma conservadora, los grandes océanos del mundo para ser
# resistente a datos AIS nuevos (el feed podría traer otras regiones).
_BBOX_AGUA: tuple[tuple[float, float, float, float], ...] = (
    # Océanos globales por longitud (franja latitudinal navegable, excluye polos sólidos).
    (-80.0, 80.0, -180.0, 180.0),
)


class MaskNoDisponibleError(RuntimeError):
    """La librería de máscara tierra/agua no está instalada en el entorno.

    Se eleva desde ``en_agua`` (método principal) cuando ``global_land_mask`` no
    puede importarse. El mensaje indica el ``%pip install`` pinneado a usar en el
    notebook serverless (ver ADR-010). El baseline ``en_agua_bbox`` NO depende de
    esta librería y sigue disponible.
    """


def _a_float(valor: Any) -> Optional[float]:
    """Convierte a ``float`` o devuelve ``None`` si no es convertible (coord. nula)."""
    if valor is None:
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def _bboxes_agua(cfg: Optional[dict[str, Any]]) -> tuple[tuple[float, float, float, float], ...]:
    """Lee las cajas de agua del baseline desde ``cfg`` o usa ``_BBOX_AGUA``.

    Formato en conf: ``cfg['calidad']['coordenada_en_agua']['bbox_agua']`` como lista
    de ``[lat_min, lat_max, lon_min, lon_max]``. Parametrizable sin tocar el código.
    """
    if cfg is not None:
        regla = cfg.get("calidad", {}).get("coordenada_en_agua", {})
        cajas = regla.get("bbox_agua")
        if cajas:
            return tuple(
                (float(c[0]), float(c[1]), float(c[2]), float(c[3])) for c in cajas
            )
    return _BBOX_AGUA


def en_agua_bbox(lat: Any, lon: Any, cfg: Optional[dict[str, Any]] = None) -> Optional[bool]:
    """Baseline SIN dependencias: ¿la coordenada cae en alguna caja de agua?

    Determinista y puro. Devuelve:
    - ``True``  si ``(lat, lon)`` cae dentro de alguna caja de ``_bboxes_agua``.
    - ``False`` si cae fuera de todas (candidato a 'tierra'/fuera de agua navegable).
    - ``None``  si la coordenada es nula/no convertible (no se puede clasificar).

    NO distingue costa con precisión: es una salvaguarda grosera para cuando el
    método principal (``global_land_mask``) no esté disponible. Su tasa de falsos
    negativos (marcar como agua algo que es tierra) es ALTA por construcción; por
    eso ADR-010 lo deja como respaldo, no como método primario.
    """
    flat = _a_float(lat)
    flon = _a_float(lon)
    if flat is None or flon is None:
        return None
    for lat_min, lat_max, lon_min, lon_max in _bboxes_agua(cfg):
        if lat_min <= flat <= lat_max and lon_min <= flon <= lon_max:
            return True
    return False


def en_agua(lat: Any, lon: Any, cfg: Optional[dict[str, Any]] = None) -> Optional[bool]:
    """Método PRINCIPAL (ADR-010): máscara tierra/agua vía ``global_land_mask``.

    Import PEREZOSO de ``global_land_mask`` para que este módulo se importe sin la
    librería instalada (el entorno serverless no la trae de fábrica). Devuelve:
    - ``True``  si la coordenada está en agua.
    - ``False`` si está en tierra.
    - ``None``  si la coordenada es nula/no convertible.

    Raises:
        MaskNoDisponibleError: si ``global_land_mask`` no está instalada. En el
            notebook serverless debe precederse de::

                %pip install global-land-mask==1.0.0

    ``global_land_mask`` usa una máscara ráster global de ~1/120° (~0.9 km) derivada
    de datos GSHHG; ``is_land(lat, lon)`` es ``True`` sobre tierra. La resolución y la
    exactitud medidas sobre una muestra real se documentan en
    evidence/req02-capas-calidad/mascara_tierra_agua.md.
    """
    flat = _a_float(lat)
    flon = _a_float(lon)
    if flat is None or flon is None:
        return None
    try:
        from global_land_mask import globe  # import perezoso (no requerido para importar el módulo)
    except ImportError as exc:  # pragma: no cover - depende del entorno
        raise MaskNoDisponibleError(
            "global_land_mask no está instalada. En el notebook serverless ejecuta "
            f"`%pip install global-land-mask=={GLOBAL_LAND_MASK_VERSION}` antes de usar "
            "en_agua(); o usa el baseline en_agua_bbox() sin dependencias."
        ) from exc
    return not bool(globe.is_land(flat, flon))
