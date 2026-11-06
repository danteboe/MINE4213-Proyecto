"""Invariante de conciliación bronce -> plata + cuarentena + duplicados (aritmética PURA).

La materialización de la capa plata debe conservar cada fila de bronce: una fila o
pasa a plata, o va a cuarentena, o se elimina como duplicado exacto. El invariante
(ADR-006, ADR-011) es::

    registros_bronce = registros_plata + registros_cuarentena + duplicados_descartados

donde ``duplicados_descartados`` son los que elimina ``dropDuplicatesWithinWatermark``
sobre la clave natural ``['MMSI','BaseDateTime']`` (658 en el corpus de 3 días). Una
fila puede violar VARIAS reglas de descarte, pero cuenta UNA sola vez en cuarentena
(su columna ``causas`` es un ``array<string>``), por eso el conteo de cuarentena es por
FILA, no por causa.

Este módulo es aritmética pura y testeable sin Spark. La lectura de los conteos reales
(``count(*)`` de cada tabla) la hace el notebook con Spark y se los pasa a
``verificar_invariante``.
"""

from __future__ import annotations

from typing import Any


def verificar_invariante(
    bronce: int, plata: int, cuarentena: int, duplicados: int
) -> dict[str, Any]:
    """Comprueba el invariante de conciliación con conteos ya leídos.

    Args:
        bronce: filas totales en ``oceanwatch.bronze.ais_raw`` (p. ej. 25.897.493).
        plata: filas en ``oceanwatch.silver.ais_clean`` (pasan todas las compuertas).
        cuarentena: FILAS en ``oceanwatch.quarantine.ais_rejected`` (una por fila rechazada,
            aunque tenga varias causas).
        duplicados: duplicados exactos por clave natural eliminados por la dedup (658).

    Returns:
        ``dict`` con:
        - ``bronce``, ``plata``, ``cuarentena``, ``duplicados``: los conteos de entrada.
        - ``suma``: ``plata + cuarentena + duplicados``.
        - ``diferencia``: ``bronce - suma`` (0 si cuadra).
        - ``cuadra``: ``bool``, ``True`` si ``bronce == plata + cuarentena + duplicados``.
        - ``descripcion``: cadena legible con la aritmética exacta.
    """
    for nombre, valor in (
        ("bronce", bronce),
        ("plata", plata),
        ("cuarentena", cuarentena),
        ("duplicados", duplicados),
    ):
        if not isinstance(valor, int) or isinstance(valor, bool):
            raise TypeError(f"El conteo '{nombre}' debe ser int, se recibió {type(valor).__name__}.")
        if valor < 0:
            raise ValueError(f"El conteo '{nombre}' no puede ser negativo: {valor}.")

    suma = plata + cuarentena + duplicados
    diferencia = bronce - suma
    cuadra = diferencia == 0
    descripcion = (
        f"{bronce:,} (bronce) "
        f"{'=' if cuadra else '!='} "
        f"{plata:,} (plata) + {cuarentena:,} (cuarentena) + {duplicados:,} (duplicados) "
        f"= {suma:,}"
        + ("" if cuadra else f"; diferencia = {diferencia:,}")
    )
    return {
        "bronce": bronce,
        "plata": plata,
        "cuarentena": cuarentena,
        "duplicados": duplicados,
        "suma": suma,
        "diferencia": diferencia,
        "cuadra": cuadra,
        "descripcion": descripcion,
    }
