"""Reglas de calidad sobre identificadores de buque (funciones PURAS, sin PySpark).

Valida MMSI (9 dígitos + MID de país), IMO (7 dígitos con dígito verificador),
indicativo de llamada (CallSign) y nombre de buque (VesselName). Todos los
umbrales se leen de ``cfg['calidad']`` y están justificados por el diagnóstico
real (``evidence/req02-capas-calidad/diagnostico_calidad_bronce.md``): MMSI no de
9 dígitos=21.960; IMO vacío=11.015.974 (la regla ACEPTA vacío).

Fuentes oficiales citadas en los docstrings y en ADR-009.
"""

from __future__ import annotations

import re
from typing import Any, Optional

_SOLO_DIGITOS = re.compile(r"^\d+$")
# Forma AIS observada del IMO: prefijo textual 'IMO' seguido de 7 dígitos.
_PREFIJO_IMO = re.compile(r"^IMO(\d{7})$", re.IGNORECASE)
# Charset razonable para CallSign: alfanumérico mayúsculas (ITU usa A-Z/0-9).
_CALLSIGN_OK = re.compile(r"^[A-Z0-9]{3,10}$")


def _normalizar_mmsi(mmsi: Any) -> Optional[str]:
    """Devuelve el MMSI como cadena de dígitos, o ``None`` si no es representable.

    Acepta ``int`` o ``str``; descarta ``None``, flotantes no enteros y cadenas
    con caracteres no numéricos.
    """
    if mmsi is None:
        return None
    if isinstance(mmsi, bool):  # bool es subtipo de int; excluir explícitamente.
        return None
    if isinstance(mmsi, int):
        return str(mmsi)
    if isinstance(mmsi, float):
        return str(int(mmsi)) if mmsi.is_integer() else None
    texto = str(mmsi).strip()
    return texto if _SOLO_DIGITOS.match(texto) else None


def mmsi_bien_formado(mmsi: Any, cfg: dict[str, Any]) -> bool:
    """¿El MMSI tiene exactamente el número de dígitos exigido (9)?

    El MMSI (Maritime Mobile Service Identity) es un identificador de 9 dígitos
    (ITU-R M.585). En el corpus 21.960 filas (~0.085%) no cumplen => descartar_fila.
    """
    digitos = int(cfg["calidad"]["mmsi"]["digitos"])
    texto = _normalizar_mmsi(mmsi)
    return texto is not None and len(texto) == digitos


def mmsi_mid_valido(mmsi: Any, cfg: Optional[dict[str, Any]] = None) -> bool:
    """¿El MID (3 primeros dígitos del MMSI) corresponde a un país marítimo?

    El MID (Maritime Identification Digits) son los 3 primeros dígitos del MMSI de
    una estación de barco y codifican el país de registro. Según la asignación de
    la ITU (Tabla de MID, apéndice de la Rec. ITU-R M.585), los MID válidos caen en
    el rango [201, 775]. Fuente oficial: ITU "Table of Maritime Identification
    Digits" (https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx).

    El MMSI debe primero estar bien formado (9 dígitos). Rango parametrizado en
    ``cfg['calidad']['mmsi']`` (por defecto [201,775]).
    """
    mid_min, mid_max = 201, 775
    if cfg is not None:
        mmsi_cfg = cfg["calidad"]["mmsi"]
        mid_min = int(mmsi_cfg["mid_min"])
        mid_max = int(mmsi_cfg["mid_max"])
    texto = _normalizar_mmsi(mmsi)
    if texto is None or len(texto) != 9:
        return False
    mid = int(texto[:3])
    return mid_min <= mid <= mid_max


def _digito_verificador_imo(seis_digitos: str) -> int:
    """Dígito verificador de un IMO a partir de sus 6 primeros dígitos.

    Algoritmo oficial (IMO Ship Identification Number Scheme, Resolución A.1117(30)
    / circular MSC/Circ.1142): se multiplica cada uno de los 6 primeros dígitos por
    7, 6, 5, 4, 3 y 2 respectivamente; se suman los productos; el último dígito
    (unidades) de esa suma es el dígito verificador (7.º dígito).
    """
    pesos = (7, 6, 5, 4, 3, 2)
    suma = sum(int(d) * p for d, p in zip(seis_digitos, pesos))
    return suma % 10


def imo_valido(imo: Any, cfg: dict[str, Any]) -> bool:
    """¿El IMO es válido? (vacío/None es VÁLIDO; si viene, valida el check digit).

    Reglas:
    - Vacío, ``None`` o cadena en blanco => VÁLIDO (en el corpus 11.015.974 filas
      ~42.5% no tienen IMO; no todos los buques lo llevan; diagnóstico).
    - Forma ``IMO`` + 7 dígitos (o 7 dígitos a secas): válido si el 7.º dígito es el
      dígito verificador correcto de los 6 primeros (ver ``_digito_verificador_imo``).
    - Cualquier otra forma => inválido => nulificar_atributo.

    Fuente oficial del esquema de numeración IMO: Resolución A.1117(30) de la OMI.
    """
    digitos = int(cfg["calidad"]["imo"]["digitos"])  # 7
    if imo is None:
        return True
    texto = str(imo).strip()
    if texto == "":
        return True
    m = _PREFIJO_IMO.match(texto)
    if m:
        numero = m.group(1)
    elif _SOLO_DIGITOS.match(texto) and len(texto) == digitos:
        numero = texto
    else:
        return False
    if len(numero) != digitos:
        return False
    return int(numero[-1]) == _digito_verificador_imo(numero[:6])


def callsign_razonable(cs: Any) -> bool:
    """¿El indicativo de llamada (CallSign) es razonable?

    Válido si es alfanumérico en mayúsculas de 3 a 10 caracteres (los indicativos
    marítimos ITU son A-Z/0-9). Vacío/None => no razonable. Se normaliza a
    mayúsculas antes de validar.
    """
    if cs is None:
        return False
    texto = str(cs).strip().upper()
    if texto == "":
        return False
    return bool(_CALLSIGN_OK.match(texto))


def vessel_name_razonable(vn: Any) -> bool:
    """¿El nombre del buque (VesselName) es razonable?

    Válido si no está vacío y su longitud está entre 1 y 100 caracteres tras
    recortar espacios. Vacío/None/solo espacios => no razonable.
    """
    if vn is None:
        return False
    texto = str(vn).strip()
    return 1 <= len(texto) <= 100
