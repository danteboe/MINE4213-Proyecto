"""Complemento LOCAL (sin Spark) de la prueba de compuerta serverless (FEAT-005).

Verifica que la parte PURA de las compuertas (``reglas.causas_de_descarte``, que
reproduce la columna ``causas`` de ``aplicar_compuertas`` / del CTAS de plata)
asigna la causa correcta a cada registro sucio del MISMO fixture pequeño que se
inyectó en el sandbox serverless (``resources/sql/fase3_prueba_compuerta.sql`` y
``notebooks/fase3_prueba_compuerta.py``), y que los limpios de control no generan
ninguna causa. Es el espejo local de la evidencia
``evidence/req02-capas-calidad/prueba_compuerta.md``.
"""

from __future__ import annotations

from datetime import datetime

from oceanwatch.quality.reglas import causas_de_descarte

# Instante de referencia para la regla de timestamp (UTC, ADR-004). Dentro del
# corpus (2023-06) y muy anterior al timestamp futuro del caso sucio.
_AHORA = datetime(2026, 11, 5, 0, 0, 0)

# Mismo conjunto que el sandbox serverless: cada sucio con la(s) causa(s) esperada(s).
_SUCIOS = [
    (
        "sucio_coord_rango",
        {"MMSI": 367000010, "BaseDateTime": datetime(2023, 6, 1, 10, 0), "LAT": 200.0, "LON": -74.01},
        ["coordenadas_en_rango"],
    ),
    (
        "sucio_centinela_lat",
        {"MMSI": 367000011, "BaseDateTime": datetime(2023, 6, 1, 10, 1), "LAT": 91.0, "LON": -74.01},
        # LAT=91 está fuera de rango (>90) Y es centinela: dos causas (igual que serverless).
        ["coordenadas_en_rango", "coordenada_no_centinela"],
    ),
    (
        "sucio_mmsi_8digitos",
        {"MMSI": 30012345, "BaseDateTime": datetime(2023, 6, 1, 10, 2), "LAT": 40.70, "LON": -74.01},
        # 8 dígitos: falla bien_formado Y mid_valido (mid exige 9 dígitos).
        ["mmsi_bien_formado", "mmsi_mid_valido"],
    ),
    (
        "sucio_mmsi_mid",
        {"MMSI": 999000000, "BaseDateTime": datetime(2023, 6, 1, 10, 3), "LAT": 40.70, "LON": -74.01},
        # 9 dígitos pero MID=999 fuera de [201,775]: solo mid_valido.
        ["mmsi_mid_valido"],
    ),
    (
        "sucio_ts_futuro",
        {"MMSI": 367000012, "BaseDateTime": datetime(2099, 1, 1, 0, 0), "LAT": 40.70, "LON": -74.01},
        ["timestamp_valido_no_futuro"],
    ),
]

_LIMPIOS = [
    {"MMSI": 367000020, "BaseDateTime": datetime(2023, 6, 1, 11, 0), "LAT": 40.701, "LON": -74.016},
    {"MMSI": 367000020, "BaseDateTime": datetime(2023, 6, 1, 11, 5), "LAT": 40.702, "LON": -74.017},
    {"MMSI": 367000020, "BaseDateTime": datetime(2023, 6, 1, 11, 10), "LAT": 40.703, "LON": -74.018},
]


def test_cada_sucio_recibe_su_causa(cfg):
    for caso, registro, esperadas in _SUCIOS:
        causas = causas_de_descarte(registro, cfg, _AHORA)
        assert causas == esperadas, f"{caso}: esperado {esperadas}, obtenido {causas}"
        # Cada sucio tiene al menos una causa => iría a cuarentena, NO a plata.
        assert len(causas) >= 1, f"{caso} debería ir a cuarentena"


def test_limpios_no_generan_causa(cfg):
    for registro in _LIMPIOS:
        causas = causas_de_descarte(registro, cfg, _AHORA)
        assert causas == [], f"limpio no debería tener causas, obtenido {causas}"


def test_la_causa_esperada_siempre_esta_contenida(cfg):
    # El _regla_esperada de cada sucio debe estar entre las causas observadas.
    esperada_por_caso = {
        "sucio_coord_rango": "coordenadas_en_rango",
        "sucio_centinela_lat": "coordenada_no_centinela",
        "sucio_mmsi_8digitos": "mmsi_bien_formado",
        "sucio_mmsi_mid": "mmsi_mid_valido",
        "sucio_ts_futuro": "timestamp_valido_no_futuro",
    }
    for caso, registro, _ in _SUCIOS:
        causas = causas_de_descarte(registro, cfg, _AHORA)
        assert esperada_por_caso[caso] in causas
