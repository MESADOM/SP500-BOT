from typing import Any, Dict, List, Optional


TRAILING_STOP_PCT = 0.08
DIAS_BLOQUEO_REENTRADA = 5
RETORNO_63_UMBRAL_SHORT = -0.08
CRUCES_SMA50_MAXIMOS_SHORT = 4
UMBRAL_BLOQUEO_SOBREEXTENSION_SHORT_SMA200 = -0.15


def _estructura_bajista_real(hoy: Dict[str, Any]) -> bool:
    qqq_mayor_sma200 = hoy.get("qqq_mayor_sma200")
    sma50 = hoy.get("sma50")
    sma200 = hoy.get("sma200_referencia")
    retorno_63 = hoy.get("retorno_63")
    cruces_sma50 = int(hoy.get("cruces_sma50_ventana", 0) or 0)

    if qqq_mayor_sma200 is not False:
        return False

    if sma50 is None or sma200 is None or not float(sma50) < float(sma200):
        return False

    if retorno_63 is None or not float(retorno_63) < RETORNO_63_UMBRAL_SHORT:
        return False

    return cruces_sma50 < CRUCES_SMA50_MAXIMOS_SHORT


def _distancia_qqq_sma200(hoy: Dict[str, Any]) -> Optional[float]:
    """
    Equivalencia exacta usada por la candidata experimental:
    `QQQ / SMA200 - 1` == `qqq_close_referencia / sma200_referencia - 1`.
    """
    qqq_close = hoy.get("qqq_close_referencia", hoy.get("qqq_close"))
    sma200 = hoy.get("sma200_referencia")

    if qqq_close is None or sma200 is None:
        return None

    sma200_float = float(sma200)
    if sma200_float == 0.0:
        return None

    return (float(qqq_close) / sma200_float) - 1.0


def _bloquear_short_por_sobreextension_sma200(hoy: Dict[str, Any]) -> bool:
    """
    Filtro short integrado globalmente en el sistema.

    Regla exacta:
    bloquear entrada short si `QQQ / SMA200 - 1 <= -0.15`.
    """
    distancia_qqq_sma200 = _distancia_qqq_sma200(hoy)
    if distancia_qqq_sma200 is None:
        return False

    return distancia_qqq_sma200 <= UMBRAL_BLOQUEO_SOBREEXTENSION_SHORT_SMA200


def permite_entrada(
    hoy: Dict[str, Any],
    ultima_fecha_salida_ejecutada,
    operaciones: List[Dict[str, Any]],
) -> bool:
    """
    Version SHORT de la logica de entrada:
    - requiere senal confirmada bajista
    - respeta bloqueo tras salida
    - evita reentradas cercanas en contexto de serrucho / exceso
    """

    if not bool(hoy.get("senal_short_confirmada", False)):
        return False

    if not _estructura_bajista_real(hoy):
        return False

    permitir_nueva_entrada = True
    dias_desde_ultima_salida = None

    if ultima_fecha_salida_ejecutada is not None:
        dias_desde_ultima_salida = (hoy["fecha"] - ultima_fecha_salida_ejecutada).days
        permitir_nueva_entrada = dias_desde_ultima_salida >= DIAS_BLOQUEO_REENTRADA

    retorno_63_hoy = hoy.get("retorno_63")
    cruces_sma50_hoy = int(hoy.get("cruces_sma50_ventana", 0) or 0)

    bloquear_por_retorno_y_cruces = (
        retorno_63_hoy is not None
        and retorno_63_hoy < RETORNO_63_UMBRAL_SHORT
        and cruces_sma50_hoy >= 4
    )

    bloqueo_reentrada_cercana = (
        dias_desde_ultima_salida is not None
        and 5 <= dias_desde_ultima_salida <= 9
        and retorno_63_hoy is not None
        and retorno_63_hoy < RETORNO_63_UMBRAL_SHORT
    )

    bloqueo_ultima_operacion_perdedora = (
        dias_desde_ultima_salida is not None
        and 5 <= dias_desde_ultima_salida <= 9
        and len(operaciones) >= 1
        and float(operaciones[-1].get("beneficio_neto_eur", 0.0)) < 0
    )

    return (
        permitir_nueva_entrada
        and not bloquear_por_retorno_y_cruces
        and not bloqueo_reentrada_cercana
        and not bloqueo_ultima_operacion_perdedora
        and not _bloquear_short_por_sobreextension_sma200(hoy)
    )


def actualizar_estado_interno_posicion(
    hoy: Dict[str, Any],
    posicion,
) -> None:
    """
    Mantiene actualizado el minimo desde entrada para trailing stop short.
    """
    qqq3_close_hoy = float(hoy["qqq3_close"])
    posicion.minimo_desde_entrada = min(posicion.minimo_desde_entrada, qqq3_close_hoy)


def calcular_stop_trailing(posicion) -> float:
    """
    En short, el trailing stop va por encima del minimo favorable.
    """
    return posicion.minimo_desde_entrada * (1.0 + TRAILING_STOP_PCT)


def senal_salida(
    hoy: Dict[str, Any],
    posicion,
) -> Optional[str]:
    """
    Logica de salida SHORT:
    - trailing stop short
    - perdida de senal base bajista
    """
    actualizar_estado_interno_posicion(hoy, posicion)

    qqq3_close_hoy = float(hoy["qqq3_close"])
    stop_trailing = calcular_stop_trailing(posicion)

    if qqq3_close_hoy >= stop_trailing:
        return "COVER_TRAILING"

    if not bool(hoy.get("senal_short_base_on", False)):
        return "COVER_SIGNAL"

    return None
