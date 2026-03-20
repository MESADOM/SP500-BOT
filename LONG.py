from typing import Any, Dict, List, Optional


TRAILING_STOP_PCT = 0.12
PERIODO_MEDIA_LARGA = 50
DIAS_BLOQUEO_REENTRADA = 5


def permite_entrada(
    hoy: Dict[str, Any],
    ultima_fecha_salida_ejecutada,
    operaciones: List[Dict[str, Any]],
) -> bool:
    """
    Replica la logica LONG principal de tu bot:
    - requiere senal confirmada
    - respeta bloqueo tras salida
    - evita ciertas reentradas en contexto de serrucho / exceso
    """

    if not bool(hoy.get("senal_confirmada", False)):
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
        and retorno_63_hoy > 0.04
        and cruces_sma50_hoy >= 4
    )

    bloqueo_reentrada_cercana = (
        dias_desde_ultima_salida is not None
        and 5 <= dias_desde_ultima_salida <= 9
        and retorno_63_hoy is not None
        and retorno_63_hoy > 0.04
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
    )


def actualizar_estado_interno_posicion(
    hoy: Dict[str, Any],
    posicion,
) -> None:
    """
    Mantiene actualizado el maximo desde entrada para trailing stop.
    """
    qqq3_close_hoy = float(hoy["qqq3_close"])
    posicion.maximo_desde_entrada = max(posicion.maximo_desde_entrada, qqq3_close_hoy)


def calcular_stop_trailing(posicion) -> float:
    return posicion.maximo_desde_entrada * (1.0 - TRAILING_STOP_PCT)


def senal_salida(
    hoy: Dict[str, Any],
    posicion,
) -> Optional[str]:
    """
    Replica la logica de salida LONG:
    - trailing stop
    - perdida de senal base
    """
    actualizar_estado_interno_posicion(hoy, posicion)

    qqq3_close_hoy = float(hoy["qqq3_close"])
    stop_trailing = calcular_stop_trailing(posicion)

    if qqq3_close_hoy <= stop_trailing:
        return "SELL_TRAILING"

    if not bool(hoy.get("senal_base_on", False)):
        return "SELL_SIGNAL"

    return None