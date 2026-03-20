from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import LONG as modulo_long_trend
import SORT as modulo_short_trend


# ============================================================
# CONFIG
# ============================================================

VERSION_SISTEMA = "2.2.2"

BASE_DIR = Path(__file__).resolve().parent
DIR_DATOS = BASE_DIR / "datos"

RUTA_QQQ = DIR_DATOS / "QQQ.csv"
RUTA_QQQ3 = DIR_DATOS / "QQQ3.csv"
RUTA_VIX = DIR_DATOS / "VIX.csv"

GUARDAR_RESULTADOS = False
RUTA_SALIDA_OPERACIONES = DIR_DATOS / "operaciones_generadas.csv"
RUTA_SALIDA_RESUMEN = DIR_DATOS / "resumen_anual_generado.csv"

CAPITAL_INICIAL_EUR = 1000.0
COMISION_POR_OPERACION_EUR = 2.0

PERIODO_MEDIA_LARGA = 50
DIAS_CONFIRMACION_ENTRADA = 1

REGIMEN_AGRESIVO = "AGRESIVO"
REGIMEN_DEFENSIVO = "DEFENSIVO"

FRECUENCIA_REVISION_REGIMEN = "SEMANAL"
PERIODO_SMA200_REGIMEN = 200
VENTANA_RETORNO_63_REGIMEN = 63
VENTANA_CRUCES_SMA50_REGIMEN = 20
UMBRAL_CRUCES_SERRUCHO = 4

SIZING_AGRESIVO_PORCENTAJE_CAPITAL = 0.90
SIZING_AGRESIVO_MAX_UNIDADES = 50

SIZING_DEFENSIVO_PORCENTAJE_CAPITAL = 0.70
SIZING_DEFENSIVO_MAX_UNIDADES = 10

REGIMEN_LONG_TREND = "LONG_TREND"
REGIMEN_SHORT_TREND = "SHORT_TREND"
REGIMEN_MEAN_REVERSION = "MEAN_REVERSION"
REGIMEN_NO_TRADE = "NO_TRADE"

MODO_META = "LONG_SHORT"


# ============================================================
# DATACLASSES
# ============================================================

@dataclass
class OperacionAbierta:
    modulo_activo: str
    fecha_entrada: datetime
    precio_entrada: float
    unidades: int
    capital_antes_eur: float
    maximo_desde_entrada: float
    minimo_desde_entrada: float
    senal_entrada: str
    regimen_entrada: str
    porcentaje_objetivo_entrada: float
    max_unidades_entrada: int
    capital_objetivo_entrada_eur: float
    capital_invertido_entrada_eur: float
    porcentaje_real_invertido: float
    entrada_capada_por_unidades: bool
    score_regimen: int
    qqq_close_referencia: float
    sma200_referencia: Optional[float]
    qqq_mayor_sma200: Optional[bool]
    retorno_63: Optional[float]
    retorno_estado: str
    cruces_sma50_ventana: int
    cruces_estado: str
    motivo_regimen: str


@dataclass
class EstadoDiagnostico:
    entradas_capadas_por_unidades: int = 0
    senales_no_ejecutadas_sin_capital: int = 0


# ============================================================
# UTILIDADES
# ============================================================

def _parse_num_es(value: str) -> float:
    value = str(value).strip().replace(".", "").replace(",", ".")
    return float(value)


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def cargar_csv(ruta: Path) -> List[Dict[str, Any]]:
    with open(ruta, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(r) for r in reader]


def guardar_csv(ruta: Path, filas: List[Dict[str, Any]]) -> None:
    if not filas:
        with open(ruta, "w", encoding="utf-8-sig", newline="") as fh:
            fh.write("")
        return

    columnas = list(filas[0].keys())
    with open(ruta, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columnas)
        writer.writeheader()
        for fila in filas:
            serializada = {}
            for k, v in fila.items():
                if isinstance(v, datetime):
                    serializada[k] = v.strftime("%Y-%m-%d")
                else:
                    serializada[k] = v
            writer.writerow(serializada)


def _serializar_tsv(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, bool):
        return "VERDADERO" if value else "FALSO"
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return ""
        texto = f"{value:.10f}".rstrip("0").rstrip(".")
        texto = "0" if texto in {"", "-0"} else texto
        return texto.replace(".", ",")
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def imprimir_tabla_tsv(columnas: List[str], filas: List[Dict[str, Any]]) -> None:
    print("\t".join(columnas))
    for fila in filas:
        print("\t".join(_serializar_tsv(fila.get(columna)) for columna in columnas))


def construir_tablas_salida(resultados: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    resumen_anual = []
    for fila in resultados["resumen_anual"]:
        resumen_anual.append(
            {
                "Año": fila.get("anio"),
                "Operaciones": fila.get("operaciones"),
                "Beneficio neto €": fila.get("beneficio_neto_eur"),
                "Ganadoras": fila.get("ganadoras"),
                "Perdedoras": fila.get("perdedoras"),
                "Win rate %": fila.get("win_rate_pct"),
                "Capital acumulado €": fila.get("capital_acumulado_eur"),
                "Rentabilidad %": fila.get("rentabilidad_pct"),
                "Drawdown máx %": fila.get("drawdown_max_pct"),
            }
        )

    operaciones_ordenadas = sorted(
        resultados["operaciones"],
        key=lambda fila: (fila["fecha_entrada"], fila["fecha_salida"]),
    )
    detalle_operaciones = []
    for fila in operaciones_ordenadas:
        detalle_operaciones.append(
            {
                "Fecha entrada": fila.get("fecha_entrada"),
                "Fecha salida": fila.get("fecha_salida"),
                "Modulo activo": (
                    "LONG_TREND"
                    if "QQQ>SMA50" in str(fila.get("senal_entrada", ""))
                    else "SHORT_TREND"
                    if "QQQ<SMA50" in str(fila.get("senal_entrada", ""))
                    else ""
                ),
                "Señal entrada": fila.get("senal_entrada"),
                "Precio entrada": fila.get("precio_entrada"),
                "Precio salida": fila.get("precio_salida"),
                "Unidades": fila.get("unidades"),
                "Motivo salida": fila.get("motivo_salida"),
                "Beneficio acumulado €": fila.get("beneficio_acumulado_eur"),
                "Rentabilidad %": fila.get("rentabilidad_pct"),
                "Capital acumulado €": fila.get("capital_acumulado_eur"),
                "Beneficio neto €": fila.get("beneficio_neto_eur"),
                "Regimen vigente": fila.get("regimen_vigente", fila.get("regimen_entrada")),
                "Motivo régimen": fila.get("motivo_regimen"),
                "Porcentaje capital usado": fila.get("porcentaje_real_invertido"),
                "Capital antes entrada €": fila.get("capital_antes_eur"),
                "QQQ > SMA200": fila.get("qqq_mayor_sma200"),
                "Retorno 63": fila.get("retorno_63"),
                "Cruces SMA50": fila.get("cruces_sma50_ventana"),
            }
        )

    return resumen_anual, detalle_operaciones


def _normalizar_columnas(rows: Iterable[Dict[str, Any]], prefijo: str) -> List[Dict[str, Any]]:
    normalizadas: List[Dict[str, Any]] = []

    for raw in rows:
        row = {str(k).strip().lower(): v for k, v in raw.items()}

        if prefijo == "qqq" and len(row) == 1:
            unico = next(iter(row.values()))
            campos = next(csv.reader([str(unico)]))
            if len(campos) >= 5:
                normalizadas.append(
                    {
                        "fecha": _to_datetime(campos[0]),
                        f"{prefijo}_close": float(campos[1]),
                        f"{prefijo}_open": float(campos[2]),
                        f"{prefijo}_high": float(campos[3]),
                        f"{prefijo}_low": float(campos[4]),
                    }
                )
            continue

        out: Dict[str, Any] = {}
        for col, val in row.items():
            if col in ["date", "fecha", '"fecha"', '"date']:
                out["fecha"] = _to_datetime(val)
            elif col in ["close", "adj close", "adj_close", "cierre", "último", "ultimo"]:
                if prefijo == "qqq":
                    out[f"{prefijo}_close"] = float(str(val).replace(",", "."))
                else:
                    out[f"{prefijo}_close"] = _parse_num_es(val)
            elif col in ["open", "apertura"]:
                if prefijo == "qqq":
                    out[f"{prefijo}_open"] = float(str(val).replace(",", "."))
                else:
                    out[f"{prefijo}_open"] = _parse_num_es(val)
            elif col in ["high", "max", "alto", "máximo"]:
                if prefijo == "qqq":
                    out[f"{prefijo}_high"] = float(str(val).replace(",", "."))
                else:
                    out[f"{prefijo}_high"] = _parse_num_es(val)
            elif col in ["low", "min", "bajo", "mínimo"]:
                if prefijo == "qqq":
                    out[f"{prefijo}_low"] = float(str(val).replace(",", "."))
                else:
                    out[f"{prefijo}_low"] = _parse_num_es(val)

        if out.get("fecha") is not None:
            normalizadas.append(out)

    normalizadas.sort(key=lambda x: x["fecha"])
    return normalizadas


# ============================================================
# INDICADORES
# ============================================================

def _media_simple(closes: List[float], idx: int, periodo: int) -> Optional[float]:
    if idx + 1 < periodo:
        return None
    inicio = idx - periodo + 1
    return sum(closes[inicio : idx + 1]) / float(periodo)


def _clasificar_retorno_63(retorno_63: Optional[float]) -> str:
    if retorno_63 is None:
        return "NEUTRAL"
    if retorno_63 > 0.03:
        return "POSITIVO"
    if retorno_63 < -0.05:
        return "NEGATIVO"
    return "NEUTRAL"


def _clasificar_cruces(cruces_sma50: int) -> str:
    return "ALTO" if cruces_sma50 > UMBRAL_CRUCES_SERRUCHO else "NO_ALTO"


def _calcular_cruces_sma50(closes: List[float], idx: int) -> int:
    cruces = 0
    inicio = max(0, idx - VENTANA_CRUCES_SMA50_REGIMEN + 1)
    ultimo_signo: Optional[int] = None

    for j in range(inicio, idx + 1):
        sma50 = _media_simple(closes, j, 50)
        if sma50 is None:
            continue

        diff = closes[j] - sma50
        signo_actual = 0
        if diff > 0:
            signo_actual = 1
        elif diff < 0:
            signo_actual = -1

        if ultimo_signo is not None and signo_actual != 0 and signo_actual != ultimo_signo:
            cruces += 1

        if signo_actual != 0:
            ultimo_signo = signo_actual

    return cruces


def calcular_variables_regimen(closes: List[float], idx: int) -> Dict[str, Any]:
    close_actual = closes[idx]
    sma200 = _media_simple(closes, idx, PERIODO_SMA200_REGIMEN)

    retorno_63: Optional[float] = None
    if idx >= VENTANA_RETORNO_63_REGIMEN:
        close_pasado = closes[idx - VENTANA_RETORNO_63_REGIMEN]
        if close_pasado > 0:
            retorno_63 = (close_actual / close_pasado) - 1.0

    cruces_sma50 = _calcular_cruces_sma50(closes, idx)

    return {
        "qqq_sobre_sma200": None if sma200 is None else close_actual > sma200,
        "sma200": sma200,
        "retorno_63": retorno_63,
        "cruces_sma50": cruces_sma50,
    }


def evaluar_regimen_sizing(variables_regimen: Dict[str, Any], qqq_close_referencia: float) -> Dict[str, Any]:
    qqq_sobre_sma200 = variables_regimen.get("qqq_sobre_sma200")
    sma200 = variables_regimen.get("sma200")
    retorno_63 = variables_regimen.get("retorno_63")
    cruces_sma50 = int(variables_regimen.get("cruces_sma50", 0) or 0)

    retorno_estado = _clasificar_retorno_63(retorno_63)
    cruces_estado = _clasificar_cruces(cruces_sma50)

    if qqq_sobre_sma200 is False and retorno_estado == "NEGATIVO" and cruces_estado == "ALTO":
        regimen = REGIMEN_DEFENSIVO
        motivo = "DEFENSIVO: qqq<sma200, retorno_63 negativo y cruces altos"
    elif qqq_sobre_sma200 is True and cruces_estado != "ALTO":
        regimen = REGIMEN_AGRESIVO
        motivo = "AGRESIVO: qqq>sma200 y cruces no altos; retorno neutral o ligeramente negativo no invalida"
    elif qqq_sobre_sma200 is True and retorno_estado == "POSITIVO":
        regimen = REGIMEN_AGRESIVO
        motivo = "AGRESIVO: qqq>sma200 y retorno positivo"
    else:
        regimen = REGIMEN_AGRESIVO
        motivo = "AGRESIVO: caso intermedio resuelto a favor del sesgo alcista"

    score = 0
    if qqq_sobre_sma200 is True:
        score += 1
    elif qqq_sobre_sma200 is False:
        score -= 1

    if retorno_estado == "POSITIVO":
        score += 1
    elif retorno_estado == "NEGATIVO":
        score -= 1

    if cruces_estado == "ALTO":
        score -= 1

    return {
        "regimen": regimen,
        "score_regimen": score,
        "qqq_close_referencia": qqq_close_referencia,
        "sma200_referencia": sma200,
        "qqq_mayor_sma200": qqq_sobre_sma200,
        "retorno_63": retorno_63,
        "retorno_estado": retorno_estado,
        "cruces_sma50_ventana": cruces_sma50,
        "cruces_estado": cruces_estado,
        "motivo_regimen": motivo,
    }


def detectar_meta_regimen(hoy: Dict[str, Any]) -> str:
    if MODO_META == "LONG_ONLY":
        return REGIMEN_LONG_TREND

    if MODO_META == "SHORT_ONLY":
        return REGIMEN_SHORT_TREND

    if MODO_META == "LONG_SHORT":
        qqq_mayor_sma200 = hoy.get("qqq_mayor_sma200")
        retorno_estado = hoy.get("retorno_estado")
        cruces_estado = hoy.get("cruces_estado")

        if qqq_mayor_sma200 is True and retorno_estado in ("POSITIVO", "NEUTRAL") and cruces_estado != "ALTO":
            return REGIMEN_LONG_TREND

        if qqq_mayor_sma200 is False and retorno_estado == "NEGATIVO":
            return REGIMEN_SHORT_TREND

        return REGIMEN_NO_TRADE

    return REGIMEN_NO_TRADE


def obtener_parametros_sizing(regimen: str) -> Tuple[float, int]:
    if regimen == REGIMEN_AGRESIVO:
        return float(SIZING_AGRESIVO_PORCENTAJE_CAPITAL), int(SIZING_AGRESIVO_MAX_UNIDADES)
    return float(SIZING_DEFENSIVO_PORCENTAJE_CAPITAL), int(SIZING_DEFENSIVO_MAX_UNIDADES)


def _es_momento_revision_regimen(
    fecha_actual: datetime,
    ultima_revision_semana: Optional[Tuple[int, int]],
) -> bool:
    if FRECUENCIA_REVISION_REGIMEN != "SEMANAL":
        return True
    semana_actual = (fecha_actual.isocalendar().year, fecha_actual.isocalendar().week)
    return semana_actual != ultima_revision_semana


# ============================================================
# PREPARAR DATOS
# ============================================================

def preparar_datos(
    df_qqq: List[Dict[str, Any]],
    df_qqq3: List[Dict[str, Any]],
    df_vix: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    qqq = _normalizar_columnas(df_qqq, prefijo="qqq")
    qqq3 = _normalizar_columnas(df_qqq3, prefijo="qqq3")
    _ = _normalizar_columnas(df_vix, prefijo="vix")

    map_qqq = {r["fecha"]: r for r in qqq}
    map_qqq3 = {r["fecha"]: r for r in qqq3}
    fechas = sorted(set(map_qqq.keys()) | set(map_qqq3.keys()))

    rows: List[Dict[str, Any]] = []
    closes: List[float] = []
    ultimas_senales: List[bool] = []
    ultimas_senales_short: List[bool] = []
    n_confirmacion = max(1, int(DIAS_CONFIRMACION_ENTRADA))

    regimen_sizing_actual = REGIMEN_DEFENSIVO
    ultima_semana_revisada: Optional[Tuple[int, int]] = None
    ultima_info_regimen: Dict[str, Any] = {
        "score_regimen": 0,
        "qqq_close_referencia": 0.0,
        "sma200_referencia": None,
        "qqq_mayor_sma200": None,
        "retorno_63": None,
        "retorno_estado": "NEUTRAL",
        "cruces_sma50_ventana": 0,
        "cruces_estado": "NO_ALTO",
        "motivo_regimen": "estado inicial",
    }

    for fecha in fechas:
        row = {
            "fecha": fecha,
            "qqq_close": map_qqq.get(fecha, {}).get("qqq_close"),
            "qqq3_close": map_qqq3.get(fecha, {}).get("qqq3_close"),
            "qqq3_open": map_qqq3.get(fecha, {}).get("qqq3_open"),
            "regimen_sizing": regimen_sizing_actual,
            "score_regimen": ultima_info_regimen["score_regimen"],
            "qqq_close_referencia": ultima_info_regimen["qqq_close_referencia"],
            "sma200_referencia": ultima_info_regimen["sma200_referencia"],
            "qqq_mayor_sma200": ultima_info_regimen["qqq_mayor_sma200"],
            "retorno_63": ultima_info_regimen["retorno_63"],
            "retorno_estado": ultima_info_regimen["retorno_estado"],
            "cruces_sma50_ventana": ultima_info_regimen["cruces_sma50_ventana"],
            "cruces_estado": ultima_info_regimen["cruces_estado"],
            "motivo_regimen": ultima_info_regimen["motivo_regimen"],
            "meta_regimen": REGIMEN_NO_TRADE,
        }

        close = row["qqq_close"]
        if close is None:
            row["qqq_media_larga"] = None
            row["senal_base_on"] = False
            row["senal_confirmada"] = False
            row["senal_short_base_on"] = False
            row["senal_short_confirmada"] = False
            row["sma50"] = None
            rows.append(row)
            continue

        close_float = float(close)
        closes.append(close_float)

        sma50 = _media_simple(closes, len(closes) - 1, PERIODO_MEDIA_LARGA)

        row["sma50"] = sma50
        row["qqq_media_larga"] = sma50
        row["senal_base_on"] = bool(sma50 is not None and close_float > sma50)
        row["senal_short_base_on"] = bool(sma50 is not None and close_float < sma50)

        ultimas_senales.append(bool(row["senal_base_on"]))
        if len(ultimas_senales) > n_confirmacion:
            ultimas_senales.pop(0)
        row["senal_confirmada"] = len(ultimas_senales) == n_confirmacion and all(ultimas_senales)

        ultimas_senales_short.append(bool(row["senal_short_base_on"]))
        if len(ultimas_senales_short) > n_confirmacion:
            ultimas_senales_short.pop(0)
        row["senal_short_confirmada"] = len(ultimas_senales_short) == n_confirmacion and all(ultimas_senales_short)

        if _es_momento_revision_regimen(fecha, ultima_semana_revisada):
            variables_regimen = calcular_variables_regimen(closes=closes, idx=len(closes) - 1)
            info_regimen = evaluar_regimen_sizing(variables_regimen, qqq_close_referencia=close_float)

            regimen_sizing_actual = info_regimen["regimen"]
            ultima_info_regimen = info_regimen
            ultima_semana_revisada = (fecha.isocalendar().year, fecha.isocalendar().week)

        row["regimen_sizing"] = regimen_sizing_actual
        row["score_regimen"] = ultima_info_regimen["score_regimen"]
        row["qqq_close_referencia"] = close_float
        row["sma200_referencia"] = ultima_info_regimen["sma200_referencia"]
        row["qqq_mayor_sma200"] = ultima_info_regimen["qqq_mayor_sma200"]
        row["retorno_63"] = ultima_info_regimen["retorno_63"]
        row["retorno_estado"] = ultima_info_regimen["retorno_estado"]
        row["cruces_sma50_ventana"] = ultima_info_regimen["cruces_sma50_ventana"]
        row["cruces_estado"] = ultima_info_regimen["cruces_estado"]
        row["motivo_regimen"] = ultima_info_regimen["motivo_regimen"]

        row["meta_regimen"] = detectar_meta_regimen(row)

        rows.append(row)

    return rows


# ============================================================
# ENGINE
# ============================================================

def ejecutar_meta_bot(
    df: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    capital_actual = float(CAPITAL_INICIAL_EUR)
    operacion_abierta: Optional[OperacionAbierta] = None
    operaciones: List[Dict[str, Any]] = []
    diagnostico = EstadoDiagnostico()

    entrada_pendiente = False
    salida_pendiente = False
    motivo_salida_pendiente = ""
    modulo_entrada_pendiente = ""
    regimen_entrada_pendiente = REGIMEN_DEFENSIVO
    diagnostico_entrada_pendiente: Dict[str, Any] = {}
    ultima_fecha_salida_ejecutada: Optional[datetime] = None

    for i in range(len(df) - 1):
        hoy = df[i]
        manana = df[i + 1]

        qqq_close_hoy = hoy.get("qqq_close")
        qqq3_close_hoy = hoy.get("qqq3_close")
        qqq3_open_manana = manana.get("qqq3_open")

        if qqq_close_hoy is None or qqq3_close_hoy is None or qqq3_open_manana is None:
            continue

        qqq3_open_manana = float(qqq3_open_manana)

        if salida_pendiente and operacion_abierta is not None:
            precio_salida = qqq3_open_manana
            if operacion_abierta.modulo_activo == REGIMEN_LONG_TREND:
                beneficio_bruto = (precio_salida - operacion_abierta.precio_entrada) * operacion_abierta.unidades
            elif operacion_abierta.modulo_activo == REGIMEN_SHORT_TREND:
                beneficio_bruto = (operacion_abierta.precio_entrada - precio_salida) * operacion_abierta.unidades
            else:
                beneficio_bruto = 0.0
            beneficio_neto = beneficio_bruto - COMISION_POR_OPERACION_EUR

            rentabilidad_pct = 0.0
            if operacion_abierta.capital_antes_eur > 0:
                rentabilidad_pct = (beneficio_neto / operacion_abierta.capital_antes_eur) * 100.0

            capital_actual += beneficio_neto
            beneficio_acumulado_eur = capital_actual - CAPITAL_INICIAL_EUR
            if operacion_abierta.modulo_activo == REGIMEN_LONG_TREND:
                stop_trailing = modulo_long_trend.calcular_stop_trailing(operacion_abierta)
            elif operacion_abierta.modulo_activo == REGIMEN_SHORT_TREND:
                stop_trailing = modulo_short_trend.calcular_stop_trailing(operacion_abierta)
            else:
                stop_trailing = 0.0

            operaciones.append(
                {
                    "version_sistema": VERSION_SISTEMA,
                    "modulo_activo": operacion_abierta.modulo_activo,
                    "fecha_entrada": operacion_abierta.fecha_entrada,
                    "fecha_salida": manana["fecha"],
                    "precio_entrada": round(operacion_abierta.precio_entrada, 6),
                    "precio_salida": round(precio_salida, 6),
                    "unidades": int(operacion_abierta.unidades),
                    "senal_entrada": operacion_abierta.senal_entrada,
                    "motivo_salida": motivo_salida_pendiente,
                    "regimen_entrada": operacion_abierta.regimen_entrada,
                    "regimen_vigente": operacion_abierta.regimen_entrada,
                    "score_regimen": operacion_abierta.score_regimen,
                    "qqq_close_referencia": operacion_abierta.qqq_close_referencia,
                    "sma200_referencia": operacion_abierta.sma200_referencia,
                    "qqq_mayor_sma200": operacion_abierta.qqq_mayor_sma200,
                    "retorno_63": operacion_abierta.retorno_63,
                    "retorno_estado": operacion_abierta.retorno_estado,
                    "cruces_sma50_ventana": operacion_abierta.cruces_sma50_ventana,
                    "cruces_estado": operacion_abierta.cruces_estado,
                    "motivo_regimen": operacion_abierta.motivo_regimen,
                    "porcentaje_objetivo_entrada": round(operacion_abierta.porcentaje_objetivo_entrada, 4),
                    "max_unidades_entrada": int(operacion_abierta.max_unidades_entrada),
                    "capital_objetivo_entrada_eur": round(operacion_abierta.capital_objetivo_entrada_eur, 2),
                    "capital_invertido_entrada_eur": round(operacion_abierta.capital_invertido_entrada_eur, 2),
                    "porcentaje_real_invertido": round(operacion_abierta.porcentaje_real_invertido, 4),
                    "entrada_capada_por_unidades": bool(operacion_abierta.entrada_capada_por_unidades),
                    "beneficio_neto_eur": round(beneficio_neto, 2),
                    "beneficio_acumulado_eur": round(beneficio_acumulado_eur, 2),
                    "rentabilidad_pct": round(rentabilidad_pct, 4),
                    "capital_antes_eur": round(operacion_abierta.capital_antes_eur, 2),
                    "capital_acumulado_eur": round(capital_actual, 2),
                    "maximo_desde_entrada": round(operacion_abierta.maximo_desde_entrada, 6),
                    "stop_trailing": round(stop_trailing, 6),
                }
            )

            ultima_fecha_salida_ejecutada = manana["fecha"]
            operacion_abierta = None
            salida_pendiente = False
            motivo_salida_pendiente = ""

        if entrada_pendiente and operacion_abierta is None:
            porcentaje_objetivo, max_unidades = obtener_parametros_sizing(regimen_entrada_pendiente)

            capital_objetivo = capital_actual * porcentaje_objetivo
            unidades_teoricas = int(math.floor(capital_objetivo / qqq3_open_manana)) if qqq3_open_manana > 0 else 0
            unidades = max(0, min(unidades_teoricas, max_unidades))
            entrada_capada = unidades_teoricas > max_unidades

            coste_entrada = unidades * qqq3_open_manana + COMISION_POR_OPERACION_EUR
            capital_invertido = unidades * qqq3_open_manana
            porcentaje_real = (capital_invertido / capital_actual) if capital_actual > 0 else 0.0

            if entrada_capada:
                diagnostico.entradas_capadas_por_unidades += 1

            if unidades > 0 and coste_entrada <= capital_actual:
                operacion_abierta = OperacionAbierta(
                    modulo_activo=modulo_entrada_pendiente,
                    fecha_entrada=manana["fecha"],
                    precio_entrada=qqq3_open_manana,
                    unidades=unidades,
                    capital_antes_eur=capital_actual,
                    maximo_desde_entrada=qqq3_open_manana,
                    minimo_desde_entrada=qqq3_open_manana,
                    senal_entrada=(
                        f"QQQ>SMA{PERIODO_MEDIA_LARGA} x{DIAS_CONFIRMACION_ENTRADA}"
                        if modulo_entrada_pendiente == REGIMEN_LONG_TREND
                        else f"QQQ<SMA{PERIODO_MEDIA_LARGA} x{DIAS_CONFIRMACION_ENTRADA}"
                    ),
                    regimen_entrada=regimen_entrada_pendiente,
                    porcentaje_objetivo_entrada=porcentaje_objetivo,
                    max_unidades_entrada=max_unidades,
                    capital_objetivo_entrada_eur=capital_objetivo,
                    capital_invertido_entrada_eur=capital_invertido,
                    porcentaje_real_invertido=porcentaje_real,
                    entrada_capada_por_unidades=entrada_capada,
                    score_regimen=int(diagnostico_entrada_pendiente.get("score_regimen", 0)),
                    qqq_close_referencia=float(diagnostico_entrada_pendiente.get("qqq_close_referencia", 0.0)),
                    sma200_referencia=diagnostico_entrada_pendiente.get("sma200_referencia"),
                    qqq_mayor_sma200=diagnostico_entrada_pendiente.get("qqq_mayor_sma200"),
                    retorno_63=diagnostico_entrada_pendiente.get("retorno_63"),
                    retorno_estado=str(diagnostico_entrada_pendiente.get("retorno_estado", "NEUTRAL")),
                    cruces_sma50_ventana=int(diagnostico_entrada_pendiente.get("cruces_sma50_ventana", 0)),
                    cruces_estado=str(diagnostico_entrada_pendiente.get("cruces_estado", "NO_ALTO")),
                    motivo_regimen=str(diagnostico_entrada_pendiente.get("motivo_regimen", "")),
                )
            else:
                diagnostico.senales_no_ejecutadas_sin_capital += 1

            entrada_pendiente = False
            modulo_entrada_pendiente = ""

        if operacion_abierta is None:
            meta_regimen_hoy = hoy.get("meta_regimen", REGIMEN_NO_TRADE)

            if meta_regimen_hoy == REGIMEN_LONG_TREND:
                permitir_entrada = modulo_long_trend.permite_entrada(
                    hoy=hoy,
                    ultima_fecha_salida_ejecutada=ultima_fecha_salida_ejecutada,
                    operaciones=operaciones,
                )

                if permitir_entrada:
                    entrada_pendiente = True
                    modulo_entrada_pendiente = REGIMEN_LONG_TREND
                    regimen_entrada_pendiente = str(hoy.get("regimen_sizing", REGIMEN_DEFENSIVO))
                    diagnostico_entrada_pendiente = {
                        "score_regimen": hoy.get("score_regimen", 0),
                        "qqq_close_referencia": hoy.get("qqq_close_referencia", hoy.get("qqq_close", 0.0) or 0.0),
                        "sma200_referencia": hoy.get("sma200_referencia"),
                        "qqq_mayor_sma200": hoy.get("qqq_mayor_sma200"),
                        "retorno_63": hoy.get("retorno_63"),
                        "retorno_estado": hoy.get("retorno_estado", "NEUTRAL"),
                        "cruces_sma50_ventana": hoy.get("cruces_sma50_ventana", 0),
                        "cruces_estado": hoy.get("cruces_estado", "NO_ALTO"),
                        "motivo_regimen": hoy.get("motivo_regimen", ""),
                    }

            elif meta_regimen_hoy == REGIMEN_SHORT_TREND:
                permitir_entrada = modulo_short_trend.permite_entrada(
                    hoy=hoy,
                    ultima_fecha_salida_ejecutada=ultima_fecha_salida_ejecutada,
                    operaciones=operaciones,
                )

                if permitir_entrada:
                    entrada_pendiente = True
                    modulo_entrada_pendiente = REGIMEN_SHORT_TREND
                    regimen_entrada_pendiente = str(hoy.get("regimen_sizing", REGIMEN_DEFENSIVO))
                    diagnostico_entrada_pendiente = {
                        "score_regimen": hoy.get("score_regimen", 0),
                        "qqq_close_referencia": hoy.get("qqq_close_referencia", hoy.get("qqq_close", 0.0) or 0.0),
                        "sma200_referencia": hoy.get("sma200_referencia"),
                        "qqq_mayor_sma200": hoy.get("qqq_mayor_sma200"),
                        "retorno_63": hoy.get("retorno_63"),
                        "retorno_estado": hoy.get("retorno_estado", "NEUTRAL"),
                        "cruces_sma50_ventana": hoy.get("cruces_sma50_ventana", 0),
                        "cruces_estado": hoy.get("cruces_estado", "NO_ALTO"),
                        "motivo_regimen": hoy.get("motivo_regimen", ""),
                    }

        else:
            if operacion_abierta.modulo_activo == REGIMEN_LONG_TREND:
                motivo_salida = modulo_long_trend.senal_salida(hoy, operacion_abierta)
                if motivo_salida:
                    salida_pendiente = True
                    motivo_salida_pendiente = motivo_salida
            elif operacion_abierta.modulo_activo == REGIMEN_SHORT_TREND:
                motivo_salida = modulo_short_trend.senal_salida(hoy, operacion_abierta)
                if motivo_salida:
                    salida_pendiente = True
                    motivo_salida_pendiente = motivo_salida

    operaciones_ordenadas = sorted(operaciones, key=lambda x: x["fecha_salida"])

    diagnostico_regimen_tsv: List[Dict[str, Any]] = []
    for op in operaciones_ordenadas:
        diagnostico_regimen_tsv.append(
            {
                "fecha_entrada": op["fecha_entrada"],
                "fecha_salida": op["fecha_salida"],
                "modulo_activo": op["modulo_activo"],
                "regimen_vigente": op["regimen_vigente"],
                "score_regimen": op.get("score_regimen", 0),
                "qqq_close_referencia": round(float(op.get("qqq_close_referencia", 0.0)), 6),
                "sma200_referencia": None if op.get("sma200_referencia") is None else round(float(op["sma200_referencia"]), 6),
                "qqq_mayor_sma200": op.get("qqq_mayor_sma200"),
                "retorno_63": None if op.get("retorno_63") is None else round(float(op["retorno_63"]), 6),
                "retorno_estado": op.get("retorno_estado"),
                "cruces_sma50_ventana": op.get("cruces_sma50_ventana", 0),
                "cruces_estado": op.get("cruces_estado"),
                "porcentaje_capital_objetivo": op.get("porcentaje_objetivo_entrada"),
                "max_unidades_regimen": op.get("max_unidades_entrada"),
                "unidades_ejecutadas": op.get("unidades"),
                "capital_objetivo": op.get("capital_objetivo_entrada_eur"),
                "capital_invertido_real": op.get("capital_invertido_entrada_eur"),
                "porcentaje_real_invertido": op.get("porcentaje_real_invertido"),
                "entrada_capada_por_max_unidades": op.get("entrada_capada_por_unidades"),
                "motivo_regimen": op.get("motivo_regimen", ""),
            }
        )

    metricas = crear_metricas_diagnosticas(operaciones_ordenadas, diagnostico)
    resumen_regimen = crear_resumen_regimen(operaciones_ordenadas)

    return operaciones_ordenadas, metricas, diagnostico_regimen_tsv, resumen_regimen


# ============================================================
# RESUMENES
# ============================================================

def crear_resumen_regimen(df_operaciones: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}

    for regimen in [REGIMEN_AGRESIVO, REGIMEN_DEFENSIVO]:
        ops = [op for op in df_operaciones if op.get("regimen_vigente") == regimen]
        total = round(sum(float(op.get("beneficio_neto_eur", 0.0)) for op in ops), 2)
        out[regimen] = {
            "operaciones": len(ops),
            "beneficio_neto_total": total,
            "beneficio_medio_por_operacion": round(total / len(ops), 4) if ops else 0.0,
        }

    return out


def crear_resumen_anual(df_operaciones: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not df_operaciones:
        return []

    by_year: Dict[int, List[Dict[str, Any]]] = {}
    for op in df_operaciones:
        anio = op["fecha_salida"].year
        by_year.setdefault(anio, []).append(op)

    resumen: List[Dict[str, Any]] = []

    for anio in sorted(by_year.keys()):
        ops = by_year[anio]
        operaciones = len(ops)
        ganadoras = sum(1 for op in ops if op["beneficio_neto_eur"] > 0)
        perdedoras = operaciones - ganadoras
        beneficio_neto = round(sum(op["beneficio_neto_eur"] for op in ops), 2)

        win_rate = round((ganadoras / operaciones * 100.0) if operaciones else 0.0, 4)
        rentabilidad = round((beneficio_neto / CAPITAL_INICIAL_EUR) * 100.0, 4)

        curva = [float(op["capital_acumulado_eur"]) for op in ops]
        pico = 0.0
        dd_min = 0.0
        for valor in curva:
            pico = max(pico, valor)
            if pico > 0:
                dd = ((valor - pico) / pico) * 100.0
                dd_min = min(dd_min, dd)

        ops_agresivo = [op for op in ops if op.get("regimen_entrada") == REGIMEN_AGRESIVO]
        ops_defensivo = [op for op in ops if op.get("regimen_entrada") == REGIMEN_DEFENSIVO]

        resumen.append(
            {
                "version_sistema": VERSION_SISTEMA,
                "anio": anio,
                "operaciones": operaciones,
                "ganadoras": ganadoras,
                "perdedoras": perdedoras,
                "win_rate_pct": win_rate,
                "beneficio_neto_eur": beneficio_neto,
                "rentabilidad_pct": rentabilidad,
                "drawdown_max_pct": round(dd_min, 4),
                "operaciones_agresivo": len(ops_agresivo),
                "operaciones_defensivo": len(ops_defensivo),
                "beneficio_neto_agresivo_eur": round(sum(op["beneficio_neto_eur"] for op in ops_agresivo), 2),
                "beneficio_neto_defensivo_eur": round(sum(op["beneficio_neto_eur"] for op in ops_defensivo), 2),
                "capital_acumulado_eur": round(float(ops[-1]["capital_acumulado_eur"]), 2),
            }
        )

    return resumen


def crear_metricas_diagnosticas(df_operaciones: List[Dict[str, Any]], estado: EstadoDiagnostico) -> Dict[str, Any]:
    total_ops = len(df_operaciones)
    unidades_medias = 0.0
    pct_real_medio = 0.0

    if total_ops > 0:
        unidades_medias = sum(float(op.get("unidades", 0)) for op in df_operaciones) / total_ops
        pct_real_medio = (
            sum(float(op.get("porcentaje_real_invertido", 0.0)) for op in df_operaciones) / total_ops
        ) * 100.0

    return {
        "unidades_medias_por_operacion": round(unidades_medias, 4),
        "porcentaje_medio_capital_real_invertido": round(pct_real_medio, 4),
        "entradas_capadas_por_limite_unidades": int(estado.entradas_capadas_por_unidades),
        "senales_no_ejecutadas_sin_capital": int(estado.senales_no_ejecutadas_sin_capital),
    }


# ============================================================
# MAIN
# ============================================================

def ejecutar_bot() -> Dict[str, Any]:
    df_qqq = cargar_csv(RUTA_QQQ)
    df_qqq3 = cargar_csv(RUTA_QQQ3)
    df_vix = cargar_csv(RUTA_VIX)

    df_base = preparar_datos(df_qqq=df_qqq, df_qqq3=df_qqq3, df_vix=df_vix)
    df_operaciones, metricas, diagnostico_regimen_tsv, resumen_regimen = ejecutar_meta_bot(df_base)
    df_resumen_anual = crear_resumen_anual(df_operaciones)

    if GUARDAR_RESULTADOS:
        guardar_csv(RUTA_SALIDA_OPERACIONES, df_operaciones)
        guardar_csv(RUTA_SALIDA_RESUMEN, df_resumen_anual)

    return {
        "version_bot": VERSION_SISTEMA,
        "datos_base": df_base,
        "operaciones": df_operaciones,
        "resumen_anual": df_resumen_anual,
        "metricas": metricas,
        "diagnostico_regimen": diagnostico_regimen_tsv,
        "resumen_regimen": resumen_regimen,
    }


if __name__ == "__main__":
    resultados = ejecutar_bot()
    tabla_resumen_anual, tabla_detalle_operaciones = construir_tablas_salida(resultados)

    print(f"Version sistema: {resultados['version_bot']}\n")
    imprimir_tabla_tsv(
        [
            "Año",
            "Operaciones",
            "Beneficio neto €",
            "Ganadoras",
            "Perdedoras",
            "Win rate %",
            "Capital acumulado €",
            "Rentabilidad %",
            "Drawdown máx %",
        ],
        tabla_resumen_anual,
    )
    print()
    imprimir_tabla_tsv(
        [
            "Fecha entrada",
            "Fecha salida",
            "Modulo activo",
            "Señal entrada",
            "Precio entrada",
            "Precio salida",
            "Unidades",
            "Motivo salida",
            "Beneficio acumulado €",
            "Rentabilidad %",
            "Capital acumulado €",
            "Beneficio neto €",
            "Regimen vigente",
            "Motivo régimen",
            "Porcentaje capital usado",
            "Capital antes entrada €",
            "QQQ > SMA200",
            "Retorno 63",
            "Cruces SMA50",
        ],
        tabla_detalle_operaciones,
    )
