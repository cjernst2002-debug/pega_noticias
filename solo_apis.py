# solo_apis.py
import re
import html
import unicodedata
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import sys
import os

# ===== IA: módulo externo =====
# Debe existir filtro_IA.py con classify_batch(inputs) -> [{"id": ..., "categoria": ...}, ...]
from filtro_IA import classify_batch

# ===================== CONFIGURACIÓN =====================
# <- newsapi.ai / Event Registry
ER_API_KEY = os.getenv("ER_API_KEY", "").strip()

# Fuentes a consultar (por host) usando Event Registry
ER_SOURCES = ["df.cl", "latercera.com", "emol.com"]

# Mapeos y límites
ALLOWED_DOMAINS = {
    "df.cl", "latercera.com", "emol.com",
    "www.df.cl", "www.latercera.com", "www.emol.com",
    "amp.latercera.com"
}
DOMAIN_DISPLAY = {
    "df.cl": "Diario Financiero",
    "latercera.com": "La Tercera",
    "emol.com": "EMOL",
}

# Límite por empresa y ventana
DOMAIN_LIMIT = 100
HOURS_BACK = 24  # será sobrescrito dinámicamente en run_once()

# Cantidad máxima a pedirle a ER por fuente
ER_MAX_ITEMS_RAW = 1000  # ↑ techo alto para no cortar recall

# Depuración
DEBUG_SUMMARY = True

# Zona horaria Chile
CL_TZ = ZoneInfo("America/Santiago")

# ============== CORREO ==============
REMITENTE     = os.getenv("REMITENTE", "").strip()
DESTINATARIO  = os.getenv("DESTINATARIO", "").strip()
DESTINATARIO2 = os.getenv("DESTINATARIO2", "").strip()  # ← segundo destinatario opcional
APP_PASSWORD  = os.getenv("APP_PASSWORD", "").strip()   # Gmail: Contraseña de aplicación

RECIPIENTS = [e for e in [DESTINATARIO, DESTINATARIO2] if e]

if not REMITENTE or not APP_PASSWORD or not RECIPIENTS:
    print("Faltan REMITENTE, APP_PASSWORD o DESTINATARIO/DESTINATARIO2. Configúralos como Secrets en GitHub.")
    sys.exit(1)

# ===================== EMPRESAS (PEGA TU LISTA) =====================
# Pega aquí tu lista real de empresas:
empresas = [
    "Clínica Indisa S.A.", "PAZ Corp S.A.", "SAAM S.A.", "Socovesa S.A.", "Watts S.A.",
    "Hortifrut S.A.", "Empresas Iansa S.A.", "Embonor S.A.", "Inversiones Lipigas S.A.",
    "Cristalerías de Chile S.A.", "Multi X S.A.", "Besalco S.A.", "Empresas Gasco S.A.",
    "Salmones Camanchaca S.A.", "Blumar S.A.", "Compañía Pesquera Camanchaca S.A.",
    "Enlasa Energía Llaima S.A.", "Tricot S.A.", "Puerto Ventanas S.A.", "Cintac S.A.",
    "Forus S.A.", "Ingevec S.A.", "Moller y Pérez-Cotapos S.A.", "SalfaCorp S.A.",
    "SMU S.A.", "ZOFRI S.A.", "Hites S.A.", "Grupo Security S.A.", "SONDA S.A.",
    "Clínica Las Condes S.A.", "Ripley Corp S.A.", "Inmobiliaria Manquehue S.A.",
    "Empresas La Polar S.A.", "Masisa S.A.", "Enjoy S.A.", "Embotelladora Andina S.A.",
    "Compañía de Cervecerías Unidas S.A.", "Viña Concha y Toro S.A.", "Cencosud Shopping S.A.",
    "Parque Arauco S.A.", "Mallplaza S.A.", "CAP S.A.", "Enaex S.A.",
    "Inversiones La Construcción S.A.", "Sigdo Koppers S.A.", "Banco Santander Chile",
    "Banco de Chile", "Banco de Crédito e Inversiones", "Itaú Corpbanca",
    "Aguas Andinas S.A.", "Engie Energía Chile S.A.", "Colbún S.A.", "Enel Chile S.A.",
    "Enel Américas S.A.", "Empresas Copec S.A.", "Empresas CMPC S.A.",
    "LATAM Airlines Group S.A.", "Sociedad Química y Minera de Chile S.A.", "Quiñenco S.A.",
    "Compañía Sudamericana de Vapores S.A.", "Cencosud S.A.", "S.A.C.I. Falabella"
]
# ----------------- ALIAS (PEGA TUS ALIAS) --------------------
EMPRESA_ALIASES = {
    "Clínica Indisa S.A.": ["Clínica Indisa", "Clinica Indisa", "INDISA", "Instituto de Diagnóstico", "Instituto de Diagnostico",
                            "Clínica Indisa S.A.", "Clinica Indisa S.A."],
    "PAZ Corp S.A.": ["PAZ Corp", "PAZ", "PAZCorp", "Paz Corp S.A."],
    "SAAM S.A.": ["SAAM", "Sociedad Matriz SAAM", "Sociedad Matriz SAAM S.A.", "SM SAAM", "SMSAAM"],
    "Socovesa S.A.": ["Socovesa", "Empresa Constructora Socovesa", "Inmobiliaria Socovesa", "Constructora Socovesa"],
    "Watts S.A.": ["Watts", "Watt's", "WATTS"],
    "Hortifrut S.A.": ["Hortifrut", "HF", "HFRUT"],
    "Empresas Iansa S.A.": ["Iansa", "Empresas Iansa", "EISA"],
    "Embonor S.A.": ["Embonor", "Embonor-B", "Embonor Serie B"],
    "Inversiones Lipigas S.A.": ["Lipigas", "Inversiones Lipigas", "LipiAndes"],
    "Cristalerías de Chile S.A.": ["Cristalerías de Chile", "Cristalerias de Chile", "CristalChile", "Cristales", "Cristalerías", "Cristalerias"],
    "Multi X S.A.": ["Multi X", "Multiexport Foods", "Multiexport", "MULTI X", "Multiexport S.A."],
    "Besalco S.A.": ["Besalco"],
    "Empresas Gasco S.A.": ["Gasco", "Empresas Gasco", "Gasco GLP", "Gasco GLP S.A."],
    "Salmones Camanchaca S.A.": ["Salmones Camanchaca", "Salmocam"],
    "Blumar S.A.": ["Blumar", "Blumar Seafoods", "Blumar Seafoods S.A."],
    "Compañía Pesquera Camanchaca S.A.": ["Compañía Pesquera Camanchaca", "Compania Pesquera Camanchaca", "Camanchaca", "Pesquera Camanchaca", "Pesquera Camanchaca S.A."],
    "Enlasa Energía Llaima S.A.": ["Enlasa", "ENLASA", "Energía Llaima", "Energia Llaima", "Llaima"],  # ⚠
    "Tricot S.A.": ["Tricot", "Tricot S.A."],
    "Puerto Ventanas S.A.": ["Puerto Ventanas", "PVSA", "Ventanas"],
    "Cintac S.A.": ["Cintac", "Cintac S.A."],
    "Forus S.A.": ["Forus", "Forus Chile"],
    "Ingevec S.A.": ["Ingevec", "Constructora Ingevec"],
    "Moller y Pérez-Cotapos S.A.": ["Moller y Pérez-Cotapos", "Moller & Pérez-Cotapos", "Moller y Perez-Cotapos", "Moller & Perez-Cotapos", "MPC",
                                    "Moller Pérez-Cotapos", "Moller Perez Cotapos"],
    "SalfaCorp S.A.": ["SalfaCorp", "Salfa", "Salfa Corp"],
    "SMU S.A.": ["SMU", "Unimarc (grupo SMU)", "Unimarc", "Alvi", "Mayorista 10"],  # ⚠
    "ZOFRI S.A.": ["ZOFRI", "Zona Franca de Iquique"],
    "Hites S.A.": ["Hites", "Banco Hites"],
    "Grupo Security S.A.": ["Grupo Security", "Security", "Banco Security", "Inversiones Security", "Vida Security", "Valores Security"],
    "SONDA S.A.": ["SONDA"],
    "Clínica Las Condes S.A.": ["Clínica Las Condes", "Clinica Las Condes", "CLC", "Clínica Las Condes S.A."],
    "Ripley Corp S.A.": ["Ripley", "Empresas Ripley", "Ripley Corp", "Banco Ripley"],  # ⚠
    "Inmobiliaria Manquehue S.A.": ["Manquehue", "Inmobiliaria Manquehue", "Manquehue S.A."],
    "Empresas La Polar S.A.": ["La Polar", "Empresas La Polar", "LaPolar", "La Polar S.A."],
    "Masisa S.A.": ["Masisa"],
    "Enjoy S.A.": ["Enjoy", "Enjoy Casinos"],
    "Embotelladora Andina S.A.": ["Andina", "Coca-Cola Andina", "Andina B", "Andina A", "Coca Cola Andina", "Andina S.A."],
    "Compañía de Cervecerías Unidas S.A.": ["CCU", "Compañía de Cervecerías Unidas", "Compania de Cervecerias Unidas", "Cervecerías Unidas", "Cervecerias Unidas",
                                            "Compañía Cervecerías Unidas", "Compania Cervecerias Unidas"],
    "Viña Concha y Toro S.A.": ["Concha y Toro", "Viña Concha y Toro", "VCT", "Viña Concha y Toro S.A.", "Concha y Toro S.A."],
    "Cencosud Shopping S.A.": ["Cencosud Shopping", "Cencosud Malls", "Centros Comerciales Sudamericanos", "Cencomalls", "Cencosud Malls S.A."],
    "Parque Arauco S.A.": ["Parque Arauco", "PARAUCO", "Parauco", "Grupo Parque Arauco"],
    "Mallplaza S.A.": ["Mallplaza", "Mall Plaza", "Plaza S.A.", "Mall Plaza S.A.", "Grupo Mallplaza"],
    "CAP S.A.": ["CAP", "Compañía de Acero del Pacífico", "Compania de Acero del Pacifico",
                 "Huachipato", "Siderúrgica Huachipato", "CMP", "Compañía Minera del Pacífico", "Compania Minera del Pacifico"],  # ⚠ Huachipato (deporte)
    "Enaex S.A.": ["Enaex", "ENAEX", "Prillex", "Prillex América"],
    "Inversiones La Construcción S.A.": ["Inversiones La Construcción", "Inversiones La Construccion", "ILC", "Grupo ILC"],
    "Sigdo Koppers S.A.": ["Sigdo Koppers", "SK", "SKC", "SKBergé", "SK Bergé"],
    "Banco Santander Chile": ["Banco Santander", "Santander Chile", "Santander", "Santander Chile S.A."],
    "Banco de Chile": ["Banco de Chile", "Bco. de Chile", "Bco de Chile", "Banchile", "Banco Edwards", "Edwards",
                       "Banchile Inversiones", "Banchile AGF", "Banchile Corredores"],  # ⚠
    "Banco de Crédito e Inversiones": ["Banco de Crédito e Inversiones", "Banco de Credito e Inversiones", "Banco BCI", "BCI", "Bci", "Banco Bci",
                                       "Bci Seguros", "Bci Corredor de Bolsa"],  # ⚠
    "Itaú Corpbanca": ["Itaú Chile", "Itau Chile", "Banco Itaú", "Banco Itau", "Itaú Corpbanca", "Itau Corpbanca",
                       "Itaú", "Itau", "CorpBanca", "ItaúCorp", "ItauCorp", "Itaú Corpbanca S.A."],  # ⚠ Itaú genérico
    "Aguas Andinas S.A.": ["Aguas Andinas", "Aguas Andinas S.A."],
    "Engie Energía Chile S.A.": ["ENGIE Energía Chile", "ENGIE Energia Chile", "ENGIE Chile", "EECL", "E-CL"],
    "Colbún S.A.": ["Colbún", "Colbun", "Colbún S.A.", "Colbun S.A."],
    "Enel Chile S.A.": ["Enel Chile", "Enel-Chile", "Enel", "Enel Distribución", "Enel Generación"],  # ⚠
    "Enel Américas S.A.": ["Enel Américas", "Enel Americas", "ENELAM", "Enel Américas S.A."],
    "Empresas Copec S.A.": ["Empresas Copec", "Copec", "Copec S.A.", "Abastible", "Terpel", "Arauco"],  # ⚠ Arauco (confusión con Parque Arauco)
    "Empresas CMPC S.A.": ["CMPC", "Empresas CMPC", "La Papelera", "Softys", "Forestal Mininco"],
    "LATAM Airlines Group S.A.": ["LATAM", "LATAM Airlines", "LATAM Airlines Group", "LAN Airlines", "LAN Chile", "LAN"],
    "Sociedad Química y Minera de Chile S.A.": ["Sociedad Química y Minera de Chile", "Sociedad Quimica y Minera de Chile", "SQM", "Soquimich",
                                               "SQM Salar", "SQM Nitratos y Yodo", "SQM Lithium"],
    "Quiñenco S.A.": ["Quiñenco", "Quinenco", "Grupo Quiñenco"],
    "Compañía Sudamericana de Vapores S.A.": ["Compañía Sudamericana de Vapores", "Compania Sudamericana de Vapores", "CSAV", "CSAV S.A."],
    "Cencosud S.A.": ["Cencosud", "Grupo Cencosud", "Jumbo", "Santa Isabel", "Paris", "Easy"],  # ⚠ marcas
    "S.A.C.I. Falabella": ["Falabella", "SACI Falabella", "S.A.C.I. Falabella", "Grupo Falabella",
                           "Sodimac", "Homecenter", "Tottus", "Banco Falabella"],  # ⚠ marcas
}

# ===================== INDUSTRIAS (PEGA TUS LISTAS) =====================
INDUSTRIA_MUST_MATCH = False
INDUSTRIA_KEYWORDS = {
    "bancaria": [
        "hecho esencial cmf","resultados trimestrales","resultados 2t25","utilidad neta",
        "roe","margen de interes","nim","provisiones","cartera vencida","morosidad 90 dias",
        "colocaciones","aumento de capital","dividendo","colocacion de bonos","bono subordinado",
        "bono verde","emision 144a","rating fitch","rating moodys","standard and poors",
        "fusion bancaria","adquisicion banco","portabilidad financiera","basilea iii",
        "indice de capital","sancion cmf","ciberataque bancario","plan estrategico banco"
    ],
    "energia": [
        "licitacion de suministro","adjudicacion suministro","ppa","precio nudo","precio spot",
        "coordinador electrico nacional","cen","declaracion de indisponibilidad","mantenimiento programado",
        "curtailment","congestion","bess","almacenamiento de baterias","linea 220 kv","linea 500 kv",
        "subestacion","puesta en servicio","entrada en operacion","hidrogeno verde",
        "parque solar","parque eolico","central hidroelectrica","descarbonizacion","cierre termo",
        "tarifa de distribucion","vad","netbilling","plan de expansion de transmision","eia ingresado",
        "rca aprobada","resolucion sea"
    ],
    "mineria": [
        "eia ingresado","rca aprobada","sernageomin","cochilco","estudio de prefactibilidad",
        "estudio de factibilidad","capex minero","plan minero","produccion de cobre","produccion de litio",
        "catodos","concentrado","contrato offtake","oferta vinculante","planta desaladora","relaves",
        "expansion de mina","suspension de faena","accidente fatal","huelga minera","negociacion colectiva",
        "royalty minero","plan de cierre","permisos sectoriales","inicio de construccion",
        "comisionamiento","mou con codelco","joint venture minero","ppa para faena"
    ],
    "retail": [
        "resultados 2t25","resultados trimestrales","ventas mismas tiendas","same store sales","sss",
        "ebitda retail","margen bruto","inventarios","apertura de tienda","cierre de tienda",
        "reorganizacion judicial","centro de distribucion","omnicanalidad","ecommerce","marketplace",
        "programa de fidelizacion","cyberday","black friday","ticket promedio","trafico en tiendas",
        "capex de aperturas","guidance de ventas","acuerdo con proveedor"
    ],
    "inmobiliario": [
        "preventas","venta en verde","permiso de edificacion","recepcion final","multifamily",
        "build to rent","btr","arriendo residencial","vacancia residencial","absorcion","stock de viviendas",
        "uf m2","tasacion","paralizacion de proyecto","financiamiento hipotecario","alza tasas hipotecarias",
        "subsidio ds19","costo de construccion","plan maestro","cambio de uso de suelo","loteo",
        "plan regulador","joint venture inmobiliario"
    ],
    "construccion": [
        "licitacion mop","adjudicacion mop","contrato epc","estado de pago","reajuste polinomico",
        "modificacion contractual","termino anticipado de contrato","avance fisico","inicio de obras",
        "paralizacion de obras","arbitraje de obra","recepcion provisoria","recepcion definitiva",
        "accidente laboral","insolvencia constructora","liquidacion","consorcio constructor",
        "oferta economica","garantia de fiel cumplimiento"
    ],
    "salud": [
        "superintendencia de salud","isapres fallo suprema","tabla de factores","copagos",
        "convenio con fonasa","habilitacion sanitaria","apertura de clinica","expansion hospitalaria",
        "licitacion servicios de salud","adquisicion de clinica","compra de prestador","camas criticas",
        "capex clinico","acreditacion en salud","contrato con aseguradoras","telemedicina convenio",
        "sancion superintendencia de salud","ciberataque a clinica","brecha de datos pacientes"
    ],
    "tecnologia": [
        "ciberataque","ransomware","filtracion de datos","data center","region de datos","cloud publica",
        "contrato cloud","hiperescalador","hiperscaler","ia generativa","modelo de lenguaje",
        "semiconductores","centro de desarrollo","subtel licitacion 5g","bloques de espectro",
        "autorizacion subtel","fintech licencia cmf","proveedor de servicios de pago",
        "open banking","sandbox regulatorio","levantamiento de capital","ronda serie a","ronda serie b",
        "alianza tecnologica","despliegue de fibra optica"
    ],
    "infraestructura": [
        "concesion vial","concesion aeroportuaria","concesion portuaria","licitacion de concesion",
        "adjudicacion de concesion","oferta economica vpi","vpi","inicio de obras","avance de obras",
        "recepcion provisoria","recepcion definitiva","tarifa de peaje","alza de peajes",
        "mop direccion de concesiones","contrato de concesion","modificacion de contrato",
        "obras adicionales","puente","tunel","ferrocarril","efe","linea de metro","embalse",
        "obra hidraulica"
    ],
    "malls": [
        "gla","superficie arrendable","ocupacion de malls","vacancia de malls","ventas de arrendatarios",
        "tenant sales","renta variable","canon de arriendo","arriendo variable","tenant mix",
        "apertura de tienda ancla","tienda ancla","expansion de mall","remodelacion de mall",
        "footfall","trafico peatonal","noi","ingreso operacional neto","cap rate","revaluacion ifrs",
        "parque comercial","strip center","centro comercial abierto"
    ]
}

NEGATIVOS_POR_INDUSTRIA = {
    "bancaria": ["banco de sangre","banco de alimentos"],
    "mineria": ["mineria de datos","minecraft"],
    "tecnologia": ["videojuego","rumor de lanzamiento"],
    "malls": ["mal"]  # errores ortográficos frecuentes
}

# ----------------- HELPERS -----------------
def _normalize_domain(host: str) -> str:
    host = (host or "").lower()
    return host[4:] if host.startswith("www.") else host

def _host_ok(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    host = _normalize_domain(host)
    return any(host.endswith(d) for d in ALLOWED_DOMAINS)

def strip_html(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text or "")).strip()

def _iso_to_cl_no_tz(ts: str) -> str:
    try:
        dt = datetime.fromisoformat((ts or "").replace("Z", "+00:00"))
        return dt.astimezone(CL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts or ""

def _hours_ago_label(ts: str) -> str:
    try:
        dt = datetime.fromisoformat((ts or "").replace("Z", "+00:00")).astimezone(CL_TZ)
        now_cl = datetime.now(CL_TZ)
        delta = now_cl - dt
        hours = int(round(delta.total_seconds() / 3600.0))
        if hours < 0:
            hours = 0
        return f"hace {hours} hrs"
    except Exception:
        return "hace ? hrs"

def _fecha_mas_relativa(ts: str) -> str:
    base = _iso_to_cl_no_tz(ts)
    rel = _hours_ago_label(ts)
    return f"{base} ({rel})"

SPANISH_MONTHS = [
    "enero","febrero","marzo","abril","mayo","junio",
    "julio","agosto","septiembre","octubre","noviembre","diciembre"
]
def _fecha_larga_cl() -> str:
    today_cl = datetime.now(CL_TZ).date()
    return f"{today_cl.day} de {SPANISH_MONTHS[today_cl.month - 1]} {today_cl.year}"

def _subject_for_today(prefix: str) -> str:
    return f"{prefix} – {_fecha_larga_cl()}"

def normalizar_texto(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()

def contiene_empresa(titulo: str, descripcion: str, empresa: str, aliases: list[str]) -> bool:
    texto_norm = normalizar_texto((titulo or "") + " " + (descripcion or ""))
    patrones = [empresa] + (aliases or [])
    for p in patrones:
        p_norm = normalizar_texto(p)
        if not p_norm:
            continue
        if re.search(rf"\b{re.escape(p_norm)}\b", texto_norm):
            return True
    return False

def detectar_industrias(titulo: str, descripcion: str) -> list[str]:
    """
    Devuelve lista de industrias (keys de INDUSTRIA_KEYWORDS) que matchean
    con titulo/descripcion, aplicando filtros negativos por industria.
    """
    t = normalizar_texto((titulo or "") + " " + (descripcion or ""))
    hits = []
    for industria, keys in (INDUSTRIA_KEYWORDS or {}).items():
        match = False
        for k in keys:
            k = (k or "").strip().lower()
            if not k:
                continue
            if re.search(rf"\b{re.escape(k)}\b", t):
                match = True
                break
        if match:
            negs = (NEGATIVOS_POR_INDUSTRIA or {}).get(industria, [])
            if any(re.search(rf"\b{re.escape(n)}\b", t) for n in negs):
                continue
            hits.append(industria)
    return hits

# ===== Helpers para consolidación y formato de clasificación =====
CAT_RANK = {"ALTA": 3, "MEDIA": 2, "BAJA": 1, "SIN CLASIFICAR": 0}

def _merge_cat(prev: str | None, new: str | None) -> str:
    """Conserva la categoría de mayor severidad."""
    p = (prev or "SIN CLASIFICAR").upper()
    n = (new or "SIN CLASIFICAR").upper()
    return n if CAT_RANK.get(n, -1) >= CAT_RANK.get(p, -1) else p

def _iso_to_dt(ts: str):
    try:
        return datetime.fromisoformat((ts or "").replace("Z", "+00:00"))
    except Exception:
        return None

def _format_tags(empresas: dict[str, str], industrias: dict[str, str]) -> str:
    """Devuelve 'e1 (ALTA), e2 (MEDIA); i1 (BAJA)' según existan empresas e industrias."""
    def _sorted_items(dd: dict[str, str]):
        # Ordena por severidad desc y luego alfabético
        return sorted(dd.items(), key=lambda kv: (-CAT_RANK.get(kv[1], -1), kv[0].lower()))
    parts = []
    if empresas:
        parts.append(", ".join([f"{name} ({cat})" for name, cat in _sorted_items(empresas)]))
    if industrias:
        parts.append(", ".join([f"{name} ({cat})" for name, cat in _sorted_items(industrias)]))
    return "; ".join(parts) if parts else "n/a"

def _group_and_collapse_by_url(noticias: list[dict], klass_map: dict[str, str]) -> list[dict]:
    """
    1) Elimina ítems NULA.
    2) Agrupa por URL consolidando empresas e industrias con su mejor categoría.
    Devuelve lista de grupos con campos: titulo, descripcion, fuente, fecha, url, empresas{}, industrias{}.
    """
    groups: dict[str, dict] = {}

    for n in noticias:
        cat = (klass_map.get(n["id"], "SIN CLASIFICAR") or "SIN CLASIFICAR").upper()
        if cat == "NULA":
            continue  # descartar completamente el ítem

        url = n.get("url") or ""
        if not url:
            continue

        g = groups.get(url)
        if not g:
            g = {
                "url": url,
                "titulo": n.get("titulo", "") or "",
                "descripcion": strip_html(n.get("descripcion", "") or ""),
                "fuente": n.get("fuente", "") or "",
                "fecha": n.get("fecha", "") or "",
                "empresas": {},    # { nombre_empresa: categoria_mejor }
                "industrias": {},  # { nombre_industria: categoria_mejor }
            }
            groups[url] = g
        else:
            # Rellenar campos vacíos con información disponible
            if not g["titulo"] and n.get("titulo"):
                g["titulo"] = n["titulo"]
            if not g["descripcion"] and n.get("descripcion"):
                g["descripcion"] = strip_html(n["descripcion"])
            if not g["fuente"] and n.get("fuente"):
                g["fuente"] = n["fuente"]
            if not g["fecha"] and n.get("fecha"):
                g["fecha"] = n["fecha"]

        # Consolidar empresas/industrias con la mejor categoría
        if n.get("tipo") == "empresa":
            name = (n.get("empresa") or "").strip()
            if name:
                g["empresas"][name] = _merge_cat(g["empresas"].get(name), cat)
        elif n.get("tipo") == "industria":
            ind = n.get("industria") or (n.get("industrias", [None]) or [None])[0]
            if ind:
                g["industrias"][ind] = _merge_cat(g["industrias"].get(ind), cat)

    # A lista y ordenar por fecha desc
    out = list(groups.values())
    out.sort(key=lambda x: (_iso_to_dt(x.get("fecha") or "") or datetime.min), reverse=True)
    return out

# --------- Sesión, cachés y stats ----------
RUN_STATS = {"counts": defaultdict(int), "errors": defaultdict(set)}
_ER_ARTICLES_CACHE_BY_HOST: dict[str, list] = {}  # cache separado por fuente

# ============== Ventana dinámica según hora Chile ==============
def _compute_hours_back(now_cl: datetime | None = None) -> int:
    """
    08:xx -> 14 horas
    18:xx -> 10 horas
    Otros momentos:
      - AM -> 14h
      - PM -> 10h
    Permite override con env var HOURS_BACK_OVERRIDE (int).
    """
    override = os.getenv("HOURS_BACK_OVERRIDE", "").strip()
    if override.isdigit():
        return max(1, int(override))

    now_cl = now_cl or datetime.now(CL_TZ)
    hr = now_cl.hour
    if hr == 8:
        return 14
    if hr == 18:
        return 10
    return 14 if hr < 12 else 10

# ===================== FETCH: Event Registry (por host) =====================
def _parse_er_dt_to_iso(dt_str: str) -> str:
    try:
        s = (dt_str or "").replace(" ", "T")
        # Event Registry suele entregar "YYYY-MM-DD HH:MM:SS"
        # Lo convertimos a ISO con Z
        if s.endswith("Z"):
            return s
        return s + "Z"
    except Exception:
        return dt_str or ""

def _parse_er_dt_to_utc_datetime(dt_str: str):
    try:
        iso = _parse_er_dt_to_iso(dt_str)
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _fetch_er_articles_for_host(host: str) -> list:
    """
    Descarga artículos de un host (df.cl, latercera.com, emol.com) vía Event Registry
    SOLO UNA VEZ y cachea.
    """
    base = _normalize_domain(host)
    if base in _ER_ARTICLES_CACHE_BY_HOST:
        return _ER_ARTICLES_CACHE_BY_HOST[base]

    if not ER_API_KEY:
        RUN_STATS["errors"][base].add("Sin ER_API_KEY: fuente desactivada")
        _ER_ARTICLES_CACHE_BY_HOST[base] = []
        return _ER_ARTICLES_CACHE_BY_HOST[base]

    try:
        from eventregistry import EventRegistry, QueryArticlesIter
    except ImportError:
        RUN_STATS["errors"][base].add("Falta package 'eventregistry' (pip install eventregistry)")
        _ER_ARTICLES_CACHE_BY_HOST[base] = []
        return _ER_ARTICLES_CACHE_BY_HOST[base]

    # Ventana local (Chile)
    end_dt_local = datetime.now(CL_TZ)
    start_dt_local = end_dt_local - timedelta(hours=HOURS_BACK)
    dateStart = start_dt_local.strftime("%Y-%m-%d")
    dateEnd = end_dt_local.strftime("%Y-%m-%d")

    # Umbrales UTC para corte por ventana
    start_cutoff_utc = start_dt_local.astimezone(timezone.utc)
    end_cutoff_utc = end_dt_local.astimezone(timezone.utc)

    try:
        er = EventRegistry(apiKey=ER_API_KEY)
        src_uri = er.getSourceUri(base)
        if not src_uri:
            # Intento con "www." si falla
            src_uri = er.getSourceUri(f"www.{base}")
        if not src_uri:
            RUN_STATS["errors"][base].add(f"Event Registry no encontró sourceUri para {base}")
            _ER_ARTICLES_CACHE_BY_HOST[base] = []
            return _ER_ARTICLES_CACHE_BY_HOST[base]

        # Para maximizar recall, no fijamos lang
        q = QueryArticlesIter(
            sourceUri=src_uri,
            # lang="spa",
            dateStart=dateStart,
            dateEnd=dateEnd,
        )

        collected, seen = [], set()
        OLD_STREAK_BREAK = 10_000
        old_streak = 0
        seen_in_window = False

        for art in q.execQuery(er, maxItems=ER_MAX_ITEMS_RAW):
            dt_utc = _parse_er_dt_to_utc_datetime(art.get("dateTime"))
            if dt_utc is not None:
                if dt_utc < start_cutoff_utc:
                    if seen_in_window:
                        old_streak += 1
                        if old_streak >= OLD_STREAK_BREAK:
                            break
                    continue
                else:
                    old_streak = 0
                    seen_in_window = True
                    if dt_utc > end_cutoff_utc:
                        continue

            url = art.get("url")
            if not url or url in seen:
                continue

            try:
                url_host = _normalize_domain(urlparse(url).netloc.lower())
            except Exception:
                continue

            # filtro estricto por host efectivo
            if not (url_host == base or url_host.endswith("." + base)):
                continue

            seen.add(url)
            collected.append({
                "title": art.get("title") or "",
                "description": (art.get("body") or "")[:600],
                "publishedAt": _parse_er_dt_to_iso(art.get("dateTime") or ""),
                "url": url,
            })

        # Ordenar por fecha desc
        def key_dt(x):
            try:
                return datetime.fromisoformat((x.get("publishedAt") or "").replace("Z", "+00:00"))
            except Exception:
                return datetime.min

        collected.sort(key=key_dt, reverse=True)
        _ER_ARTICLES_CACHE_BY_HOST[base] = collected
        if DEBUG_SUMMARY:
            print(f"[DEBUG] EventRegistry {base}: artículos cacheados = {len(collected)}", flush=True)
        return _ER_ARTICLES_CACHE_BY_HOST[base]

    except Exception as e:
        RUN_STATS["errors"][base].add(f"Event Registry falló: {e}")
        _ER_ARTICLES_CACHE_BY_HOST[base] = []
        return _ER_ARTICLES_CACHE_BY_HOST[base]

def _er_articles_all_sources() -> dict[str, list]:
    """
    Devuelve { base_domain: [articles...] } cacheados para todas las fuentes ER_SOURCES.
    """
    out = {}
    for host in ER_SOURCES:
        base = _normalize_domain(host)
        arts = _fetch_er_articles_for_host(base)
        out[base] = arts
    return out

# ===================== ORQUESTACIÓN =====================
def obtener_noticias() -> list[dict]:
    """
    Flujo:
      1) Descarga/caché Event Registry por cada host (DF, LT, EMOL)
      2) Para cada empresa, filtra y genera ítems:
         - Siempre 1 ítem de tipo "empresa" cuando hay match de empresa.
         - Además, 1 ítem de tipo "industria" POR CADA industria detectada.
    """
    all_news = []

    # 1) ER por fuente
    domain_buckets = _er_articles_all_sources()

    # 2) Filtrar por empresa y asignar IDs (empresa + múltiples industria)
    print(f"→ Filtrando noticias para {len(empresas)} empresas...", flush=True)
    for company_idx, empresa in enumerate(empresas, start=1):
        aliases = EMPRESA_ALIASES.get(empresa, [])
        item_seq = 0  # contador por empresa para respetar DOMAIN_LIMIT

        # Recorremos DF, LT y EMOL en ese orden
        for base_domain in ["df.cl", "latercera.com", "emol.com"]:
            arts = domain_buckets.get(base_domain, []) or []
            display = DOMAIN_DISPLAY.get(base_domain, base_domain)

            for a in arts:
                url = a.get("url") or ""
                if not url or not _host_ok(url):
                    continue

                title = a.get("title") or ""
                desc = a.get("description") or ""

                # Match por empresa
                match_emp = contiene_empresa(title, desc, empresa, aliases)
                if not match_emp:
                    continue

                # Detección de industrias
                inds = detectar_industrias(title, desc)

                # Si se exige match de industria global y no hay industrias, descartar
                if INDUSTRIA_MUST_MATCH and not inds:
                    continue

                # Campos comunes base
                base_item = {
                    "empresa_id": company_idx,
                    "empresa": empresa,
                    "titulo": title,
                    "fuente": display,
                    "fecha": a.get("publishedAt") or "",
                    "url": url,
                    "descripcion": desc,
                }

                # 1) ÍTEM EMPRESA
                item_seq += 1
                noticia_emp = {
                    **base_item,
                    "id": f"{company_idx}-{item_seq}-E",
                    "industrias": [],
                    "es_empresa": True,
                    "es_industria": False,
                    "tipo": "empresa",
                }
                all_news.append(noticia_emp)
                if item_seq >= DOMAIN_LIMIT:
                    break

                # 2) ÍTEMS INDUSTRIA, UNO POR CADA INDUSTRIA DETECTADA
                for ind in (inds or []):
                    item_seq += 1
                    noticia_ind = {
                        **base_item,
                        "id": f"{company_idx}-{item_seq}-I",
                        "industrias": [ind],
                        "industria": ind,
                        "es_empresa": False,
                        "es_industria": True,
                        "tipo": "industria",
                    }
                    all_news.append(noticia_ind)
                    if item_seq >= DOMAIN_LIMIT:
                        break

                if item_seq >= DOMAIN_LIMIT:
                    break

            if item_seq >= DOMAIN_LIMIT:
                break

    if DEBUG_SUMMARY:
        by_src = defaultdict(int)
        for n in all_news:
            by_src[n["fuente"]] += 1
        print("[DEBUG] Conteo por fuente:", dict(by_src), flush=True)

    return all_news

# ===================== COMPILAR REPORTE =====================
def compilar_reporte():
    print("📡 Descargando y filtrando noticias...", flush=True)
    noticias = obtener_noticias()
    print(f"✔ Noticias filtradas: {len(noticias)}", flush=True)

    # 1) Preparar inputs para IA
    ai_inputs = [
        {
            "id": n["id"],
            "titulo": n.get("titulo", ""),
            "descripcion": strip_html(n.get("descripcion", "")),
            "empresa": n.get("empresa", ""),
            "industrias": n.get("industrias", []) or [],
            "es_empresa": n.get("es_empresa", False),
            "es_industria": n.get("es_industria", False),
            "tipo": n.get("tipo", "empresa"),
        }
        for n in noticias
    ]

    # 2) Llamada a IA (respeta el switch en filtro_IA.py)
    print("🤖 Clasificando con IA...", flush=True)
    try:
        ai_results = classify_batch(ai_inputs)  # [{"id": ..., "categoria": ...}, ...]
    except Exception as e:
        print(f"[WARN] Falla en classify_batch: {e}. Se marcarán como 'SIN CLASIFICAR'.", flush=True)
        ai_results = [{"id": x["id"], "categoria": "SIN CLASIFICAR"} for x in ai_inputs]
    klass_map = {r["id"]: (r.get("categoria") or "SIN CLASIFICAR").upper() for r in ai_results}
    print("✔ Clasificación lista.", flush=True)

    # 3) Eliminar NULA y agrupar por URL consolidando etiquetas
    grupos = _group_and_collapse_by_url(noticias, klass_map)

    if DEBUG_SUMMARY:
        by_src = defaultdict(int)
        for g in grupos:
            by_src[g["fuente"]] += 1
        print("[DEBUG] Conteo por fuente (agrupado por URL):", dict(by_src), flush=True)

    # ===== HTML con estética mejorada (inline CSS para compatibilidad en email) =====
    html_parts = []
    html_parts.append("<html><body style='margin:0;padding:0;'>")
    html_parts.append(
        "<div style='background:#fafafa;padding:16px;font-family:Arial,Helvetica,sans-serif;color:#111;'>"
        "<div style='max-width:860px;margin:0 auto;background:#ffffff;border:1px solid #eaeaea;border-radius:8px;padding:20px;'>"
    )

    titulo_encabezado = f"Reporte de Noticias (últimas {HOURS_BACK} hrs) { _fecha_larga_cl() }"
    html_parts.append(
        f"<h2 style='margin:0 0 16px 0;font-size:18px;line-height:1.3;font-weight:700;'>{html.escape(titulo_encabezado)}</h2>"
    )

    html_parts.append("<ul style='list-style:none;padding:0;margin:0;'>")

    # Texto plano
    texto_lines = []

    for idx, g in enumerate(grupos, start=1):
        fecha_mostrar = _fecha_mas_relativa(g.get("fecha", "")) if g.get("fecha") else "(sin fecha)"
        desc_clean = (g.get("descripcion") or "").rstrip(" .")

        # Empresa/Industria consolidado con categorías
        ei_value = _format_tags(g.get("empresas", {}), g.get("industrias", {}))

        # ---------- TEXTO PLANO ----------
        if idx > 1:
            texto_lines.append("")  # línea en blanco extra entre noticias
        texto_lines.append(f"Empresa/Industria: {ei_value}")
        texto_lines.append(f"Título: {g.get('titulo', '')}")
        texto_lines.append(f"Fuente: {g.get('fuente', '')}")
        texto_lines.append(f"Fecha: {fecha_mostrar}")
        texto_lines.append(f"Descripción: {desc_clean}.")

        # ---------- HTML ----------
        li_border = "border-top:1px solid #eee;" if idx > 1 else ""
        html_parts.append(
            f"<li style='{li_border}padding:18px 0 20px 0;margin:0;'>"
            f"<div style='font-size:16px;font-weight:700;margin:0 0 8px 0;'>"
            f"Empresa/Industria: {html.escape(ei_value)}</div>"
            f"<div style='margin:0 0 6px 0;'>"
            f"<span style='font-weight:600;'>Título:</span> "
            f"<a href='{html.escape(g.get('url',''))}' target='_blank' rel='noopener noreferrer' style='color:#1155cc;text-decoration:none;'>"
            f"{html.escape(g.get('titulo',''))}</a></div>"
            f"<div style='margin:0 0 4px 0;'><span style='font-weight:600;'>Fuente:</span> {html.escape(g.get('fuente',''))}</div>"
            f"<div style='margin:0 0 6px 0;'><span style='font-weight:600;'>Fecha:</span> {html.escape(fecha_mostrar)}</div>"
            f"<div style='margin:0 0 8px 0;'><span style='font-weight:600;'>Descripción:</span> {html.escape(desc_clean)}.</div>"
            f"</li>"
        )

    html_parts.append("</ul>")

    # Errores, si los hay
    errores_existentes = {b: e for b, e in RUN_STATS["errors"].items() if e}
    if errores_existentes:
        html_parts.append(
            "<hr style='border:none;border-top:1px solid #eee;margin:20px 0;'>"
            "<h3 style='font-size:16px;margin:0 0 10px 0;'>⚠️ Errores durante la corrida</h3>"
            "<ul style='padding-left:18px;margin:0;'>"
        )
        for base, errs in sorted(errores_existentes.items()):
            for msg in sorted(errs):
                html_parts.append(f"<li style='margin:4px 0;'>{html.escape(base)}: {html.escape(msg)}</li>")
        html_parts.append("</ul>")

    html_parts.append("</div></div></body></html>")

    return "\n".join(texto_lines), "".join(html_parts)

# ===================== ENVIAR MAIL =====================
def enviar_mail(texto, cuerpo_html, remitente, destinatarios: list[str], password):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = _subject_for_today("Reporte de Noticias")
    msg["From"] = remitente
    msg["To"] = ", ".join(destinatarios)
    msg.attach(MIMEText(texto, "plain", "utf-8"))
    msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))

    # SMTP seguro (SSL 465). Si prefieres STARTTLS: usa puerto 587 y server.starttls()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(remitente, password)
        server.sendmail(remitente, destinatarios, msg.as_string())
    print("📨 Correo enviado con éxito", flush=True)

# ===================== MAIN =====================
def run_once():
    # Ajusta la ventana según hora de Chile
    global HOURS_BACK
    HOURS_BACK = _compute_hours_back(datetime.now(CL_TZ))
    print(f"⏱️ Ventana dinámica seleccionada: últimas {HOURS_BACK} horas (CLT).", flush=True)

    # Limpia cachés por si corres muchas veces seguidas
    _ER_ARTICLES_CACHE_BY_HOST.clear()

    texto, html_body = compilar_reporte()
    enviar_mail(texto, html_body, REMITENTE, RECIPIENTS, APP_PASSWORD)

if __name__ == "__main__":
    # Ejecuta una corrida única. (El agendamiento real lo hace GitHub Actions)
    run_once()