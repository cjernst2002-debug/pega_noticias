# filtro_IA.py
import json, re, os
from typing import List, Dict

# =============== HABILITADOR ===============
# Pon "Si" para usar la IA (llama a OpenAI) o "No" para saltarla.
Correr_codigo = "Si"
# ==========================================

VERBOSE = True
BATCH_SIZE = 60  # opcional: por estabilidad si hay muchas noticias

PROMPT_BASE = """
Eres un analista que clasifica noticias EXCLUSIVAMENTE desde la perspectiva de la entidad objetivo indicada en el item:

- Si 'tipo' = "empresa": la entidad objetivo es el campo 'empresa'.
- Si 'tipo' = "industria": la entidad objetivo es el/los sectores listados en 'industrias'.

DEVUELVE SOLO JSON con este formato:
[
  {"id": "...", "categoria": "ALTA|MEDIA|BAJA|NULA"}
]

Cada item de entrada provee:
- id
- titulo
- descripcion
- empresa
- industrias (lista de strings)
- es_empresa (bool)
- es_industria (bool)
- tipo ("empresa" | "industria")

REGLAS DE DECISIÓN (aplican SIEMPRE respecto de la entidad objetivo):

1) Foco en la entidad objetivo
   - Si el texto trata principalmente de OTRA entidad (p. ej., otra empresa) y la entidad objetivo aparece solo tangencialmente, clasifica BAJA o NULA.
   - Si la mención a la entidad objetivo es ambigua o por homónimos sin señales claras de vínculo, clasifica NULA.
   - Si la coincidencia con la entidad objetivo depende de una palabra ambigua (p. ej., "Andina") y el resto del texto apunta a otra entidad (p. ej., Codelco), clasifica NULA.

2) Umbrales por categoría (empresa)
   ALTA (empresa): hechos financieros/regulatorios de impacto DIRECTO y material para esa empresa:
   - resultados/FECU o guidance
   - M&A, OPA/OPV
   - emisiones de deuda/acciones, rating, hecho esencial
   - sanción/regulador, fallo/medida de autoridad que la afecte
   - hitos operacionales propios (entrada en operación, PPA relevante, adjudicación grande, suspensión faena, huelga crítica)
   MEDIA (empresa): hechos corporativos/operativos relevantes pero no transformacionales:
   - contratos relevantes, expansión/tiendas, inversiones no gigantes, integraciones, acuerdos comerciales significativos
   BAJA (empresa): menciones con relación débil o sin evidencia de impacto financiero claro.
   NULA (empresa): contenido ajeno (policial, deporte, farándula, cultura, otra empresa), aunque comparta palabras.

3) Umbrales por categoría (industria)
   ALTA (industria): cambios normativos/macroeconómicos/materiales que afecten sustancialmente al sector objetivo (p. ej., royalty, impuestos/tarifas sectoriales, regulación CMF/SEA/Coordinador, shocks de precios de insumos/energía con consecuencias plausibles amplias).
   MEDIA (industria): tendencias, licitaciones, proyectos o acuerdos sectoriales relevantes pero no transformacionales.
   BAJA (industria): notas sectoriales periféricas, reseñas o cifras sin señal de materialidad.
   NULA (industria): contenido ajeno al sector o meramente generalista.

4) Evidencia explícita
   - Prioriza expresiones como: “la empresa [objetivo] anunció / informó / obtuvo / firmó / fue sancionada / publicó resultados”.
   - No infieras magnitudes ni relaciones si no están en el texto.

Devuelve EXACTAMENTE la lista JSON (sin comentarios ni texto extra).
"""

def _extract_json(s: str) -> str:
    m = re.search(r'\[\s*\{.*?\}\s*\]', s, flags=re.S)
    return m.group(0) if m else s

def _chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def _normalize_cat(value) -> str:
    v = (value or "").strip().upper()
    if v in {"ALTA", "MEDIA", "BAJA", "NULA"}:
        return v
    return "NULA"

def classify_batch(items: List[Dict]) -> List[Dict]:
    """
    Recibe items con:
      { "id","titulo","descripcion","empresa","industrias","es_empresa","es_industria","tipo" }

    Devuelve:
      [ {"id": "...", "categoria": "ALTA|MEDIA|BAJA|NULA|SIN CLASIFICAR"}, ... ]
    """
    if not items:
        return []

    # Switch maestro o falta de key: no llamar a OpenAI
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if Correr_codigo.strip().lower() != "si" or not api_key:
        if VERBOSE:
            msg = "IA deshabilitada (Correr_codigo != 'Si')" if Correr_codigo.strip().lower() != "si" else "Sin OPENAI_API_KEY en entorno"
            print(f"⚙️  {msg}: 'SIN CLASIFICAR' para todos.", flush=True)
        return [{"id": it["id"], "categoria": "SIN CLASIFICAR"} for it in items]

    try:
        from openai import OpenAI
    except Exception as e:
        print("❌ Falta paquete openai:", e, flush=True)
        return [{"id": it["id"], "categoria": "SIN CLASIFICAR"} for it in items]

    client = OpenAI(api_key=api_key)
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    results: Dict[str, str] = {}

    for batch in _chunk(items, BATCH_SIZE):
        # Payload compacto y directo
        casos = []
        for it in batch:
            casos.append({
                "id": it.get("id", ""),
                "empresa": it.get("empresa", ""),
                "industrias": it.get("industrias", []) or [],
                "es_empresa": bool(it.get("es_empresa", False)),
                "es_industria": bool(it.get("es_industria", False)),
                "tipo": it.get("tipo", "empresa"),
                "titulo": it.get("titulo", "") or "",
                "descripcion": it.get("descripcion", "") or "",
            })

        user_payload = "Clasifica estas noticias (JSON de entrada):\n" + json.dumps(casos, ensure_ascii=False)

        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": PROMPT_BASE.strip()},
                    {"role": "user", "content": user_payload}
                ],
                temperature=0,
            )
            raw = (resp.choices[0].message.content or "").strip()
            raw = _extract_json(raw)
            data = json.loads(raw)

            for c in data:
                _id = c.get("id")
                cat = _normalize_cat(c.get("categoria"))
                if _id:
                    results[_id] = cat

        except Exception as e:
            if VERBOSE:
                print("❌ Error clasificando con OpenAI:", e, flush=True)
            for it in batch:
                # Sin romper el flujo: marcamos como SIN CLASIFICAR
                results[it["id"]] = "SIN CLASIFICAR"

    # Arma salida en orden de entrada
    out = []
    for it in items:
        out.append({
            "id": it["id"],
            "categoria": results.get(it["id"], "SIN CLASIFICAR")
        })
    return out