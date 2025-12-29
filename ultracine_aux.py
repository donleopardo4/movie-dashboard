# ultracine_aux.py
import re
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# En la home de Ultracine figuran como endpoints (comentados en el HTML):
# https://www.ultracine.com/webservices/services/json/wsHomeTopMovies.php?token=...&cty_id=ar&limit=10
# https://www.ultracine.com/webservices/services/json/wsHomeTopMovies03.php?token=...&cty_id=ar&limit=20

DEFAULT_TOKEN = "c4ca4238a0b923820dcc509a6f75849b"  # aparece en el HTML público
DEFAULT_CTY_ID = "ar"

def title_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    # Normalización simple sin dependencias extra
    s = (
        s.replace("á", "a").replace("é", "e").replace("í", "i")
        .replace("ó", "o").replace("ú", "u").replace("ü", "u")
        .replace("ñ", "n")
    )
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()

def _safe_int(x) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, int):
        return x
    s = str(x).strip()
    s = s.replace(".", "").replace(",", "")  # 49.988 / 49,988
    if not s.isdigit():
        return None
    return int(s)

def _try_parse_json(text: str):
    # A veces devuelven JSON puro, a veces envuelven
    try:
        return json.loads(text)
    except Exception:
        # intento: encontrar primer { ... } o [ ... ]
        m = re.search(r"(\{.*\}|\[.*\])", text, flags=re.S)
        if not m:
            return None
        try:
            return json.loads(m.group(1))
        except Exception:
            return None

def fetch_ultracine_top(token: str = DEFAULT_TOKEN, cty_id: str = DEFAULT_CTY_ID, limit: int = 20, timeout: int = 25) -> Tuple[List[Dict], Optional[str]]:
    """
    Devuelve lista de items: {title, title_key, publico, acumulado, source_url}
    Si falla, devuelve ([], error_str).
    """
    urls = [
        f"https://www.ultracine.com/webservices/services/json/wsHomeTopMovies03.php?token={token}&cty_id={cty_id}&limit={limit}",
        f"https://www.ultracine.com/webservices/services/json/wsHomeTopMovies.php?token={token}&cty_id={cty_id}&limit={min(limit,10)}",
    ]

    last_err = None
    for url in urls:
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "movie-alerts/1.0"})
            r.raise_for_status()
            data = _try_parse_json(r.text)
            if not data:
                last_err = f"Ultracine: respuesta sin JSON parseable ({url})"
                continue

            # Estructuras posibles: lista directa / dict con "data" / dict con "result"
            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for k in ("data", "result", "results", "top", "movies"):
                    if k in data and isinstance(data[k], list):
                        items = data[k]
                        break
                if items is None:
                    # a veces viene con claves numéricas
                    if all(isinstance(v, dict) for v in data.values()):
                        items = list(data.values())

            if not items:
                last_err = f"Ultracine: JSON sin lista de items ({url})"
                continue

            out = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                # claves típicas vistas en servicios similares: title/titulo/name, publico/público, acumulado
                t = it.get("Título") or it.get("Titulo") or it.get("titulo") or it.get("title") or it.get("name") or it.get("movie") or it.get("pelicula")
                if not t:
                    continue

                publico = (
                    it.get("Público") or it.get("Publico") or it.get("publico") or
                    it.get("tickets") or it.get("attendance") or it.get("audience")
                )
                acumulado = (
                    it.get("Acumulado") or it.get("acumulado") or it.get("total") or
                    it.get("total_tickets") or it.get("cume") or it.get("cumulative")
                )

                out.append({
                    "title": str(t).strip(),
                    "title_key": title_key(str(t)),
                    "publico": _safe_int(publico),
                    "acumulado": _safe_int(acumulado),
                    "source_url": url,
                })

            if out:
                return out, None

            last_err = f"Ultracine: lista vacía tras normalizar ({url})"
        except Exception as e:
            last_err = f"Ultracine error: {type(e).__name__}: {e}"

    return [], last_err


def best_match_ultracine(ultra_items: List[Dict], movie_title: str) -> Optional[Dict]:
    """
    Match simple por title_key exacta; si no, fallback por contención.
    """
    k = title_key(movie_title)
    if not k:
        return None
    exact = [x for x in ultra_items if x.get("title_key") == k]
    if exact:
        return exact[0]

    # fallback: contención (para títulos con subtítulos)
    cand = []
    for x in ultra_items:
        uk = x.get("title_key") or ""
        if not uk:
            continue
        if k in uk or uk in k:
            cand.append((abs(len(uk) - len(k)), x))
    if not cand:
        return None
    cand.sort(key=lambda t: t[0])
    return cand[0][1]
