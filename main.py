# main.py
# Dashboard de estrenos argentinos (INCAA catálogo CSV) + métricas trailer (YouTube/Vimeo) + X + Ultracine (aux)
# Publica 3 páginas en GitHub Pages: index.html / proximos.html / estrenadas.html
# Envía un mail diario SOLO con el link (PAGES_URL).
#
# Requisitos:
#   pip install requests
#
# Archivos esperados:
#   - config.env  (en esta misma carpeta)
#   - publisher_github_pages.py  (en esta misma carpeta; te lo pasé antes)
#   - (opcional) incaa_historico_manual.csv  (manual, para INCAA acumulado)
#
# NOTA: este script incluye "migración" de tablas SQLite (agrega columnas si faltan)
#       para evitar errores tipo "no column named ...".

import os
import re
import csv
import json
import base64
import sqlite3
import smtplib
import requests
from io import StringIO
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, parse_qs

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from publisher_github_pages import publish_html


# =========================================================
# CONFIG / ENV
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "movie.db")

# Ventana de títulos a mostrar en el dashboard:
PAST_DAYS = 30
FUTURE_DAYS = 30

# Umbrales para marcar "ALERTA" (seguimiento manual recomendado)
ALERT_VIEWS_CHANGE_24H = 2000
ALERT_LIKES_CHANGE_24H = 150
ALERT_COMMENTS_CHANGE_24H = 50
ALERT_X_POSTS_CHANGE_24H = 30
ALERT_X_ENG_CHANGE_24H = 150

# Ultracine: usa endpoints públicos (home top semanal). Sirve si el título aparece en TOP.
ULTRACINE_TOKEN_DEFAULT = "c4ca4238a0b923820dcc509a6f75849b"
ULTRACINE_CTY_DEFAULT = "ar"
ULTRACINE_LIMIT = 20


def load_env():
    env_path = os.path.join(BASE_DIR, "config.env")
    if not os.path.exists(env_path):
        raise RuntimeError(f"No existe {env_path}. Crealo y agregá las variables.")
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if (not line) or line.startswith("#") or ("=" not in line):
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()


load_env()

# Catálogo INCAA (2 pestañas exportadas a CSV)
CSV_URLS = [os.getenv("INCAA_CSV_URL_1"), os.getenv("INCAA_CSV_URL_2")]
CSV_URLS = [u for u in CSV_URLS if u]
if not CSV_URLS:
    raise RuntimeError("Faltan INCAA_CSV_URL_1 / INCAA_CSV_URL_2 en config.env")

# INCAA histórico manual desactivado (no se usa)
INCAA_CSV_URL_1 = os.getenv("INCAA_CSV_URL_1", "").strip()
INCAA_CSV_URL_2 = os.getenv("INCAA_CSV_URL_2", "").strip()
# No levantar error si faltan

# APIs
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # opcional, pero recomendado
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")    # opcional

# GitHub Pages publish
GITHUB_OWNER = os.getenv("GITHUB_OWNER")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
PAGES_URL = os.getenv("PAGES_URL")

if not all([GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN, PAGES_URL]):
    raise RuntimeError("Faltan GITHUB_OWNER / GITHUB_REPO / GITHUB_TOKEN / PAGES_URL en config.env")

# Email (solo link)
EMAIL_TO = os.getenv("EMAIL_TO")
EMAIL_FROM = os.getenv("EMAIL_FROM")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

if not all([EMAIL_TO, EMAIL_FROM, SMTP_USER, SMTP_PASS]):
    raise RuntimeError("Faltan EMAIL_TO / EMAIL_FROM / SMTP_USER / SMTP_PASS en config.env")

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.7",
}


# =========================================================
# DB + MIGRACIONES
# =========================================================
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()


def table_columns(table_name: str):
    cur.execute(f"PRAGMA table_info({table_name})")
    return [r[1] for r in cur.fetchall()]


def ensure_table_trailer_daily():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trailer_daily (
        date TEXT,
        title_key TEXT,
        trailer_url TEXT,
        platform TEXT,
        views INTEGER,
        likes INTEGER,
        comments INTEGER,
        err TEXT,
        PRIMARY KEY (date, title_key, trailer_url)
    )
    """)
    conn.commit()

    cols = set(table_columns("trailer_daily"))
    # Por si venías con una versión anterior sin estas columnas
    needed = {
        "date": "TEXT",
        "title_key": "TEXT",
        "trailer_url": "TEXT",
        "platform": "TEXT",
        "views": "INTEGER",
        "likes": "INTEGER",
        "comments": "INTEGER",
        "err": "TEXT",
    }
    for c, t in needed.items():
        if c not in cols:
            cur.execute(f"ALTER TABLE trailer_daily ADD COLUMN {c} {t}")
    conn.commit()


def ensure_table_x_daily():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS x_daily (
        date TEXT,
        title_key TEXT,
        title TEXT,
        posts_7d INTEGER,
        eng_7d INTEGER,
        err TEXT,
        PRIMARY KEY (date, title_key)
    )
    """)
    conn.commit()

    cols = set(table_columns("x_daily"))
    needed = {
        "date": "TEXT",
        "title_key": "TEXT",
        "title": "TEXT",
        "posts_7d": "INTEGER",
        "eng_7d": "INTEGER",
        "err": "TEXT",
    }
    for c, t in needed.items():
        if c not in cols:
            cur.execute(f"ALTER TABLE x_daily ADD COLUMN {c} {t}")
    conn.commit()


ensure_table_trailer_daily()
ensure_table_x_daily()


# =========================================================
# HELPERS
# =========================================================
def sniff_delimiter(text: str) -> str:
    sample = text[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") > sample.count(",") else ","


def norm_header(h: str) -> str:
    if h is None:
        return ""
    h = str(h).replace("\ufeff", "").replace("\u00a0", " ")
    h = h.strip().lower()
    h = re.sub(r"\s+", " ", h)
    return h


def pick_fieldname_by_keywords(fieldnames, keywords):
    for fn in fieldnames:
        nh = norm_header(fn)
        for kw in keywords:
            if kw in nh:
                return fn
    return None


def parse_date_to_date(s):
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    s = s.strip('"').strip("'")
    s = s.replace("T", " ")
    s = s.split(" ")[0].strip()
    # normalizo separadores comunes
    s = s.replace(".", "/").replace("-", "/")
    s = re.sub(r"[^0-9/]", "", s)

    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def norm_title_key(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    # mantengo letras/números/espacios + acentos básicos para no destruir demasiado
    t = re.sub(r"[^\w\sáéíóúñü]", "", t)
    return t


def title_key_loose(s: str) -> str:
    # Normalización más agresiva para matching “Ultracine”
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = (
        s.replace("á", "a").replace("é", "e").replace("í", "i")
        .replace("ó", "o").replace("ú", "u").replace("ü", "u")
        .replace("ñ", "n")
    )
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def to_int_any(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("\u00a0", " ")
    # para entradas/recaudaciones con puntos/commas
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"[^\d]", "", s)
    try:
        return int(s) if s else None
    except Exception:
        return None


def fmt_int(n):
    if n is None:
        return "—"
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return "—"


def fmt_money(n):
    if n is None:
        return "—"
    try:
        n = int(n)
        return "$" + f"{n:,}".replace(",", ".")
    except Exception:
        return "—"


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def relation_with_today(release: date) -> str:
    d = (release - date.today()).days
    if d > 0:
        return f"Faltan {d} días"
    if d < 0:
        return f"Hace {abs(d)} días"
    return "Hoy"


def is_youtube(url: str) -> bool:
    u = (url or "").lower()
    return "youtube.com" in u or "youtu.be" in u


def is_vimeo(url: str) -> bool:
    u = (url or "").lower()
    return "vimeo.com" in u


def youtube_video_id(url: str):
    if not url:
        return None
    try:
        u = url.strip()
        if "youtu.be/" in u:
            return u.split("youtu.be/")[1].split("?")[0].split("/")[0]
        if "youtube.com" in u:
            p = urlparse(u)
            q = parse_qs(p.query)
            if "v" in q:
                return q["v"][0]
            if "/shorts/" in p.path:
                return p.path.split("/shorts/")[1].split("/")[0]
    except Exception:
        return None
    return None


def vimeo_id(url: str):
    if not url:
        return None
    try:
        p = urlparse(url.strip())
        if "vimeo.com" not in p.netloc.lower():
            return None
        parts = [x for x in p.path.split("/") if x]
        for part in reversed(parts):
            if part.isdigit():
                return part
        return None
    except Exception:
        return None


# =========================================================
# INCAA CATALOGO (CSV REMOTO)
# =========================================================
def load_catalog(csv_urls):
    all_rows = []
    fieldnames = None
    delims_used = []

    for url in csv_urls:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=60)
        resp.encoding = "utf-8"
        text = resp.text
        delim = sniff_delimiter(text)
        delims_used.append(delim)

        reader = csv.DictReader(StringIO(text), delimiter=delim)
        rows = list(reader)
        if fieldnames is None:
            fieldnames = reader.fieldnames or []
        all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("No se pudieron leer filas desde las pestañas CSV.")
    if not fieldnames:
        raise RuntimeError("No se pudieron detectar encabezados en el CSV.")

    title_col = pick_fieldname_by_keywords(
        fieldnames,
        ["peliculas", "películas", "pelicula", "título", "titulo", "obra", "film", "nombre"]
    )
    date_col = pick_fieldname_by_keywords(
        fieldnames,
        ["fecha de estreno", "estreno", "fecha", "release"]
    )
    trailer_col = pick_fieldname_by_keywords(
        fieldnames,
        ["trailer", "tráiler", "avance", "video", "link trailer", "url trailer"]
    )
    # si no detecta, intento 3ra columna como venías usando
    if not trailer_col:
        trailer_col = fieldnames[2] if len(fieldnames) >= 3 else None

    if not title_col:
        raise RuntimeError(f"No pude detectar columna de TÍTULO. Encabezados: {fieldnames}")
    if not date_col:
        raise RuntimeError(f"No pude detectar columna de FECHA. Encabezados: {fieldnames}")
    if not trailer_col:
        raise RuntimeError(f"No pude detectar columna de TRAILER (ni hay 3ra columna). Encabezados: {fieldnames}")

    today = date.today()
    start_window = today - timedelta(days=PAST_DAYS)
    end_window = today + timedelta(days=FUTURE_DAYS)

    movies = []
    seen = set()

    for r in all_rows:
        title = (r.get(title_col) or "").strip()
        d = parse_date_to_date(r.get(date_col))
        trailer_url = (r.get(trailer_col) or "").strip()

        if not title or not d:
            continue
        if not (start_window <= d <= end_window):
            continue

        k = norm_title_key(title)
        if k in seen:
            continue
        seen.add(k)

        movies.append({
            "title": title,
            "title_key": k,
            "release_date": d,
            "release_date_str": d.isoformat(),
            "trailer_url": trailer_url,
        })

    movies.sort(key=lambda x: x["release_date"])
    meta = (title_col, date_col, trailer_col, delims_used, start_window, end_window)
    return movies, meta


# =========================================================
# INCAA HISTÓRICO MANUAL (CSV LOCAL)
# =========================================================
def load_incaa_historico_manual(path):
    full = os.path.join(BASE_DIR, path)
    if not os.path.exists(full):
        return {}, f"No existe {path} (si lo creás, se muestran los acumulados)."

    with open(full, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}, f"{path} sin encabezados."
        headers = [h.strip() for h in reader.fieldnames]

        col_title = pick_fieldname_by_keywords(headers, ["titulo", "título", "pelicula", "película", "obra", "film", "nombre"])
        col_ent = pick_fieldname_by_keywords(headers, ["entradas", "espectadores", "publico", "público"])
        col_rec = pick_fieldname_by_keywords(headers, ["recaud", "taquilla", "importe", "monto"])
        col_pan = pick_fieldname_by_keywords(headers, ["pantalla", "pantallas", "copias"])

        if not col_title or not col_ent:
            return {}, f"{path}: faltan columnas mínimas (TITULO + ENTRADAS...). Encabezados: {headers}"

        out = {}
        for r in reader:
            t = (r.get(col_title) or "").strip()
            if not t:
                continue
            tk = norm_title_key(t)
            ent = to_int_any(r.get(col_ent))
            rec = to_int_any(r.get(col_rec)) if col_rec else None
            pan = to_int_any(r.get(col_pan)) if col_pan else None

            out[tk] = {"titulo": t, "entradas": ent, "recaudacion": rec, "pantallas": pan}

        return out, f"OK ({path})"


# =========================================================
# TRAILER STATS (YouTube / Vimeo)
# =========================================================
def youtube_stats(url: str):
    if not url or (not is_youtube(url)) or (not YOUTUBE_API_KEY):
        return {"views": None, "likes": None, "comments": None, "err": None}

    vid = youtube_video_id(url)
    if not vid:
        return {"views": None, "likes": None, "comments": None, "err": "no_video_id"}

    api = "https://www.googleapis.com/youtube/v3/videos"
    params = {"id": vid, "part": "statistics", "key": YOUTUBE_API_KEY}

    try:
        r = requests.get(api, params=params, timeout=30)
        data = r.json()
        if "items" not in data or not data["items"]:
            return {"views": None, "likes": None, "comments": None, "err": "not_found"}
        st = data["items"][0].get("statistics", {})
        return {
            "views": to_int_any(st.get("viewCount")),
            "likes": to_int_any(st.get("likeCount")),
            "comments": to_int_any(st.get("commentCount")),
            "err": None
        }
    except Exception as e:
        return {"views": None, "likes": None, "comments": None, "err": str(e)}


def vimeo_stats(url: str):
    # Vimeo views puede quedar vacío: nos enfocamos en likes/comentarios
    if not url or (not is_vimeo(url)) or (not vimeo_id(url)):
        return {"views": None, "likes": None, "comments": None, "err": None}

    try:
        page = requests.get(url, headers=BROWSER_HEADERS, timeout=30).text

        likes = None
        comments = None

        # Intentos frecuentes
        m = re.search(r'"likesCount"\s*:\s*([0-9]+)', page) or re.search(r'"likes"\s*:\s*([0-9]+)', page)
        if m:
            likes = int(m.group(1))

        m = re.search(r'"commentsCount"\s*:\s*([0-9]+)', page) or re.search(r'"comments"\s*:\s*([0-9]+)', page)
        if m:
            comments = int(m.group(1))

        return {"views": None, "likes": likes, "comments": comments, "err": None}
    except Exception as e:
        return {"views": None, "likes": None, "comments": None, "err": str(e)}


# =========================================================
# X METRICS (tweets últimos días; esto depende del plan de API que tengas)
# =========================================================
def x_metrics(title: str):
    if not X_BEARER_TOKEN:
        return {"posts_7d": None, "eng_7d": None, "err": None}

    # Search recent (últimos ~7 días según plan); usamos max 50 por simplicidad
    query = f'"{title}" lang:es -is:retweet'
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {"query": query, "max_results": 50, "tweet.fields": "public_metrics"}
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        data = r.json()

        if "data" not in data:
            err = data.get("error") or data.get("title") or data.get("detail") or str(data)
            return {"posts_7d": None, "eng_7d": None, "err": err}

        tweets = data["data"]
        posts_7d = len(tweets)

        eng = 0
        for t in tweets:
            pm = t.get("public_metrics", {})
            eng += int(pm.get("like_count", 0))
            eng += int(pm.get("reply_count", 0))
            eng += int(pm.get("retweet_count", 0))
            eng += int(pm.get("quote_count", 0))

        return {"posts_7d": posts_7d, "eng_7d": eng, "err": None}
    except Exception as e:
        return {"posts_7d": None, "eng_7d": None, "err": str(e)}


# =========================================================
# ULTRACINE TOP (SEMI-AUTOMÁTICO)
# =========================================================
def try_parse_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"(\{.*\}|\[.*\])", text, flags=re.S)
        if not m:
            return None
        try:
            return json.loads(m.group(1))
        except Exception:
            return None


def fetch_ultracine_top(token=ULTRACINE_TOKEN_DEFAULT, cty_id=ULTRACINE_CTY_DEFAULT, limit=ULTRACINE_LIMIT):
    """
    Devuelve (items, err). items: [{title, key, publico_semana, acumulado, source_url}]
    """
    urls = [
        f"https://www.ultracine.com/webservices/services/json/wsHomeTopMovies03.php?token={token}&cty_id={cty_id}&limit={limit}",
        f"https://www.ultracine.com/webservices/services/json/wsHomeTopMovies.php?token={token}&cty_id={cty_id}&limit={min(limit,10)}",
    ]

    last_err = None
    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": "movie-alerts/1.0"}, timeout=30)
            r.raise_for_status()
            data = try_parse_json(r.text)
            if not data:
                last_err = f"Ultracine: respuesta sin JSON parseable ({url})"
                continue

            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for k in ("data", "result", "results", "top", "movies"):
                    if k in data and isinstance(data[k], list):
                        items = data[k]
                        break
                if items is None and all(isinstance(v, dict) for v in data.values()):
                    items = list(data.values())

            if not items:
                last_err = f"Ultracine: JSON sin lista de items ({url})"
                continue

            out = []
            for it in items:
                if not isinstance(it, dict):
                    continue
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
                    "key": title_key_loose(str(t)),
                    "publico_semana": to_int_any(publico),
                    "acumulado": to_int_any(acumulado),
                    "source_url": url
                })

            if out:
                return out, None

            last_err = f"Ultracine: lista vacía tras normalizar ({url})"
        except Exception as e:
            last_err = f"Ultracine error: {type(e).__name__}: {e}"

    return [], last_err


def best_match_ultracine(ultra_items, movie_title: str):
    k = title_key_loose(movie_title)
    if not k:
        return None

    exact = [x for x in ultra_items if x.get("key") == k]
    if exact:
        return exact[0]

    # fallback por contención
    cand = []
    for x in ultra_items:
        uk = x.get("key") or ""
        if not uk:
            continue
        if k in uk or uk in k:
            cand.append((abs(len(uk) - len(k)), x))
    if not cand:
        return None
    cand.sort(key=lambda t: t[0])
    return cand[0][1]


# =========================================================
# SNAPSHOTS DB
# =========================================================
def upsert_trailer_daily(today_str, title_key, trailer_url, platform, views, likes, comments, err):
    cur.execute("""
    INSERT OR REPLACE INTO trailer_daily
    (date, title_key, trailer_url, platform, views, likes, comments, err)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (today_str, title_key, trailer_url, platform, views, likes, comments, err))
    conn.commit()


def get_trailer_yesterday(title_key, trailer_url):
    y = (date.today() - timedelta(days=1)).isoformat()
    cur.execute("""
    SELECT views, likes, comments FROM trailer_daily
    WHERE date=? AND title_key=? AND trailer_url=?
    """, (y, title_key, trailer_url))
    row = cur.fetchone()
    if not row:
        return None
    return {"views": row[0], "likes": row[1], "comments": row[2]}


def upsert_x_daily(today_str, title_key, title, posts_7d, eng_7d, err):
    cur.execute("""
    INSERT OR REPLACE INTO x_daily
    (date, title_key, title, posts_7d, eng_7d, err)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (today_str, title_key, title, posts_7d, eng_7d, err))
    conn.commit()


def get_x_yesterday(title_key):
    y = (date.today() - timedelta(days=1)).isoformat()
    cur.execute("""
    SELECT posts_7d, eng_7d FROM x_daily
    WHERE date=? AND title_key=?
    """, (y, title_key))
    row = cur.fetchone()
    if not row:
        return None
    return {"posts_7d": row[0], "eng_7d": row[1]}


# =========================================================
# HTML (3 páginas)
# =========================================================
BASE_STYLE = """
<style>
  :root { --bg:#0b0f19; --card:#0f172a; --muted:#9aa4b2; --text:#e5e7eb; --line:rgba(255,255,255,.08); --ok:#22c55e; --warn:#f97316;}
  body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background: radial-gradient(1200px 800px at 20% 0%, #111a33 0%, var(--bg) 60%); color:var(--text); }
  .container{max-width:1300px;margin:0 auto;padding:26px;}
  .top{display:flex;gap:12px;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;margin-bottom:10px;}
  h1{margin:0;font-size:26px;}
  .muted{color:var(--muted);font-size:12px;line-height:1.35;}
  .card{background:rgba(15,23,42,.85);border:1px solid var(--line);border-radius:16px;padding:14px;margin:14px 0;box-shadow:0 10px 30px rgba(0,0,0,.25);}
  .btn{display:inline-block;padding:10px 12px;border-radius:12px;background:rgba(255,255,255,.06);border:1px solid var(--line);color:var(--text);text-decoration:none;font-size:13px}
  .btn:hover{background:rgba(255,255,255,.09);}
  .kpis{display:flex;gap:10px;flex-wrap:wrap;margin:10px 0 0 0;}
  .kpi{background:rgba(255,255,255,.06);border:1px solid var(--line);border-radius:14px;padding:10px 12px;min-width:170px;}
  .kpi .n{font-size:18px;font-weight:800;}
  .kpi .t{font-size:12px;color:var(--muted);}
  .tablewrap{overflow:auto;border-radius:12px;border:1px solid var(--line);margin-top:10px;}
  table{border-collapse:separate;border-spacing:0;min-width:1400px;width:100%;background:rgba(0,0,0,.15);}
  th,td{padding:10px 10px;border-bottom:1px solid var(--line);vertical-align:top;}
  th{position:sticky;top:0;background:rgba(0,0,0,.35);backdrop-filter: blur(6px);text-align:left;font-size:12px;color:#dbe3ef;}
  tr:hover td{background:rgba(255,255,255,.04);}
  .num{text-align:right;white-space:nowrap;}
  .badge{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;font-weight:800;}
  .badge.ok{background:rgba(34,197,94,.18);color:#86efac;border:1px solid rgba(34,197,94,.35);}
  .badge.alert{background:rgba(249,115,22,.18);color:#fdba74;border:1px solid rgba(249,115,22,.35);}
  .footer{margin-top:16px;color:var(--muted);font-size:12px;}
</style>
"""


def page_shell(title, subtitle, body_html):
    return f"""<!doctype html>
<html lang="es"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{html_escape(title)}</title>
{BASE_STYLE}
</head><body>
<div class="container">
  <div class="top">
    <div>
      <h1>{html_escape(title)}</h1>
      <div class="muted">{subtitle}</div>
    </div>
    <div style="display:flex;gap:10px;flex-wrap:wrap">
      <a class="btn" href="index.html">Inicio</a>
      <a class="btn" href="proximos.html">Próximos</a>
      <a class="btn" href="estrenadas.html">Estrenadas</a>
    </div>
  </div>
  {body_html}
  <div class="footer">Actualizado: {date.today().strftime('%d/%m/%Y')}</div>
</div>
</body></html>"""


def build_index(meta, incaa_status, ultracine_status, counts):
    title_col, date_col, trailer_col, delims_used, start_window, end_window = meta
    c_total, c_up, c_rel, c_alert = counts

    body = f"""
    <div class="card">
      <div class="muted">
        Ventana de estrenos: <b>{start_window.isoformat()}</b> → <b>{end_window.isoformat()}</b><br/>
        Catálogo INCAA (CSV): “{html_escape(title_col)}” / “{html_escape(date_col)}” / “{html_escape(trailer_col)}” · delimitadores detectados: {html_escape(str(delims_used))}<br/>
        INCAA histórico manual: <b>{html_escape(incaa_status)}</b><br/>
        Ultracine (TOP semanal): <b>{html_escape(ultracine_status)}</b>
      </div>
      <div class="kpis">
        <div class="kpi"><div class="n">{c_total}</div><div class="t">títulos en ventana</div></div>
        <div class="kpi"><div class="n">{c_up}</div><div class="t">próximos estrenos</div></div>
        <div class="kpi"><div class="n">{c_rel}</div><div class="t">ya estrenadas</div></div>
        <div class="kpi"><div class="n">{c_alert}</div><div class="t">con alerta</div></div>
      </div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px">
        <a class="btn" href="proximos.html">Ver Próximos estrenos</a>
        <a class="btn" href="estrenadas.html">Ver Estrenadas</a>
      </div>
    </div>
    """
    return page_shell("Estrenos argentinos – inicio", "Portada del dashboard", body)


def build_table(rows, incaa_map, ultra_items, ultra_err):
    if not rows:
        return "<div class='muted'>— Sin títulos en esta página —</div>"

    trs = []
    for r in rows:
        inc = incaa_map.get(r["title_key"], {})

        trailer_url = (r.get("trailer_url") or "").strip()
        link_html = f'<a class="btn" href="{html_escape(trailer_url)}" target="_blank" rel="noopener">Abrir trailer</a>' if trailer_url else "—"

        badge = "ALERTA" if r.get("has_alert") else "OK"
        badge_class = "badge alert" if r.get("has_alert") else "badge ok"

        # Ultracine match (solo si aparece en TOP semanal)
        u = best_match_ultracine(ultra_items, r["title"]) if ultra_items else None
        ultra_pub = u.get("publico_semana") if u else None
        ultra_acc = u.get("acumulado") if u else None

        # Si Ultracine falló, NO rompemos: mostramos — y listo
        # (ultra_err lo mostramos en subtítulo de la página, no fila por fila)

        trs.append(f"""
        <tr>
          <td><span class="{badge_class}">{badge}</span></td>
          <td>
            <b>{html_escape(r['title'])}</b>
            <div class="muted">{html_escape(r.get('alert_reason') or '')}</div>
          </td>

          <td>
            {html_escape(r['release_date_str'])}
            <div class="muted">{html_escape(relation_with_today(r['release_date']))}</div>
          </td>

          <td>{html_escape(r.get('trailer_platform') or '—')}</td>

          <td class="num">
            {fmt_int(r.get('views'))}
            <div class="muted">Cambio 24 h: {fmt_int(r.get('views_change_24h'))}</div>
          </td>

          <td class="num">
            {fmt_int(r.get('likes'))}
            <div class="muted">Cambio 24 h: {fmt_int(r.get('likes_change_24h'))}</div>
          </td>

          <td class="num">
            {fmt_int(r.get('comments'))}
            <div class="muted">Cambio 24 h: {fmt_int(r.get('comments_change_24h'))}</div>
          </td>

          <td>{link_html}</td>

          <td class="num">
            {fmt_int(r.get('x_posts_7d'))}
            <div class="muted">Cambio 24 h: {fmt_int(r.get('x_posts_change_24h'))}</div>
          </td>

          <td class="num">
            {fmt_int(r.get('x_eng_7d'))}
            <div class="muted">Cambio 24 h: {fmt_int(r.get('x_eng_change_24h'))}</div>
          </td>

          <td class="num">{fmt_int(ultra_pub)}</td>
          <td class="num">{fmt_int(ultra_acc)}</td>

          <td class="num">{fmt_int(inc.get('entradas'))}</td>
          <td class="num">{fmt_money(inc.get('recaudacion'))}</td>
          <td class="num">{fmt_int(inc.get('pantallas'))}</td>
        </tr>
        """)

    return f"""
    <div class="tablewrap">
      <table>
        <thead>
          <tr>
            <th>Estado</th>
            <th>Película</th>
            <th>Estreno</th>

            <th>Trailer</th>
            <th>Vistas</th>
            <th>Me gusta</th>
            <th>Comentarios</th>
            <th>Link</th>

            <th>X: publicaciones</th>
            <th>X: interacciones</th>

            <th>Ultracine: público</th>
            <th>Ultracine: acumulado</th>

            <th>INCAA: entradas</th>
            <th>INCAA: recaudación</th>
            <th>INCAA: pantallas</th>
          </tr>
        </thead>
        <tbody>
          {''.join(trs)}
        </tbody>
      </table>
    </div>
    """


# =========================================================
# EMAIL (solo link)
# =========================================================
def send_link_email(public_url: str):
    subject = f"Dashboard estrenos argentinos – {date.today().strftime('%d/%m/%Y')}"
    html_body = f"""
    <html><body style="font-family:Arial,Helvetica,sans-serif;color:#111">
      <p>Dashboard:</p>
      <p><a href="{html_escape(public_url)}">{html_escape(public_url)}</a></p>
    </body></html>
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(public_url, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


# =========================================================
# MAIN
# =========================================================
def main():
    movies, meta = load_catalog(CSV_URLS)
    incaa_map, incaa_status = load_incaa_historico_manual(INCAA_HIST_CSV)

    # Ultracine TOP (aux)
    ultra_items, ultra_err = fetch_ultracine_top()
    ultracine_status = "OK (TOP semanal)" if ultra_items else (ultra_err or "Sin datos (no disponible)")

    today = date.today()
    today_str = today.isoformat()

    enriched = []
    for m in movies:
        trailer_url = (m.get("trailer_url") or "").strip()

        # --- Trailer stats
        if is_youtube(trailer_url):
            stats = youtube_stats(trailer_url)
            platform = "YouTube"
        elif is_vimeo(trailer_url):
            stats = vimeo_stats(trailer_url)
            platform = "Vimeo"
        elif trailer_url:
            stats = {"views": None, "likes": None, "comments": None, "err": None}
            platform = "Link"
        else:
            stats = {"views": None, "likes": None, "comments": None, "err": None}
            platform = "—"

        upsert_trailer_daily(
            today_str,
            m["title_key"],
            trailer_url or "",
            platform.lower(),
            stats.get("views"),
            stats.get("likes"),
            stats.get("comments"),
            stats.get("err"),
        )

        y = get_trailer_yesterday(m["title_key"], trailer_url or "")
        views_change = likes_change = comments_change = None
        if y:
            if stats.get("views") is not None and y.get("views") is not None:
                views_change = stats["views"] - y["views"]
            if stats.get("likes") is not None and y.get("likes") is not None:
                likes_change = stats["likes"] - y["likes"]
            if stats.get("comments") is not None and y.get("comments") is not None:
                comments_change = stats["comments"] - y["comments"]

        # --- X metrics
        xm = x_metrics(m["title"])
        upsert_x_daily(today_str, m["title_key"], m["title"], xm.get("posts_7d"), xm.get("eng_7d"), xm.get("err"))

        xy = get_x_yesterday(m["title_key"])
        x_posts_change = x_eng_change = None
        if xy:
            if xm.get("posts_7d") is not None and xy.get("posts_7d") is not None:
                x_posts_change = xm["posts_7d"] - xy["posts_7d"]
            if xm.get("eng_7d") is not None and xy.get("eng_7d") is not None:
                x_eng_change = xm["eng_7d"] - xy["eng_7d"]

        # --- Alertas (motivo)
        reasons = []
        if views_change is not None and views_change >= ALERT_VIEWS_CHANGE_24H:
            reasons.append(f"Suba fuerte de vistas (24 h): {fmt_int(views_change)}")
        if likes_change is not None and likes_change >= ALERT_LIKES_CHANGE_24H:
            reasons.append(f"Suba fuerte de me gusta (24 h): {fmt_int(likes_change)}")
        if comments_change is not None and comments_change >= ALERT_COMMENTS_CHANGE_24H:
            reasons.append(f"Suba fuerte de comentarios (24 h): {fmt_int(comments_change)}")
        if x_posts_change is not None and x_posts_change >= ALERT_X_POSTS_CHANGE_24H:
            reasons.append(f"Más conversación en X (24 h): {fmt_int(x_posts_change)}")
        if x_eng_change is not None and x_eng_change >= ALERT_X_ENG_CHANGE_24H:
            reasons.append(f"Más interacción en X (24 h): {fmt_int(x_eng_change)}")

        mm = dict(m)
        mm["trailer_platform"] = platform
        mm["views"] = stats.get("views")
        mm["likes"] = stats.get("likes")
        mm["comments"] = stats.get("comments")
        mm["views_change_24h"] = views_change
        mm["likes_change_24h"] = likes_change
        mm["comments_change_24h"] = comments_change
        mm["x_posts_7d"] = xm.get("posts_7d")
        mm["x_eng_7d"] = xm.get("eng_7d")
        mm["x_posts_change_24h"] = x_posts_change
        mm["x_eng_change_24h"] = x_eng_change
        mm["has_alert"] = bool(reasons)
        mm["alert_reason"] = " · ".join(reasons) if reasons else None

        enriched.append(mm)

    # Separar páginas
    upcoming = [m for m in enriched if m["release_date"] > today]
    released = [m for m in enriched if m["release_date"] <= today]
    released.sort(key=lambda x: x["release_date"], reverse=True)

    counts = (
        len(enriched),
        len(upcoming),
        len(released),
        len([m for m in enriched if m["has_alert"]]),
    )

    # HTML
    index_html = build_index(meta, incaa_status, ultracine_status, counts)

    prox_sub = "Estrenos que todavía no ocurrieron."
    if ultra_err and not ultra_items:
        prox_sub += f" Ultracine: {ultra_err}"
    proximos_html = page_shell(
        "Próximos estrenos",
        prox_sub,
        f"<div class='card'>{build_table(upcoming, incaa_map, ultra_items, ultra_err)}</div>"
    )

    est_sub = "Estrenadas dentro de la ventana configurada."
    if ultra_err and not ultra_items:
        est_sub += f" Ultracine: {ultra_err}"
    estrenadas_html = page_shell(
        "Ya estrenadas",
        est_sub,
        f"<div class='card'>{build_table(released, incaa_map, ultra_items, ultra_err)}</div>"
    )

    # Publicar 3 archivos
    publish_html(GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN, index_html, target_path="index.html")
    publish_html(GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN, proximos_html, target_path="proximos.html")
    publish_html(GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN, estrenadas_html, target_path="estrenadas.html")

    # Mail SOLO con el index
    send_link_email(PAGES_URL)

    print("OK: publicado index/proximos/estrenadas + mail:", PAGES_URL)


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()

