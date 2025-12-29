# main.py
# Dashboard automático de estrenos argentinos (catálogo INCAA vía CSV) + métricas de trailers (YouTube/Vimeo)
# + (opcional) métricas en X (Twitter) usando Bearer Token.
#
# - Ventana de monitoreo: 30 días ANTES del estreno y 30 días DESPUÉS.
# - Separa en 2 páginas: upcoming.html (próximos) y released.html (ya estrenadas en últimos 30 días).
# - Publica a GitHub Pages (opcional) subiendo HTML a tu repo vía GitHub API.
# - (Opcional) manda un mail con el link del dashboard (solo link).
#
# Requisitos (pip):
#   pip install requests
#
# config.env (en la misma carpeta):
#   INCAA_CSV_URL_1=...
#   INCAA_CSV_URL_2=...              (opcional)
#   TZ=America/Argentina/Buenos_Aires (opcional)
#
#   # YouTube (opcional pero recomendado)
#   YOUTUBE_API_KEY=...
#
#   # X / Twitter (opcional)
#   X_BEARER_TOKEN=...
#
#   # Publicación GitHub Pages (opcional)
#   GH_TOKEN=...            # token con permiso repo:contents
#   GH_REPO=usuario/repo    # ej: donleopardo4/movie-dashboard
#   GH_BRANCH=main
#   GH_PAGES_BASEURL=https://usuario.github.io/repo
#
#   # Email (opcional)
#   EMAIL_TO=...
#   EMAIL_FROM=...
#   SMTP_HOST=smtp.gmail.com
#   SMTP_PORT=587
#   SMTP_USER=...
#   SMTP_PASS=...           # app password
#
# Notas:
# - Vimeo: sin token oficial, NO hay un endpoint público confiable para views. Aquí intentamos likes/comments
#   vía oEmbed/HTML (best-effort). Si no se puede, quedará vacío sin romper el flujo.

import os
import re
import csv
import json
import base64
import sqlite3
import smtplib
from io import StringIO
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

import requests


# -------------------- ENV --------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "movie.db")


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


load_env_file(os.path.join(BASE_DIR, "config.env"))

INCAA_CSV_URL_1 = (os.getenv("INCAA_CSV_URL_1") or "").strip()
INCAA_CSV_URL_2 = (os.getenv("INCAA_CSV_URL_2") or "").strip()

YOUTUBE_API_KEY = (os.getenv("YOUTUBE_API_KEY") or "").strip()
X_BEARER_TOKEN = (os.getenv("X_BEARER_TOKEN") or "").strip()

GH_TOKEN = (os.getenv("GH_TOKEN") or "").strip()
GH_REPO = (os.getenv("GH_REPO") or "").strip()
GH_BRANCH = (os.getenv("GH_BRANCH") or "main").strip()
GH_PAGES_BASEURL = (os.getenv("GH_PAGES_BASEURL") or "").strip()

EMAIL_TO = (os.getenv("EMAIL_TO") or "").strip()
EMAIL_FROM = (os.getenv("EMAIL_FROM") or "").strip()
SMTP_HOST = (os.getenv("SMTP_HOST") or "").strip()
SMTP_PORT = int((os.getenv("SMTP_PORT") or "587").strip())
SMTP_USER = (os.getenv("SMTP_USER") or "").strip()
SMTP_PASS = (os.getenv("SMTP_PASS") or "").strip()

TZ_NAME = (os.getenv("TZ") or "America/Argentina/Buenos_Aires").strip()


def now_local() -> datetime:
    # Sin dependencias extra (pytz/zoneinfo): usamos hora local "naive".
    # Para ventanas de días es suficiente.
    return datetime.now()


# -------------------- DB --------------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def db_init(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        title_key TEXT PRIMARY KEY,
        title TEXT,
        release_date TEXT,      -- YYYY-MM-DD
        trailer_url TEXT,
        trailer_kind TEXT       -- youtube/vimeo/other
    )
    """)

    # Snapshot diario por película (trailer)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trailer_daily (
        date TEXT,
        title_key TEXT,
        trailer_url TEXT,
        trailer_kind TEXT,
        views INTEGER,
        likes INTEGER,
        comments INTEGER,
        err TEXT,
        PRIMARY KEY (date, title_key)
    )
    """)

    # Snapshot diario por película (X)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS x_daily (
        date TEXT,
        title_key TEXT,
        title TEXT,
        tweets_7d INTEGER,
        eng_7d INTEGER,
        err TEXT,
        PRIMARY KEY (date, title_key)
    )
    """)

    conn.commit()


# -------------------- Helpers --------------------

def title_to_key(title: str) -> str:
    t = title.strip().lower()
    t = re.sub(r"\s+", " ", t)
    # quitamos signos raros
    t = re.sub(r"[^a-z0-9áéíóúñü\s\-]", "", t, flags=re.IGNORECASE)
    t = t.strip()
    return t


def safe_int(x):
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return x
        s = str(x).strip()
        if not s:
            return None
        # 295.191 o 295,191
        s = s.replace(".", "").replace(",", "")
        return int(s)
    except Exception:
        return None


def parse_date_any(s: str):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def detect_csv_delimiter(sample: str) -> str:
    # intenta detectar delimitador
    candidates = [",", ";", "\t", "|"]
    scores = {}
    for c in candidates:
        scores[c] = sample.count(c)
    # el que más aparece
    best = max(scores.items(), key=lambda kv: kv[1])[0]
    return best if scores[best] > 0 else ","


def pick_col(headers, candidates):
    low = {h: h.strip().lower() for h in headers}
    for h in headers:
        hl = low[h]
        for c in candidates:
            if c in hl:
                return h
    return None


def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# -------------------- INCAA Catalog (CSV) --------------------

def load_incaa_catalog(csv_urls):
    if not csv_urls:
        raise RuntimeError("Falta INCAA_CSV_URL_1 (y/o INCAA_CSV_URL_2) en config.env / secrets.")

    all_rows = []
    last_headers = None
    delim_used = []

    for url in csv_urls:
        if not url:
            continue
        r = requests.get(url, timeout=40)
        r.raise_for_status()
        text = r.text
        sample = text[:2000]
        delim = detect_csv_delimiter(sample)
        delim_used.append(delim)

        reader = csv.DictReader(StringIO(text), delimiter=delim)
        rows = list(reader)
        if rows:
            last_headers = reader.fieldnames
            all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("No se pudieron leer filas desde los CSV del catálogo INCAA.")

    headers = [h.strip() for h in (last_headers or []) if h]
    # columnas típicas
    title_col = pick_col(headers, ["titulo", "título", "pelicula", "película", "obra", "film", "nombre"])
    date_col = pick_col(headers, ["fecha", "estreno", "release"])
    trailer_col = pick_col(headers, ["trailer", "youtube", "vimeo", "link", "url"])

    if not title_col or not date_col:
        raise RuntimeError(f"No pude detectar columnas de TÍTULO/FECHA. Encabezados: {headers}")

    movies = []
    for r in all_rows:
        title = (r.get(title_col) or "").strip()
        d = parse_date_any(r.get(date_col) or "")
        trailer = (r.get(trailer_col) or "").strip() if trailer_col else ""

        if not title or not d:
            continue

        # normalizar trailer
        trailer_url = trailer
        kind = classify_trailer(trailer_url)

        movies.append({
            "title": title,
            "title_key": title_to_key(title),
            "release_date": d.strftime("%Y-%m-%d"),
            "trailer_url": trailer_url,
            "trailer_kind": kind,
        })

    # dedupe por title_key (si hay 2 pestañas, te quedás con el que tenga trailer si el otro no)
    ded = {}
    for m in movies:
        k = m["title_key"]
        if k not in ded:
            ded[k] = m
        else:
            if (not ded[k].get("trailer_url")) and m.get("trailer_url"):
                ded[k] = m

    meta = {
        "headers": headers,
        "delims": delim_used,
        "title_col": title_col,
        "date_col": date_col,
        "trailer_col": trailer_col,
    }
    return list(ded.values()), meta


def classify_trailer(url: str) -> str:
    u = (url or "").strip().lower()
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "vimeo.com" in u:
        return "vimeo"
    if u:
        return "other"
    return "—"


# -------------------- Upsert catalog -> DB --------------------

def upsert_movies(conn: sqlite3.Connection, movies):
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO movies (title_key, title, release_date, trailer_url, trailer_kind)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(m["title_key"], m["title"], m["release_date"], m.get("trailer_url",""), m.get("trailer_kind","—")) for m in movies]
    )
    conn.commit()


# -------------------- YouTube stats --------------------

YOUTUBE_ID_PATTERNS = [
    re.compile(r"(?:v=)([A-Za-z0-9_\-]{11})"),
    re.compile(r"youtu\.be/([A-Za-z0-9_\-]{11})"),
    re.compile(r"youtube\.com/shorts/([A-Za-z0-9_\-]{11})"),
]


def extract_youtube_id(url: str):
    if not url:
        return None
    for p in YOUTUBE_ID_PATTERNS:
        m = p.search(url)
        if m:
            return m.group(1)
    return None


def fetch_youtube_video_stats(video_id: str):
    # returns dict {views, likes, comments}
    if not YOUTUBE_API_KEY or not video_id:
        return {"views": None, "likes": None, "comments": None, "err": "sin_youtube_key_o_id"}
    try:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "statistics",
            "id": video_id,
            "key": YOUTUBE_API_KEY,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or []
        if not items:
            return {"views": None, "likes": None, "comments": None, "err": "youtube_no_items"}
        st = items[0].get("statistics") or {}
        return {
            "views": safe_int(st.get("viewCount")),
            "likes": safe_int(st.get("likeCount")),
            "comments": safe_int(st.get("commentCount")),
            "err": None
        }
    except Exception as e:
        return {"views": None, "likes": None, "comments": None, "err": f"youtube_err:{e.__class__.__name__}"}


# -------------------- Vimeo best-effort --------------------

VIMEO_ID_PAT = re.compile(r"vimeo\.com/(\d+)")


def extract_vimeo_id(url: str):
    if not url:
        return None
    m = VIMEO_ID_PAT.search(url)
    return m.group(1) if m else None


def fetch_vimeo_best_effort(video_url: str):
    """
    Sin token oficial, intentamos:
    - oEmbed (no trae likes/comments)
    - scrape simple del HTML (puede fallar, y está ok: devolvemos None sin romper)
    """
    if not video_url:
        return {"views": None, "likes": None, "comments": None, "err": "sin_url"}

    # likes/comments: best-effort scrape (puede ser None)
    try:
        r = requests.get(video_url, timeout=30, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code >= 400:
            return {"views": None, "likes": None, "comments": None, "err": f"vimeo_http_{r.status_code}"}
        html = r.text

        # intentos típicos: buscar números cerca de "likes" / "comments" (muy frágil)
        likes = None
        comments = None

        # pattern: "likes":123 o "likeCount":123
        m = re.search(r'"likeCount"\s*:\s*(\d+)', html)
        if m:
            likes = safe_int(m.group(1))
        if likes is None:
            m = re.search(r'"likes"\s*:\s*(\d+)', html)
            if m:
                likes = safe_int(m.group(1))

        m = re.search(r'"commentCount"\s*:\s*(\d+)', html)
        if m:
            comments = safe_int(m.group(1))
        if comments is None:
            m = re.search(r'"comments"\s*:\s*(\d+)', html)
            if m:
                comments = safe_int(m.group(1))

        return {"views": None, "likes": likes, "comments": comments, "err": None}
    except Exception as e:
        return {"views": None, "likes": None, "comments": None, "err": f"vimeo_err:{e.__class__.__name__}"}


# -------------------- X / Twitter stats (7d) --------------------

def fetch_x_7d(title: str):
    """
    Busca tweets recientes (últimos 7 días) por título.
    Devuelve:
      tweets_7d: cantidad de tweets encontrados (limitado por API)
      eng_7d: suma de (like+reply+retweet+quote)
    """
    if not X_BEARER_TOKEN:
        return {"tweets_7d": None, "eng_7d": None, "err": "sin_x_token"}

    # Query: título exacto entre comillas + OR sin comillas, filtrando retweets
    # Ojo: X Search es sensible; esto es un punto de partida.
    q_title = title.strip()
    if not q_title:
        return {"tweets_7d": None, "eng_7d": None, "err": "sin_titulo"}

    query = f'"{q_title}" -is:retweet lang:es OR "{q_title}" -is:retweet'
    # time range: now-7d
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)

    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    params = {
        "query": query,
        "max_results": 100,
        "start_time": start_time.isoformat().replace("+00:00", "Z"),
        "end_time": end_time.isoformat().replace("+00:00", "Z"),
        "tweet.fields": "public_metrics",
    }

    tweets = 0
    eng = 0
    next_token = None

    try:
        for _ in range(6):  # hasta 600 tweets (best-effort)
            if next_token:
                params["next_token"] = next_token
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 429:
                return {"tweets_7d": None, "eng_7d": None, "err": "x_rate_limited"}
            r.raise_for_status()
            data = r.json()
            items = data.get("data") or []
            for t in items:
                tweets += 1
                pm = t.get("public_metrics") or {}
                eng += int(pm.get("like_count", 0)) + int(pm.get("reply_count", 0)) + int(pm.get("retweet_count", 0)) + int(pm.get("quote_count", 0))

            meta = data.get("meta") or {}
            next_token = meta.get("next_token")
            if not next_token:
                break

        return {"tweets_7d": tweets, "eng_7d": eng, "err": None}
    except Exception as e:
        return {"tweets_7d": None, "eng_7d": None, "err": f"x_err:{e.__class__.__name__}"}


# -------------------- Snapshot upserts --------------------

def upsert_trailer_daily(conn: sqlite3.Connection, day: str, m, stats):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO trailer_daily
        (date, title_key, trailer_url, trailer_kind, views, likes, comments, err)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            day,
            m["title_key"],
            m.get("trailer_url") or "",
            m.get("trailer_kind") or "—",
            stats.get("views"),
            stats.get("likes"),
            stats.get("comments"),
            stats.get("err"),
        )
    )
    conn.commit()


def upsert_x_daily(conn: sqlite3.Connection, day: str, m, xstats):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO x_daily
        (date, title_key, title, tweets_7d, eng_7d, err)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            day,
            m["title_key"],
            m["title"],
            xstats.get("tweets_7d"),
            xstats.get("eng_7d"),
            xstats.get("err"),
        )
    )
    conn.commit()


def get_snapshot(conn: sqlite3.Connection, table: str, day: str, title_key: str):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE date=? AND title_key=?", (day, title_key))
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def get_latest_before(conn: sqlite3.Connection, table: str, day: str, title_key: str, days_back: int):
    # busca un snapshot de "day - days_back"
    d = datetime.strptime(day, "%Y-%m-%d") - timedelta(days=days_back)
    target = d.strftime("%Y-%m-%d")
    return get_snapshot(conn, table, target, title_key)


# -------------------- Dashboard rendering --------------------

def fmt_num(x):
    if x is None:
        return "—"
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return "—"


def fmt_delta(x):
    if x is None:
        return "—"
    try:
        v = int(x)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:,}".replace(",", ".")
    except Exception:
        return "—"


def compute_alerts(row):
    # marca ALERTA si hay salto importante (heurística simple)
    # preferimos: crecimiento fuerte 24h en views o eng
    dv = row.get("views_24h_delta")
    deng = row.get("x_eng_24h_delta")

    flags = []
    if dv is not None and dv >= 5000:
        flags.append("Salto vistas 24h")
    if deng is not None and deng >= 500:
        flags.append("Salto X 24h")

    return " / ".join(flags) if flags else "—"


def build_rows(conn: sqlite3.Connection, movies, today: datetime):
    day = today.strftime("%Y-%m-%d")
    rows = []

    for m in movies:
        rel = datetime.strptime(m["release_date"], "%Y-%m-%d")
        days_to = (rel - today).days  # positivo si falta
        # ventana: (rel - 30) .. (rel + 30)
        if not (rel - timedelta(days=30) <= today <= rel + timedelta(days=30)):
            continue

        snap_t = get_snapshot(conn, "trailer_daily", day, m["title_key"]) or {}
        snap_t_1d = get_latest_before(conn, "trailer_daily", day, m["title_key"], 1) or {}

        views = snap_t.get("views")
        likes = snap_t.get("likes")
        com = snap_t.get("comments")

        dv_24h = None
        dl_24h = None
        dc_24h = None

        if views is not None and snap_t_1d.get("views") is not None:
            dv_24h = views - snap_t_1d.get("views")
        if likes is not None and snap_t_1d.get("likes") is not None:
            dl_24h = likes - snap_t_1d.get("likes")
        if com is not None and snap_t_1d.get("comments") is not None:
            dc_24h = com - snap_t_1d.get("comments")

        # total desde (estreno-30)
        base_day = (rel - timedelta(days=30)).strftime("%Y-%m-%d")
        snap_base = get_snapshot(conn, "trailer_daily", base_day, m["title_key"]) or {}
        dv_since = None
        if views is not None and snap_base.get("views") is not None:
            dv_since = views - snap_base.get("views")

        # X
        snap_x = get_snapshot(conn, "x_daily", day, m["title_key"]) or {}
        snap_x_1d = get_latest_before(conn, "x_daily", day, m["title_key"], 1) or {}

        x_tweets = snap_x.get("tweets_7d")
        x_eng = snap_x.get("eng_7d")

        dx_24h = None
        deng_24h = None
        if x_tweets is not None and snap_x_1d.get("tweets_7d") is not None:
            dx_24h = x_tweets - snap_x_1d.get("tweets_7d")
        if x_eng is not None and snap_x_1d.get("eng_7d") is not None:
            deng_24h = x_eng - snap_x_1d.get("eng_7d")

        row = {
            "title": m["title"],
            "release_date": m["release_date"],
            "days_to": days_to,
            "trailer_kind": m.get("trailer_kind") or "—",
            "trailer_url": m.get("trailer_url") or "",
            "views": views,
            "views_24h_delta": dv_24h,
            "likes": likes,
            "likes_24h_delta": dl_24h,
            "comments": com,
            "comments_24h_delta": dc_24h,
            "views_since_release_minus_30": dv_since,
            "x_tweets_7d": x_tweets,
            "x_tweets_24h_delta": dx_24h,
            "x_eng_7d": x_eng,
            "x_eng_24h_delta": deng_24h,
        }
        row["alert"] = compute_alerts(row)
        rows.append(row)

    # separar upcoming / released
    upcoming = [r for r in rows if r["days_to"] >= 0]
    released = [r for r in rows if r["days_to"] < 0]

    # ordenar: próximos por estreno asc; ya estrenadas por estreno desc
    upcoming.sort(key=lambda r: r["release_date"])
    released.sort(key=lambda r: r["release_date"], reverse=True)

    return upcoming, released


def make_html_page(title: str, subtitle: str, rows, today: datetime):
    # tabla con columnas sin símbolos raros
    # Orden: trailer stats + crecimiento total (desde estreno-30) inmediatamente después, y luego X.
    css = """
    :root{color-scheme:dark;}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:#0b0f14;color:#e8edf2;}
    header{padding:20px 18px;border-bottom:1px solid #1b2430;background:linear-gradient(180deg,#0f1722,#0b0f14);}
    h1{margin:0;font-size:20px;font-weight:700;}
    .sub{margin-top:6px;color:#a8b3c2;font-size:13px}
    .wrap{padding:18px;}
    .card{background:#0f1722;border:1px solid #1b2430;border-radius:14px;overflow:hidden;box-shadow:0 12px 30px rgba(0,0,0,.25);}
    .toolbar{display:flex;gap:10px;flex-wrap:wrap;padding:12px 12px;border-bottom:1px solid #1b2430;background:#0c1320}
    .pill{font-size:12px;color:#a8b3c2;border:1px solid #1b2430;padding:6px 10px;border-radius:999px}
    table{width:100%;border-collapse:collapse;font-size:13px;}
    th,td{padding:10px 10px;border-bottom:1px solid #152031;vertical-align:middle;}
    th{position:sticky;top:0;background:#0f1722;text-align:left;font-size:12px;color:#a8b3c2;z-index:1;}
    tr:hover td{background:#0c1320;}
    a{color:#7aa2ff;text-decoration:none}
    a:hover{text-decoration:underline}
    .muted{color:#a8b3c2}
    .right{text-align:right}
    .badge{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;border:1px solid #1b2430;color:#cfe0ff;background:#0c1320}
    .warn{border-color:#3b2b0f;color:#ffd38b;background:#1a1408}
    footer{padding:18px;color:#7f8a9a;font-size:12px}
    """
    head = f"""<!doctype html>
<html lang="es"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{html_escape(title)}</title>
<style>{css}</style>
</head><body>
<header>
  <h1>{html_escape(title)}</h1>
  <div class="sub">{html_escape(subtitle)} · Actualizado: {today.strftime("%d/%m/%Y %H:%M")}</div>
</header>
<div class="wrap">
<div class="card">
<div class="toolbar">
  <span class="pill">Ventana: 30 días antes / 30 días después del estreno</span>
  <span class="pill">Trailer: vistas / likes / comentarios + variación 24h</span>
  <span class="pill">Crecimiento total: desde (estreno - 30)</span>
  <span class="pill">X: tweets 7d / engagement 7d + variación 24h</span>
</div>
<table>
<thead>
<tr>
  <th>Alerta</th>
  <th>Película</th>
  <th>Estreno</th>
  <th>Días</th>
  <th>Trailer</th>
  <th>Vistas</th>
  <th>Vistas (cambio 24h)</th>
  <th>Likes</th>
  <th>Likes (cambio 24h)</th>
  <th>Comentarios</th>
  <th>Comentarios (cambio 24h)</th>
  <th>Crecimiento total vistas (desde estreno - 30)</th>
  <th>X tweets (7 días)</th>
  <th>X tweets (cambio 24h)</th>
  <th>X engagement (7 días)</th>
  <th>X engagement (cambio 24h)</th>
  <th>Link</th>
</tr>
</thead>
<tbody>
"""
    body = []
    for r in rows:
        days_str = f"{r['days_to']:+d}"
        badge = '<span class="badge">—</span>'
        if r["alert"] != "—":
            badge = f'<span class="badge warn">{html_escape(r["alert"])}</span>'

        trailer_kind = r["trailer_kind"]
        trailer_label = trailer_kind.capitalize() if trailer_kind else "—"
        trailer_url = r.get("trailer_url") or ""
        link = f'<a href="{html_escape(trailer_url)}" target="_blank" rel="noopener">ver</a>' if trailer_url else "—"

        body.append(f"""
<tr>
  <td>{badge}</td>
  <td>{html_escape(r["title"])}</td>
  <td class="muted">{html_escape(r["release_date"])}</td>
  <td class="muted">{html_escape(days_str)}</td>
  <td class="muted">{html_escape(trailer_label)}</td>
  <td class="right">{fmt_num(r["views"])}</td>
  <td class="right">{fmt_delta(r["views_24h_delta"])}</td>
  <td class="right">{fmt_num(r["likes"])}</td>
  <td class="right">{fmt_delta(r["likes_24h_delta"])}</td>
  <td class="right">{fmt_num(r["comments"])}</td>
  <td class="right">{fmt_delta(r["comments_24h_delta"])}</td>
  <td class="right">{fmt_delta(r["views_since_release_minus_30"])}</td>
  <td class="right">{fmt_num(r["x_tweets_7d"])}</td>
  <td class="right">{fmt_delta(r["x_tweets_24h_delta"])}</td>
  <td class="right">{fmt_num(r["x_eng_7d"])}</td>
  <td class="right">{fmt_delta(r["x_eng_24h_delta"])}</td>
  <td>{link}</td>
</tr>
""")
    tail = """
</tbody></table></div>
<footer>
  Si alguna columna aparece como "—", es porque esa fuente no está configurada (API key/token) o el dato no está disponible.
</footer>
</div></body></html>
"""
    return head + "".join(body) + tail


def make_index_html(today: datetime):
    css = """
    :root{color-scheme:dark;}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:#0b0f14;color:#e8edf2;}
    header{padding:26px 18px;border-bottom:1px solid #1b2430;background:linear-gradient(180deg,#0f1722,#0b0f14);}
    h1{margin:0;font-size:22px;font-weight:800;}
    p{margin:8px 0 0;color:#a8b3c2}
    .wrap{padding:18px;display:grid;gap:14px;max-width:980px}
    .card{background:#0f1722;border:1px solid #1b2430;border-radius:16px;padding:16px;box-shadow:0 12px 30px rgba(0,0,0,.25);}
    a{color:#7aa2ff;text-decoration:none;font-weight:700}
    a:hover{text-decoration:underline}
    .small{color:#7f8a9a;font-size:12px;margin-top:10px}
    """
    return f"""<!doctype html>
<html lang="es"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Movie Dashboard</title>
<style>{css}</style>
</head><body>
<header>
  <h1>Movie Dashboard</h1>
  <p>Estrenos argentinos · actualizado {today.strftime("%d/%m/%Y %H:%M")}</p>
</header>
<div class="wrap">
  <div class="card">
    <a href="upcoming.html">Próximos estrenos (ventana de monitoreo)</a>
    <div class="small">Películas dentro de la ventana (estreno -30 a estreno +30) que todavía no estrenaron.</div>
  </div>
  <div class="card">
    <a href="released.html">Ya estrenadas (últimos 30 días)</a>
    <div class="small">Películas dentro de la ventana que ya estrenaron.</div>
  </div>
  <div class="small">Fuentes: catálogo INCAA (CSV) + trailers oficiales (YouTube/Vimeo) + X (opcional).</div>
</div>
</body></html>
"""


# -------------------- GitHub publisher (single-file upload) --------------------

def gh_api_headers(token: str):
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "movie-dashboard-bot",
    }


def gh_get_sha(repo: str, path: str, token: str, branch: str):
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    r = requests.get(url, headers=gh_api_headers(token), params={"ref": branch}, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return (r.json() or {}).get("sha")


def gh_put_file(repo: str, path: str, content_bytes: bytes, token: str, branch: str, message: str):
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    sha = gh_get_sha(repo, path, token, branch)
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=gh_api_headers(token), data=json.dumps(payload), timeout=30)
    r.raise_for_status()
    return r.json()


def publish_pages(index_html: str, upcoming_html: str, released_html: str):
    if not (GH_TOKEN and GH_REPO and GH_PAGES_BASEURL):
        return None  # no publicar

    gh_put_file(GH_REPO, "index.html", index_html.encode("utf-8"), GH_TOKEN, GH_BRANCH, "Update dashboard (index)")
    gh_put_file(GH_REPO, "upcoming.html", upcoming_html.encode("utf-8"), GH_TOKEN, GH_BRANCH, "Update dashboard (upcoming)")
    gh_put_file(GH_REPO, "released.html", released_html.encode("utf-8"), GH_TOKEN, GH_BRANCH, "Update dashboard (released)")
    return GH_PAGES_BASEURL.rstrip("/") + "/"


# -------------------- Email (link only) --------------------

def send_email_link(dashboard_url: str, today: datetime):
    if not (EMAIL_TO and EMAIL_FROM and SMTP_HOST and SMTP_USER and SMTP_PASS):
        return

    subject = f"Dashboard estrenos argentinos – {today.strftime('%d/%m/%Y')}"
    body = f"""Dashboard actualizado ({today.strftime('%d/%m/%Y %H:%M')}):

{dashboard_url}

(En el dashboard tenés páginas separadas: próximos / ya estrenadas.)
"""
    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


# -------------------- Main --------------------

def main():
    today = now_local()
    day = today.strftime("%Y-%m-%d")

    csv_urls = [INCAA_CSV_URL_1, INCAA_CSV_URL_2]
    csv_urls = [u for u in csv_urls if u]

    conn = db_connect()
    try:
        db_init(conn)

        # 1) catálogo INCAA (estrenos + trailer oficial)
        movies, meta = load_incaa_catalog(csv_urls)
        upsert_movies(conn, movies)

        # 2) snapshot trailer (YouTube/Vimeo)
        for m in movies:
            rel = datetime.strptime(m["release_date"], "%Y-%m-%d")
            if not (rel - timedelta(days=30) <= today <= rel + timedelta(days=30)):
                continue

            kind = m.get("trailer_kind") or "—"
            url = m.get("trailer_url") or ""

            if kind == "youtube":
                vid = extract_youtube_id(url)
                stats = fetch_youtube_video_stats(vid)
            elif kind == "vimeo":
                stats = fetch_vimeo_best_effort(url)
            elif kind == "other":
                stats = {"views": None, "likes": None, "comments": None, "err": "trailer_no_soportado"}
            else:
                stats = {"views": None, "likes": None, "comments": None, "err": "sin_trailer"}

            upsert_trailer_daily(conn, day, m, stats)

        # 3) snapshot X 7d (opcional)
        for m in movies:
            rel = datetime.strptime(m["release_date"], "%Y-%m-%d")
            if not (rel - timedelta(days=30) <= today <= rel + timedelta(days=30)):
                continue
            xstats = fetch_x_7d(m["title"])
            upsert_x_daily(conn, day, m, xstats)

        # 4) construir páginas
        upcoming_rows, released_rows = build_rows(conn, movies, today)

        upcoming_html = make_html_page(
            "Próximos estrenos (aún no estrenó)",
            "Dentro de la ventana de monitoreo",
            upcoming_rows,
            today
        )
        released_html = make_html_page(
            "Ya estrenadas (últimos 30 días)",
            "Dentro de la ventana de monitoreo",
            released_rows,
            today
        )
        index_html = make_index_html(today)

        # 5) guardar local (útil para debug)
        with open(os.path.join(BASE_DIR, "index.html"), "w", encoding="utf-8") as f:
            f.write(index_html)
        with open(os.path.join(BASE_DIR, "upcoming.html"), "w", encoding="utf-8") as f:
            f.write(upcoming_html)
        with open(os.path.join(BASE_DIR, "released.html"), "w", encoding="utf-8") as f:
            f.write(released_html)

        # 6) publicar (opcional)
        published_url = publish_pages(index_html, upcoming_html, released_html)

        # 7) mail con link (opcional)
        if published_url:
            send_email_link(published_url, today)

        # Log simple (para Actions)
        print("OK: dashboard generado.")
        if published_url:
            print("Publicado en:", published_url)
        else:
            print("No se publicó (faltan GH_TOKEN / GH_REPO / GH_PAGES_BASEURL).")
        print(f"Catálogo INCAA: title_col='{meta['title_col']}', date_col='{meta['date_col']}', trailer_col='{meta['trailer_col']}', delims={meta['delims']}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
