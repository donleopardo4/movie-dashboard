import os
import re
import csv
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Entrada/salida por defecto
IN_FILE_DEFAULT = "incaa_historico_raw.csv"         # lo que pegás/exportás desde Excel
OUT_FILE_DEFAULT = "incaa_historico_manual.csv"     # lo que usa main.py

def norm_header(h: str) -> str:
    if h is None:
        return ""
    h = str(h).replace("\ufeff", "").replace("\u00a0", " ")
    h = h.strip().lower()
    h = re.sub(r"\s+", " ", h)
    return h

def pick_col(headers, keywords):
    for h in headers:
        nh = norm_header(h)
        for kw in keywords:
            if kw in nh:
                return h
    return None

def to_int_any(x):
    """Convierte '12.345', '$ 1.234.567', '1,234,567', '12345' -> int"""
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("\u00a0", " ")
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"[^\d]", "", s)
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None

def sniff_delimiter(text: str) -> str:
    sample = text[:5000]
    # preferimos ; si parece archivo latino
    return ";" if sample.count(";") > sample.count(",") else ","

def read_rows_any_delim(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        text = f.read()
    delim = sniff_delimiter(text)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        rows = list(reader)
        headers = reader.fieldnames or []
    return rows, headers, delim

def main():
    in_file = sys.argv[1] if len(sys.argv) >= 2 else IN_FILE_DEFAULT
    out_file = sys.argv[2] if len(sys.argv) >= 3 else OUT_FILE_DEFAULT

    in_path = os.path.join(BASE_DIR, in_file)
    out_path = os.path.join(BASE_DIR, out_file)

    if not os.path.exists(in_path):
        print(f"ERROR: No existe '{in_file}' en {BASE_DIR}")
        print("Tip: exportá/guardá desde Excel como CSV y ponelo con ese nombre en la carpeta del proyecto.")
        sys.exit(1)

    rows, headers, delim = read_rows_any_delim(in_path)
    if not rows or not headers:
        print(f"ERROR: '{in_file}' no tiene filas o encabezados.")
        sys.exit(1)

    # Detectamos columnas típicas de INCAA / fiscalización / excel
    col_title = pick_col(headers, ["titulo", "título", "pelicula", "película", "obra", "film", "nombre"])
    col_ent   = pick_col(headers, ["entradas", "espectadores", "publico", "público"])
    col_rec   = pick_col(headers, ["recaud", "taquilla", "importe", "monto"])
    col_pan   = pick_col(headers, ["pantalla", "pantallas", "copias"])
    col_corte = pick_col(headers, ["fecha", "corte", "actualizacion", "actualización"])

    if not col_title or not col_ent:
        print("ERROR: No pude detectar columnas mínimas.")
        print("Encabezados detectados:", headers)
        print("Necesito al menos: TITULO + ENTRADAS (o ESPECTADORES/PÚBLICO).")
        sys.exit(1)

    # Normalizamos y deduplicamos quedándonos con el mayor entradas por título
    out_map = {}
    corte_val = None

    for r in rows:
        title = (r.get(col_title) or "").strip()
        if not title:
            continue

        ent = to_int_any(r.get(col_ent))
        rec = to_int_any(r.get(col_rec)) if col_rec else None
        pan = to_int_any(r.get(col_pan)) if col_pan else None

        if col_corte and not corte_val:
            corte_val = (r.get(col_corte) or "").strip() or None

        key = title.strip().lower()
        prev = out_map.get(key)

        if prev is None:
            out_map[key] = {
                "TITULO": title,
                "ENTRADAS_ACUMULADAS": ent,
                "RECAUDACION_ACUMULADA": rec,
                "PANTALLAS": pan
            }
        else:
            prev_ent = prev.get("ENTRADAS_ACUMULADAS") or 0
            new_ent = ent or 0
            if new_ent >= prev_ent:
                out_map[key] = {
                    "TITULO": title,
                    "ENTRADAS_ACUMULADAS": ent,
                    "RECAUDACION_ACUMULADA": rec,
                    "PANTALLAS": pan
                }

    # Fecha de corte: si no viene, ponemos hoy
    fecha_corte = None
    # si corte_val parece fecha, la usamos; si no, dejamos hoy
    if corte_val:
        # intentamos extraer YYYY-MM-DD o DD/MM/YYYY
        m = re.search(r"(\d{4}-\d{2}-\d{2})", corte_val)
        if m:
            fecha_corte = m.group(1)
        else:
            m = re.search(r"(\d{2}/\d{2}/\d{4})", corte_val)
            if m:
                try:
                    fecha_corte = datetime.strptime(m.group(1), "%d/%m/%Y").date().isoformat()
                except Exception:
                    fecha_corte = None

    if not fecha_corte:
        fecha_corte = datetime.now().date().isoformat()

    # Escribimos CSV final
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["TITULO", "ENTRADAS_ACUMULADAS", "RECAUDACION_ACUMULADA", "PANTALLAS", "FECHA_CORTE"])
        for _, v in sorted(out_map.items(), key=lambda kv: (kv[1]["TITULO"].lower())):
            writer.writerow([
                v["TITULO"],
                v["ENTRADAS_ACUMULADAS"] if v["ENTRADAS_ACUMULADAS"] is not None else "",
                v["RECAUDACION_ACUMULADA"] if v["RECAUDACION_ACUMULADA"] is not None else "",
                v["PANTALLAS"] if v["PANTALLAS"] is not None else "",
                fecha_corte
            ])

    print("OK")
    print(f"- Leí:  {in_file} (delimitador '{delim}')")
    print(f"- Escribí: {out_file}")
    print(f"- Filas: {len(out_map)}")
    print(f"- FECHA_CORTE: {fecha_corte}")
    print("\nColumnas detectadas:")
    print(f"- TITULO: {col_title}")
    print(f"- ENTRADAS: {col_ent}")
    print(f"- RECAUDACION: {col_rec or '—'}")
    print(f"- PANTALLAS: {col_pan or '—'}")
    print(f"- CORTE/FECHA: {col_corte or '—'}")

if __name__ == "__main__":
    main()
