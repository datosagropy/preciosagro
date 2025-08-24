# -*- coding: utf-8 -*-
import os
import re
import unicodedata
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread_dataframe import get_as_dataframe  # solo leer, no escribir
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
BASE_DIR        = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR         = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))  # reservado
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL")
CREDS_JSON      = os.environ.get("CREDS_JSON_PATH", os.path.abspath("creds.json"))
WORKSHEET_NAME  = os.environ.get("WORKSHEET_NAME", "precios_supermercados")
MAX_WORKERS     = 8
REQ_TIMEOUT     = 20  # segundos

# Esquema base requerido
REQUIRED_COLUMNS = [
    'Supermercado','Producto','Precio','Unidad',
    'Grupo','Subgrupo','ClasificaProducto','FechaConsulta'
]
KEY_COLS = ['Supermercado','Producto','FechaConsulta']

# ────────────────── 1. Utilidades de texto ─────────────────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

def normalize_text(txt: str) -> str:
    return strip_accents(str(txt)).lower()

def tokenize(txt: str) -> List[str]:
    import re as _re
    return [strip_accents(t.lower()) for t in _re.findall(r"[a-záéíóúñü]+", str(txt), flags=_re.I)]

# ─────────────── 2. Clasificación Grupo/Subgrupo ─────────────────
BROAD_GROUP_KEYWORDS = {
    "Panificados": ["pan","baguette","bizcocho","galleta","masa"],
    "Frutas":      ["naranja","manzana","banana","banano","platano","plátano","pera","uva","frutilla","guineo","mandarina","pomelo","limon","limón","apepu"],
    "Verduras":    ["tomate","cebolla","papa","patata","zanahoria","lechuga","espinaca","morron","morrón","locote","pimiento","pimenton","pimentón","remolacha","rucula","rúcula","berro"],
    "Huevos":      ["huevo","huevos","codorniz"],
    "Lácteos":     ["leche","yogur","queso","manteca","crema"],
}
BROAD_TOKENS = {g:{strip_accents(w) for w in ws} for g,ws in BROAD_GROUP_KEYWORDS.items()}

SUBGROUP_KEYWORDS = {
    "Naranja": ["naranja","naranjas"],
    "Cebolla": ["cebolla","cebollas"],
    "Leche Entera": ["entera"],
    "Leche Descremada": ["descremada"],
    "Queso Paraguay": ["paraguay"],
    "Huevo Gallina": ["gallina"],
    "Huevo Codorniz": ["codorniz"],
}
SUB_TOKENS = {sg:{strip_accents(w) for w in ws} for sg,ws in SUBGROUP_KEYWORDS.items()}

def classify_group_subgroup(name: str) -> Tuple[str, str]:
    toks = set(tokenize(name))
    grp = next((g for g, ks in BROAD_TOKENS.items() if toks & ks), "")
    sub = next((s for s, ks in SUB_TOKENS.items() if toks & ks), "")
    return grp, sub

# ─────────────── 2.1. Clasificación “fresco” ───────────────
EXCLUSIONES_GENERALES_RE = re.compile(
    r"\b(extracto|jugo|sabor|pulpa|pure|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|mermelada|chips|snack|polvo|humo)\b"
)
TOMATE_EXC_RE = re.compile(
    r"(arroz\s+con\s+tomate|en\s+tomate|salsa(\s+de)?\s+tomate|ketchup|tomate\s+en\s+polvo|tomate\s+en\s+lata|extracto|jugo|pulpa|pure|congelad[oa]|deshidratad[oa])"
)
CEBOLLA_EXC_RE = re.compile(r"(en\s+polvo|salsa|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|crema|sopa)")
PAPA_EXC_RE    = re.compile(r"(chips|frita|fritas|chuño|pure|congelad[oa]|deshidratad[oa]|harina|sopa|snack)")
ZANAHORIA_EXC_RE = re.compile(r"(jugo|pure|conserva|congelad[oa]|deshidratad[oa]|salsa|tarta|pastel|mermelada)")
REMOLACHA_EXC_RE = re.compile(r"(en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|jugo|pulpa|mermelada)")
BANANA_EXC_RE    = re.compile(r"(harina|polvo|chips|frita|dulce|mermelada|batido|jugo|snack|pure|congelad[oa]|deshidratad[oa])")
CITRICOS_EXC_RE  = re.compile(r"(jugo|mermelada|concentrad[oa]|esencia|sabor|pulpa|congelad[oa]|deshidratad[oa]|dulce|jarabe)")

def clasifica_producto(descripcion: str) -> str:
    d = normalize_text(descripcion)
    if not d:
        return ""
    if "tomate" in d and not TOMATE_EXC_RE.search(d):
        return "Tomate fresco"
    tiene_morron = any(k in d for k in ("morron","locote","pimiento","pimenton"))
    if tiene_morron and "rojo" in d and not re.search(r"(salsa|mole|pasta|conserva|encurtido|en\s+vinagre|en\s+lata|molid[oa]|deshidratad[oa]|congelad[oa]|pulpa|pate)", d):
        return "Morrón rojo"
    if "cebolla" in d and not CEBOLLA_EXC_RE.search(d):
        return "Cebolla fresca"
    if ("papa" in d or "patata" in d) and not PAPA_EXC_RE.search(d):
        return "Papa fresca"
    if "zanahoria" in d and not ZANAHORIA_EXC_RE.search(d):
        return "Zanahoria fresca"
    if "lechuga" in d and not re.search(r"(ensalada\s+procesada|mix\s+de\s+ensaladas|congelad[oa]|deshidratad[oa])", d):
        return "Lechuga fresca"
    if "remolacha" in d and not REMOLACHA_EXC_RE.search(d):
        return "Remolacha fresca"
    if ("rucula" in d or "arugula" in d) and not EXCLUSIONES_GENERALES_RE.search(d):
        return "Rúcula fresca"
    if "berro" in d and not EXCLUSIONES_GENERALES_RE.search(d):
        return "Berro fresco"
    if any(k in d for k in ("banana","banano","platano","guineo")) and not BANANA_EXC_RE.search(d):
        return "Banana fresca"
    if any(k in d for k in ("naranja","mandarina","pomelo","limon","apepu")) and not CITRICOS_EXC_RE.search(d):
        return "Cítrico fresco"
    return ""

# ─────────────── 2.5. Exclusiones ─────────────────
EXCLUDE_PATTERNS = [r"\bcombo\b", r"\bpack\b", r"\bdisney\b"]
_ex_re = re.compile("|".join(EXCLUDE_PATTERNS), re.I)
def is_excluded(name: str) -> bool:
    return bool(_ex_re.search(name))

# ─────────────── 3. Unidad ─────────────────────────────
_unit_re = re.compile(
    r"(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilos?|g|gr|ml|cc|l(?:itro)?s?|lt|unid(?:ad)?s?|u|paq|stk)\b",
    re.I
)
def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    if not m:
        return ""
    val = float(m.group('val').replace(',', '.'))
    unit = m.group('unit').lower().rstrip('s')
    if unit in ('kg', 'kilo'):
        val *= 1000; unit_out = 'GR'
    elif unit in ('l', 'lt', 'litro'):
        val *= 1000; unit_out = 'CC'
    elif unit in ('g', 'gr'):
        unit_out = 'GR'
    elif unit in ('ml', 'cc'):
        unit_out = 'CC'
    else:
        unit_out = unit.upper()
    val_str = str(int(val)) if val.is_integer() else f"{val:.2f}".rstrip('0').rstrip('.')
    return f"{val_str}{unit_out}"

# ─────────────── 4. Precio ───────────────────────────
_price_selectors = [
    "[data-price]","[data-price-final]","[data-price-amount]",
    "meta[itemprop='price']","span.price ins span.amount","span.price > span.amount",
    "span.woocommerce-Price-amount","span.amount","bdi","div.price","p.price"
]
def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace('.', '').replace(',', '.')
    try:
        return float(txt)
    except:
        return 0.0

def _first_price(node: Tag) -> float:
    for attr in ("data-price","data-price-final","data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr])>0:
            return norm_price(node[attr])
    meta = node.select_one("meta[itemprop='price']")
    if meta and norm_price(meta.get('content',''))>0:
        return norm_price(meta.get('content',''))
    for sel in _price_selectors:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get(sel,''))
            if p>0:
                return p
    return 0.0

# ─────────────── 5. HTTP session ───────────────────────────
def _build_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","HEAD")
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "es-ES,es;q=0.9",
    })
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

# ─────────────── 6. Clase base ───────────────────────────────
class HtmlSiteScraper:
    def __init__(self, name: str, base: str):
        self.name = name
        self.base_url = base.rstrip('/')
        self.session = _build_session()

    def category_urls(self) -> List[str]:
        raise NotImplementedError

    def parse_category(self, url: str) -> List[Dict]:
        raise NotImplementedError

    def scrape(self) -> List[Dict]:
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        rows: List[Dict] = []
        urls = self.category_urls()
        if not urls:
            return rows
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            for fut in as_completed([pool.submit(self.parse_category, u) for u in urls]):
                try:
                    for r in fut.result():
                        if r.get("Precio", 0) <= 0:
                            continue
                        r["FechaConsulta"] = ts
                        rows.append({
                            'Supermercado': r['Supermercado'],
                            'Producto':      r['Producto'],
                            'Precio':        r['Precio'],
                            'Unidad':        r['Unidad'],
                            'Grupo':         r['Grupo'],
                            'Subgrupo':      r['Subgrupo'],
                            'ClasificaProducto': r.get('ClasificaProducto', ''),
                            'FechaConsulta': r['FechaConsulta'],
                        })
                except Exception:
                    pass
        return rows

# ─────────────── 7. Scrapers ───────────────────────────────
class StockScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('stock', 'https://www.stock.com.py')

    def category_urls(self) -> List[str]:
        try:
            soup = BeautifulSoup(
                self.session.get(self.base_url, timeout=REQ_TIMEOUT).text,
                'html.parser'
            )
        except Exception:
            return []
        kws = [t for lst in BROAD_GROUP_KEYWORDS.values() for t in lst]
        return [
            urljoin(self.base_url, a['href'])
            for a in soup.select('a[href*="/category/"]')
            if a.has_attr('href') and any(k in a['href'].lower() for k in kws)
        ]

    def parse_category(self, url: str) -> List[Dict]:
        out: List[Dict] = []
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return out
        soup = BeautifulSoup(resp.content, 'html.parser')
        for card in soup.select('div.product-item'):
            el = card.select_one('h2.product-title')
            if not el:
                continue
            nm = el.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            price = _first_price(card)
            grp, sub = classify_group_subgroup(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            cp = clasifica_producto(nm)
            out.append({
                'Supermercado': 'stock',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub,
                'ClasificaProducto': cp
            })
        return out

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("superseis", "https://www.superseis.com.py")

    def category_urls(self) -> List[str]:
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        return list({
            urljoin(self.base_url, a["href"])
            for a in soup.find_all("a", href=True, class_="collapsed")
            if "/category/" in a.get("href","")
        })

    def parse_category(self, url: str) -> List[dict]:
        regs: List[dict] = []
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return regs
        soup = BeautifulSoup(r.content, "html.parser")
        for a in soup.find_all("a", class_="product-title-link"):
            nombre = a.get_text(strip=True)
            if is_excluded(nombre):
                continue
            cont = a.find_parent("div", class_="product-item")
            tag = (cont and cont.find("span", class_="price-label")) or a.find_next("span", class_="price-label")
            precio = norm_price(tag.get_text()) if tag else 0.0
            if precio <= 0:
                continue
            grp, sub = classify_group_subgroup(nombre)
            if not grp:
                continue
            unidad = extract_unit(nombre)
            cp = clasifica_producto(nombre)
            regs.append({
                "Supermercado":   self.name,
                "Producto":       nombre.upper(),
                "Precio":         precio,
                "Unidad":         unidad,
                "Grupo":          grp,
                "Subgrupo":       sub,
                "ClasificaProducto": cp
            })
        return regs

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('salemma', 'https://www.salemmaonline.com.py')

    def category_urls(self) -> List[str]:
        urls = set()
        try:
            resp = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        for a in BeautifulSoup(resp.content, 'html.parser').find_all('a', href=True):
            h = a['href'].lower()
            if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                urls.add(urljoin(self.base_url, h))
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        out: List[Dict] = []
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return out
        soup = BeautifulSoup(resp.content, 'html.parser')
        for f in soup.select('form.productsListForm'):
            inp_name = f.find('input', {'name': 'name'})
            if not inp_name:
                continue
            nm = inp_name.get('value', '')
            if is_excluded(nm):
                continue
            price = norm_price((f.find('input', {'name': 'price'}) or {}).get('value', ''))
            grp, sub = classify_group_subgroup(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            cp = clasifica_producto(nm)
            out.append({
                'Supermercado': 'salemma',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub,
                'ClasificaProducto': cp
            })
        return out

class AreteScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('arete', 'https://www.arete.com.py')

    def category_urls(self) -> List[str]:
        urls = set()
        try:
            resp = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        for sel in ('#departments-menu', '#menu-departments-menu-1'):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                h = a['href'].split('?')[0].lower()
                if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                    urls.add(urljoin(self.base_url + '/', h))
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        out: List[Dict] = []
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return out
        soup = BeautifulSoup(resp.content, 'html.parser')
        for card in soup.select('div.product'):
            el = card.select_one('h2.ecommercepro-loop-product__title')
            if not el:
                continue
            nm = el.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            price = _first_price(card)
            grp, sub = classify_group_subgroup(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            cp = clasifica_producto(nm)
            out.append({
                'Supermercado': 'arete',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub,
                'ClasificaProducto': cp
            })
        return out

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = 'losjardines'
        self.base_url = 'https://losjardinesonline.com.py'

class BiggieScraper:
    name = 'biggie'
    API  = 'https://api.app.biggie.com.py/api/articles'
    TAKE = 100
    GROUPS = ['huevos','lacteos','frutas','verduras','cereales','panificados']

    def __init__(self):
        self.session = _build_session()

    def parse_category(self, grp: str) -> List[Dict]:
        out: List[Dict] = []
        skip = 0
        while True:
            try:
                js = self.session.get(
                    self.API,
                    params={'take': self.TAKE, 'skip': skip, 'classificationName': grp},
                    timeout=REQ_TIMEOUT
                ).json()
            except Exception:
                break
            for it in js.get('items', []):
                nm = it.get('name', '')
                price = norm_price(it.get('price', 0))
                if price <= 0:
                    continue
                g, sub = classify_group_subgroup(nm)
                unit = extract_unit(nm)
                cp = clasifica_producto(nm)
                out.append({
                    'Supermercado': 'biggie',
                    'Producto':     nm.upper(),
                    'Precio':       price,
                    'Unidad':       unit,
                    'Grupo':        g or grp.capitalize(),
                    'Subgrupo':     sub,
                    'ClasificaProducto': cp
                })
            skip += self.TAKE
            if skip >= js.get('count', 0):
                break
        return out

    def scrape(self) -> List[Dict]:
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        rows: List[Dict] = []
        for grp in self.GROUPS:
            for r in self.parse_category(grp):
                r['FechaConsulta'] = ts
                rows.append(r)
        return rows

# Registro de scrapers
SCRAPERS = {
    'stock':       StockScraper,
    'superseis':   SuperseisScraper,
    'salemma':     SalemmaScraper,
    'arete':       AreteScraper,
    'losjardines': JardinesScraper,
    'biggie':      BiggieScraper,
}

# ─────────────── 13. Google Sheets (partición mensual, sin resize masivo) ─────────────────
def _authorize_sheet():
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ]
    cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    gc   = gspread.authorize(cred)
    sh   = gc.open_by_url(SPREADSHEET_URL)
    return sh

def _ensure_worksheet(sh, title: str):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows='2', cols=str(len(REQUIRED_COLUMNS)))
        ws.update('A1', [REQUIRED_COLUMNS])
    return ws

def _ensure_required_columns(ws) -> List[str]:
    header = ws.row_values(1)
    header = [h for h in header if h]
    if not header:
        header = REQUIRED_COLUMNS.copy()
        ws.update('A1', [header])
        return header
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        new_header = header + missing
        # ajustar columnas si faltan
        if ws.col_count < len(new_header):
            try:
                ws.add_cols(len(new_header) - ws.col_count)
            except Exception:
                pass
        ws.update('A1', [new_header])
        return new_header
    return header

def _align_df_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = "" if c != "Precio" else pd.NA
    return out[columns]

def _get_existing_df(ws) -> pd.DataFrame:
    # lectura segura de toda la hoja (hojas mensuales deben ser pequeñas)
    df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False).dropna(how='all')
    return df if not df.empty else pd.DataFrame(columns=ws.row_values(1))

def _append_rows(ws, df: pd.DataFrame):
    # Convierte NaN a "" y fechas a texto
    if "FechaConsulta" in df.columns:
        df["FechaConsulta"] = pd.to_datetime(df["FechaConsulta"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    values = df.where(pd.notnull(df), "").values.tolist()
    # Append en lotes para ser prudentes con el tamaño de request
    chunk = 5000
    for i in range(0, len(values), chunk):
        ws.append_rows(values[i:i+chunk], value_input_option="USER_ENTERED")

def _shrink_grid(ws, nrows: int, ncols: int):
    # Compacta la grilla para no acumular celdas vacías (reduce el total de celdas del workbook)
    try:
        ws.resize(rows=max(1, nrows), cols=max(1, ncols))
    except Exception:
        # si falla (permisos/otros), simplemente continuar
        pass

def main() -> None:
    # 1) Ejecutar scrapers y recolectar
    all_rows: List[Dict] = []
    for _, cls in SCRAPERS.items():
        try:
            rows = cls().scrape()
            if rows:
                all_rows.extend(rows)
        except Exception:
            # tolerante a fallos por sitio
            pass

    if not all_rows:
        return

    # 2) Consolidado y tipos
    df_all = pd.DataFrame(all_rows)
    # normalizar tipos
    df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce")
    df_all = df_all[df_all["Precio"] > 0]
    df_all["FechaConsulta"] = pd.to_datetime(df_all["FechaConsulta"], errors="coerce")
    df_all = df_all.drop_duplicates(subset=KEY_COLS, keep="last")

    # 3) Determinar partición mensual
    # En una corrida normal todos comparten el mismo mes
    if df_all["FechaConsulta"].notna().any():
        part = df_all["FechaConsulta"].dt.strftime("%Y%m").iloc[0]
    else:
        part = datetime.now(timezone.utc).strftime("%Y%m")
    target_title = f"{WORKSHEET_NAME}_{part}"

    # 4) Abrir workbook y hoja mensual
    sh = _authorize_sheet()
    ws = _ensure_worksheet(sh, target_title)
    header = _ensure_required_columns(ws)

    # 5) Leer existente y alinear
    prev_df = _get_existing_df(ws)
    if prev_df.empty:
        prev_df = pd.DataFrame(columns=header)
    prev_df = _align_df_columns(prev_df, header)
    df_all  = _align_df_columns(df_all, header)

    # 6) Detección de nuevas filas por clave
    for c in KEY_COLS:
        if c not in prev_df.columns:
            prev_df[c] = ""
    if not prev_df.empty:
        prev_keys = set(prev_df[KEY_COLS].astype(str).agg('|'.join, axis=1))
    else:
        prev_keys = set()
    all_keys = df_all[KEY_COLS].astype(str).agg('|'.join, axis=1)
    mask_new = ~all_keys.isin(prev_keys)
    new_rows = df_all.loc[mask_new].copy()
    if new_rows.empty:
        # compactar (opcional) por si la hoja quedó grande
        _shrink_grid(ws, nrows=len(prev_df)+1, ncols=len(header))
        return

    # 7) Ordenar columnas según encabezado y anexar
    cols_to_write = [c for c in header if c in new_rows.columns]
    new_rows = new_rows[cols_to_write]
    _append_rows(ws, new_rows)

    # 8) Compactar grilla para contener exactamente datos + encabezado
    total_rows = 1 + len(prev_df) + len(new_rows)
    _shrink_grid(ws, nrows=total_rows, ncols=len(header))

if __name__ == "__main__":
    main()
