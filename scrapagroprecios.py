import os
import sys
import glob
import re
import json
import unicodedata
import tempfile

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
BASE_DIR        = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR         = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))
FILE_TAG        = "frutihort"
PATTERN_DAILY   = os.path.join(OUT_DIR, "*.csv")
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL")
CREDS_JSON      = os.environ.get("CREDS_JSON_PATH", os.path.abspath("creds.json"))
WORKSHEET_NAME  = os.environ.get("WORKSHEET_NAME", "precios_supermercados")
MAX_WORKERS     = 8
REQ_TIMEOUT     = 20  # segundos de timeout por request

# Esquema con nueva columna ClasificaProducto
COLUMNS  = [
    'Supermercado','Producto','Precio','Unidad',
    'Grupo','Subgrupo','ClasificaProducto','FechaConsulta'
]
KEY_COLS = ['Supermercado','Producto','FechaConsulta']

os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Utilidades de texto ─────────────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)

def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

def normalize_text(txt: str) -> str:
    return strip_accents(txt).lower()

def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

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

# ─────────────── 2.1. Clasificación "fresco": ClasificaProducto ───────────────
# Se normaliza la descripción (minúsculas, sin tildes) para robustez.
# Reglas inspiradas en el JS compartido; exclusiones y categorías replicadas.

# Exclusiones generales para procesados
EXCLUSIONES_GENERALES_RE = re.compile(
    r"\b(extracto|jugo|sabor|pulpa|pure|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|mermelada|chips|snack|polvo|humo)\b"
)

# Patrones específicos por rubro "fresco"
TOMATE_EXC_RE = re.compile(
    r"(arroz\s+con\s+tomate|en\s+tomate|salsa(\s+de)?\s+tomate|ketchup|tomate\s+en\s+polvo|tomate\s+en\s+lata|extracto|jugo|pulpa|pure|congelad[oa]|deshidratad[oa])"
)
CEBOLLA_EXC_RE = re.compile(
    r"(en\s+polvo|salsa|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|crema|sopa)"
)
PAPA_EXC_RE = re.compile(
    r"(chips|frita|fritas|chuño|pure|congelad[oa]|deshidratad[oa]|harina|sopa|snack)"
)
ZANAHORIA_EXC_RE = re.compile(
    r"(jugo|pure|conserva|congelad[oa]|deshidratad[oa]|salsa|tarta|pastel|mermelada)"
)
REMOLACHA_EXC_RE = re.compile(
    r"(en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|jugo|pulpa|mermelada)"
)
BANANA_EXC_RE = re.compile(
    r"(harina|polvo|chips|frita|dulce|mermelada|batido|jugo|snack|pure|congelad[oa]|deshidratad[oa])"
)
CITRICOS_EXC_RE = re.compile(
    r"(jugo|mermelada|concentrad[oa]|esencia|sabor|pulpa|congelad[oa]|deshidratad[oa]|dulce|jarabe)"
)

def clasifica_producto(descripcion: str) -> str:
    if not descripcion:
        return ""
    d = normalize_text(descripcion)

    # Tomate fresco
    if "tomate" in d and not TOMATE_EXC_RE.search(d):
        return "Tomate fresco"

    # Morrón rojo / locote / pimiento rojo
    tiene_morron = any(k in d for k in ("morron","locote","pimiento","pimenton"))
    if tiene_morron and "rojo" in d and not re.search(r"(salsa|mole|pasta|conserva|encurtido|en\s+vinagre|en\s+lata|molid[oa]|deshidratad[oa]|congelad[oa]|pulpa|pate)", d):
        return "Morrón rojo"

    # Cebolla fresca
    if "cebolla" in d and not CEBOLLA_EXC_RE.search(d):
        return "Cebolla fresca"

    # Papa fresca
    if ("papa" in d or "patata" in d) and not PAPA_EXC_RE.search(d):
        return "Papa fresca"

    # Zanahoria fresca
    if "zanahoria" in d and not ZANAHORIA_EXC_RE.search(d):
        return "Zanahoria fresca"

    # Lechuga fresca
    if "lechuga" in d and not re.search(r"(ensalada\s+procesada|mix\s+de\s+ensaladas|congelad[oa]|deshidratad[oa])", d):
        return "Lechuga fresca"

    # Remolacha fresca
    if "remolacha" in d and not REMOLACHA_EXC_RE.search(d):
        return "Remolacha fresca"

    # Rúcula fresca
    if ("rucula" in d or "arugula" in d) and not EXCLUSIONES_GENERALES_RE.search(d):
        return "Rúcula fresca"

    # Berro fresco
    if "berro" in d and not EXCLUSIONES_GENERALES_RE.search(d):
        return "Berro fresco"

    # Banana/Plátano/Guineo fresco
    if any(k in d for k in ("banana","banano","platano","guineo")) and not BANANA_EXC_RE.search(d):
        return "Banana fresca"

    # Cítricos frescos: naranja, mandarina, pomelo, limón, apepú
    if any(k in d for k in ("naranja","mandarina","pomelo","limon","apepu")) and not CITRICOS_EXC_RE.search(d):
        return "Cítrico fresco"

    return ""

# ─────────────── 2.5. Filtro de exclusión opcional ─────────────────
EXCLUDE_PATTERNS = [r"\bcombo\b", r"\bpack\b", r"\bdisney\b"]
_ex_re = re.compile("|".join(EXCLUDE_PATTERNS), re.I)
def is_excluded(name: str) -> bool:
    return bool(_ex_re.search(name))

# ─────────────── 3. Extracción de unidad ─────────────────────────────
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

# ─────────────── 4. Normalización de precio ───────────────────────────
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

# ─────────────── 5. HTTP session robusta ───────────────────────────
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

# ─────────────── 6. Clase base scraper ───────────────────────────────
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
        rows = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futures = [pool.submit(self.parse_category, u) for u in self.category_urls()]
            for fut in as_completed(futures):
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
                    # Silencioso para no imprimir
                    pass
        return rows

    def save_csv(self, rows: List[Dict]):
        if not rows:
            return
        fn = f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR, fn), index=False)

# ─────────────── 7. Stock ───────────────────────────────
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
        kws = [tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
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

# ─────────────── 8. Superseis ───────────────────────────────
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

# ─────────────── 9. Salemma ───────────────────────────────
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

# ─────────────── 10. Arete y Jardines ─────────────────────────
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

# ─────────────── 11. Biggie (API) ─────────────────────────────
class BiggieScraper:
    name = 'biggie'
    API  = 'https://api.app.biggie.com.py/api/articles'
    TAKE = 100
    GROUPS = ['huevos','lacteos','frutas','verduras','cereales','panificados']

    def __init__(self):
        self.session = _build_session()

    def category_urls(self) -> List[str]:
        return self.GROUPS

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

    def save_csv(self, rows: List[Dict]):
        if not rows:
            return
        fn = f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR, fn), index=False)

# ─────────────── 12. Registro de scrapers ─────────────────────────
SCRAPERS = {
    'stock':       StockScraper,
    'superseis':   SuperseisScraper,
    'salemma':     SalemmaScraper,
    'arete':       AreteScraper,
    'losjardines': JardinesScraper,
    'biggie':      BiggieScraper,
}

# ─────────────── 13. Google Sheets & Orquestador ─────────────────
def _open_sheet():
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ]
    cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    gc   = gspread.authorize(cred)
    sh   = gc.open_by_url(SPREADSHEET_URL)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
        df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False).dropna(how='all')
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows='10000', cols='60')
        df = pd.DataFrame(columns=COLUMNS)
        # Garantiza encabezado inicial
        header_df = pd.DataFrame(columns=["ID"] + COLUMNS)
        set_with_dataframe(ws, header_df, include_index=False)
    return ws, df

def _align_schema(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    df = df.copy()
    # Añadir faltantes
    for c in columns:
        if c not in df.columns:
            df[c] = "" if c != "Precio" else pd.NA
    # Reordenar y limitar
    df = df[[c for c in columns]]
    return df

def _ensure_header(ws, columns: List[str]) -> None:
    # Lee encabezado actual y rehace si no coincide
    try:
        hdr = ws.row_values(1)
    except Exception:
        hdr = []
    expected = ["ID"] + columns
    if hdr != expected:
        header_df = pd.DataFrame(columns=expected)
        ws.clear()
        set_with_dataframe(ws, header_df, include_index=False)

def main() -> None:
    # 1) Ejecutar scrapers y recolectar
    all_rows: List[Dict] = []
    for cls in SCRAPERS.values():
        inst = cls()
        rows = inst.scrape()
        inst.save_csv(rows)
        all_rows.extend(rows)

    if not all_rows:
        return

    # 2) DF consolidado y formateo de FechaConsulta
    df_all = pd.DataFrame(all_rows)
    df_all = _align_schema(df_all, COLUMNS)
    df_all["FechaConsulta"] = pd.to_datetime(df_all["FechaConsulta"], errors="coerce")

    # 3) Leer hoja y alinear esquema
    ws, prev_df = _open_sheet()
    _ensure_header(ws, COLUMNS)

    if prev_df.empty:
        prev_df = pd.DataFrame(columns=COLUMNS)
    else:
        # Si la hoja ya existe con esquema anterior, alinear
        # (get_as_dataframe ya descarta la fila de encabezados)
        prev_df = _align_schema(prev_df, COLUMNS)
        prev_df["FechaConsulta"] = pd.to_datetime(prev_df["FechaConsulta"], errors="coerce")

    # 4) Detectar filas nuevas por KEY_COLS
    idx_prev = prev_df.set_index(KEY_COLS).index if not prev_df.empty else pd.Index([])
    idx_all  = df_all.set_index(KEY_COLS).index
    new_idx  = idx_all.difference(idx_prev)

    if new_idx.empty:
        return

    # 5) Preparar filas nuevas con ID consecutivo
    new_rows = df_all.set_index(KEY_COLS).loc[new_idx].reset_index()
    start_id = (len(prev_df) + 1) if not prev_df.empty else 1
    new_rows.insert(0, "ID", range(start_id, start_id + len(new_rows)))
    new_rows["FechaConsulta"] = pd.to_datetime(new_rows["FechaConsulta"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # 6) Append sin reescribir toda la hoja
    set_with_dataframe(
        ws,
        new_rows[["ID"] + COLUMNS],
        row=len(prev_df) + 2,
        include_index=False,
        include_column_header=False
    )

if __name__ == "__main__":
    main()
