# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – Py 3.7+ adaptado para GitHub Actions
Autor  : Diego B. Meza · Rev: 2025-06-29 corrección cuenta alterna

Este script:
1) Realiza scraping de supermercados Stock, Superseis, Salemma, Arete, Jardines y Biggie.
2) Clasifica cada producto en Grupo y Subgrupo.
3) Extrae unidad de medida (g, kg→g, ml, l→cc, unid., paq).
4) Registra FechaConsulta con fecha+hora+min+seg en zona UTC.
5) Consolida CSVs diarios y actualiza hoja de Google Sheets.
Columns: ['ID','Supermercado','Producto','Precio','Unidad','Grupo','Subgrupo','FechaConsulta']
"""
import os
import sys
import glob
import re
import json
import unicodedata
import tempfile

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set
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

COLUMNS  = ['Supermercado','Producto','Precio','Unidad','Grupo','Subgrupo','FechaConsulta']
KEY_COLS = ['Supermercado','Producto','FechaConsulta']

os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Utilidades de texto ─────────────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ─────────────── 2. Clasificación Grupo/Subgrupo ─────────────────
BROAD_GROUP_KEYWORDS = {
    "Panificados": ["pan","baguette","bizcocho","galleta","masa"],
    "Frutas":      ["naranja","manzana","banana","pera","uva","frutilla"],
    "Verduras":    ["tomate","cebolla","papa","zanahoria","lechuga","espinaca"],
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

def classify(name: str) -> (str, str):
    toks = set(tokenize(name))
    grp = next((g for g, ks in BROAD_TOKENS.items() if toks & ks), "")
    sub = next((s for s, ks in SUB_TOKENS.items() if toks & ks), "")
    return grp, sub

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
                            'FechaConsulta': r['FechaConsulta'],
                        })
                except Exception as e:
                    print(f"[{self.name}] WARN {e}")
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
        soup = BeautifulSoup(
            self.session.get(self.base_url, timeout=REQ_TIMEOUT).text,
            'html.parser'
        )
        kws = [tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [
            urljoin(self.base_url, a['href'])
            for a in soup.select('a[href*="/category/"]')
            if any(k in a['href'].lower() for k in kws)
        ]

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"[stock] WARN {e} -> {url}")
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for card in soup.select('div.product-item'):
            el = card.select_one('h2.product-title')
            if not el:
                continue
            nm = el.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            price = _first_price(card)
            grp, sub = classify(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            out.append({
                'Supermercado': 'stock',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub
            })
        return out

# ─────────────── 8. Superseis ───────────────────────────────
class SuperseisScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('superseis', 'https://www.superseis.com.py')

    def category_urls(self) -> List[str]:
        soup = BeautifulSoup(
            self.session.get(self.base_url, timeout=REQ_TIMEOUT).text,
            'html.parser'
        )
        kws = [tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [
            urljoin(self.base_url, a['href'])
            for a in soup.select('a.collapsed[href*="/category/"]')
            if any(k in a['href'].lower() for k in kws)
        ]

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"[superseis] WARN {e} -> {url}")
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for link in soup.select('a.product-title-link'):
            nm = link.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            parent = link.find_parent('div.product-item') or link
            price = _first_price(parent)
            grp, sub = classify(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            out.append({
                'Supermercado': 'superseis',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub
            })
        return out

# ─────────────── 9. Salemma ───────────────────────────────
class SalemmaScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('salemma', 'https://www.salemmaonline.com.py')

    def category_urls(self) -> List[str]:
        urls = set()
        try:
            resp = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"[salemma] WARN {e} -> {self.base_url}")
            return []
        for a in BeautifulSoup(resp.content, 'html.parser').find_all('a', href=True):
            h = a['href'].lower()
            if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                urls.add(urljoin(self.base_url, h))
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"[salemma] WARN {e} -> {url}")
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for f in soup.select('form.productsListForm'):
            nm = f.find('input', {'name': 'name'}).get('value', '')
            if is_excluded(nm):
                continue
            price = norm_price(f.find('input', {'name': 'price'}).get('value', ''))
            grp, sub = classify(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            out.append({
                'Supermercado': 'salemma',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub
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
        except Exception as e:
            print(f"[arete] WARN {e} -> {self.base_url}")
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        for sel in ('#departments-menu', '#menu-departments-menu-1'):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                h = a['href'].split('?')[0].lower()
                if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                    urls.add(urljoin(self.base_url + '/', h))
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"[arete] WARN {e} -> {url}")
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for card in soup.select('div.product'):
            el = card.select_one('h2.ecommercepro-loop-product__title')
            if not el:
                continue
            nm = el.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            price = _first_price(card)
            grp, sub = classify(nm)
            if not grp:
                continue
            unit = extract_unit(nm)
            out.append({
                'Supermercado': 'arete',
                'Producto':     nm.upper(),
                'Precio':       price,
                'Unidad':       unit,
                'Grupo':        grp,
                'Subgrupo':     sub
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
        out = []
        skip = 0
        while True:
            try:
                js = self.session.get(
                    self.API,
                    params={'take': self.TAKE, 'skip': skip, 'classificationName': grp},
                    timeout=REQ_TIMEOUT
                ).json()
            except Exception as e:
                print(f"[biggie] WARN {e} -> grp={grp}")
                break
            for it in js.get('items', []):
                nm = it.get('name', '')
                price = norm_price(it.get('price', 0))
                if price <= 0:
                    continue
                g, sub = classify(nm)
                unit = extract_unit(nm)
                out.append({
                    'Supermercado': 'biggie',
                    'Producto':     nm.upper(),
                    'Precio':       price,
                    'Unidad':       unit,
                    'Grupo':        g or grp.capitalize(),
                    'Subgrupo':     sub
                })
            skip += self.TAKE
            if skip >= js.get('count', 0):
                break
        return out

    def scrape(self) -> List[Dict]:
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        rows = []
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
    'stock':      StockScraper,
    'superseis':  SuperseisScraper,
    'salemma':    SalemmaScraper,
    'arete':      AreteScraper,
    'losjardines':JardinesScraper,
    'biggie':     BiggieScraper,
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
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows='10000', cols='40')
        df = pd.DataFrame(columns=COLUMNS)

    return ws, df

def _write_sheet(ws, df: pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws, df[['ID'] + COLUMNS], include_index=False)

def main() -> None:
    # 1) Scrapear y guardar CSVs...
    all_rows = [...]
    if not all_rows:
        print("Sin datos nuevos.")
        return

    # 2) Abrir Sheets y leer lo que ya hay
    ws, prev_df = _open_sheet()
    prev_df["FechaConsulta"] = pd.to_datetime(prev_df["FechaConsulta"], errors="coerce")

    # 3) Construir df_all (tus filas actuales)
    df_all = pd.DataFrame(all_rows, columns=COLUMNS)
    df_all["FechaConsulta"] = pd.to_datetime(df_all["FechaConsulta"], errors="coerce")

    # 4) Identificar SOLO las filas que NO estén en prev_df
    merged = pd.merge(
        df_all,
        prev_df[KEY_COLS],
        on=KEY_COLS,
        how="left",
        indicator=True
    )
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

    if new_rows.empty:
        print("No hay filas nuevas para añadir.")
        return

    # 5) Formatear FechaConsulta de las filas nuevas
    new_rows["FechaConsulta"] = new_rows["FechaConsulta"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 6) Append: escribe SOLO las nuevas debajo de lo existente
    #    calcula la fila de inicio (toma en cuenta cabecera)
    start_row = len(prev_df) + 2
    set_with_dataframe(
        ws,
        new_rows[['ID'] + COLUMNS],       # incluye tu columna ID si la usas
        row=start_row,
        include_index=False,
        include_column_header=False
    )

    print(f"Añadidas {len(new_rows)} filas nuevas a la hoja.")

if __name__ == "__main__":
    main()
