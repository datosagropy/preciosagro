# -*- coding: utf-8 -*-
import os
import re
import unicodedata
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread_dataframe import get_as_dataframe  # sólo lectura
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
BASE_DIR        = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR         = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))  # reservado
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL")
CREDS_JSON      = os.environ.get("CREDS_JSON_PATH", os.path.abspath("creds.json"))
WORKSHEET_NAME  = os.environ.get("WORKSHEET_NAME", "precios_supermercados")
MAX_WORKERS     = 8
REQ_TIMEOUT     = 20  # segundos

# Esquema base requerido (añadimos campos nuevos solicitados)
REQUIRED_COLUMNS = [
    'Supermercado','Producto','Precio',
    'Unidad',
    'Unidad_corr','Cantidad','Unidades_Separado','Precio_comparable',
    'Grupo','Subgrupo','ClasificaProducto',
    'FechaConsulta'
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

# ─────────────── 2. Clasificación (mejorada) ─────────────────
TAXONOMY: Dict[str, Dict[str, List[str]]] = {
    "Verduras": {
        "Tomate": ["tomate", "perita", "cherry"],
        "Cebolla": ["cebolla"],
        "Papa": ["papa", "patata"],
        "Zanahoria": ["zanahoria"],
        "Lechuga": ["lechuga", "capuchina", "mantecosa", "romana"],
        "Morron": ["morron", "locote", "pimiento", "pimenton"],
        "Remolacha": ["remolacha"],
        "Rucula": ["rucula", "arugula"],
        "Berro": ["berro"],
        "Pepino": ["pepino"],
        "Ajo": ["ajo"],
        "Zapallo": ["zapallo", "calabaza"],
        "Repollo": ["repollo"],
        "Cebollita de verdeo": ["verdeo", "cebollita"],
    },
    "Frutas": {
        "Banana": ["banana", "banano", "platano", "guineo"],
        "Naranja": ["naranja", "apepu"],
        "Mandarina": ["mandarina"],
        "Pomelo": ["pomelo"],
        "Limon": ["limon"],
        "Manzana": ["manzana"],
        "Pera": ["pera"],
        "Uva": ["uva"],
        "Frutilla": ["frutilla", "fresa"],
        "Anana": ["anana", "piña"],
        "Sandia": ["sandia"],
        "Melon": ["melon"],
    },
    "Lacteos": {
        "Leche Entera": ["leche entera", "entera", "3%", "3,0%", "3.0%"],
        "Leche Descremada": ["leche descremada", "descremada", "0%", "0,5%", "0.5%"],
        "Yogur": ["yogur", "yoghurt", "yogurt"],
        "Queso Paraguay": ["queso paraguay", "paraguay"],
        "Queso Mozzarella": ["mozzarella", "muzzarella"],
        "Queso Ricotta": ["ricotta"],
        "Queso Cuartirolo": ["cuartirolo"],
        "Manteca": ["manteca", "mantequilla"],
        "Crema de leche": ["crema de leche", "nata"],
    },
    "Huevos": {
        "Huevo Gallina": ["huevo", "huevos", "docena", "media docena"],
        "Huevo Codorniz": ["codorniz"],
    },
    "Panificados": {
        "Pan": ["pan", "baguette", "lactal", "sandwich"],
        "Galleta": ["galleta", "cracker", "cookies"],
        "Bizcocho": ["bizcocho", "factura"],
        "Tortilla": ["tortilla"],
    },
    "Cereales y Granos": {
        "Arroz": ["arroz"],
        "Fideos": ["fideo", "pasta", "spaghetti", "tallarines"],
        "Harina": ["harina", "trigo 000", "000", "0000"],
        "Avena": ["avena"],
        "Legumbres": ["lenteja", "poroto", "garbanzo"],
    },
    "Aceites y Grasas": {
        "Aceite Girasol": ["aceite girasol"],
        "Aceite Soja": ["aceite soja", "aceite soya"],
        "Aceite Maiz": ["aceite maiz", "aceite maíz"],
        "Aceite Oliva": ["aceite oliva", "oliva extra virgen", "extra virgen"],
    },
    "Dulces y Azucares": {
        "Azucar": ["azucar", "azúcar"],
        "Dulce de leche": ["dulce de leche"],
        "Mermelada": ["mermelada"],
        "Cacao": ["cacao", "chocolate en polvo"],
    },
    "Bebidas": {
        "Agua": ["agua", "agua mineral"],
        "Gaseosa": ["gaseosa", "cola", "coca cola", "pepsi"],
        "Jugo": ["jugo", "zumo", "nectar", "néctar"],
        "Cerveza": ["cerveza"],
        "Vino": ["vino"],
    },
}

EXCLUSIONES_GENERALES_RE = re.compile(
    r"\b(extracto|jugo|sabor|pulpa|pure|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|mermelada|chips|snack|polvo|humo)\b"
)
TOMATE_EXC_RE   = re.compile(r"(arroz\s+con\s+tomate|en\s+tomate|salsa(\s+de)?\s+tomate|ketchup|tomate\s+en\s+polvo|tomate\s+en\s+lata|extracto|jugo|pulpa|pure|congelad[oa]|deshidratad[oa])")
CEBOLLA_EXC_RE  = re.compile(r"(en\s+polvo|salsa|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|crema|sopa)")
PAPA_EXC_RE     = re.compile(r"(chips|frita|fritas|chuño|pure|congelad[oa]|deshidratad[oa]|harina|sopa|snack)")
ZANAHORIA_EXC_RE= re.compile(r"(jugo|pure|conserva|congelad[oa]|deshidratad[oa]|salsa|tarta|pastel|mermelada)")
REMOLACHA_EXC_RE= re.compile(r"(en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|jugo|pulpa|mermelada)")
BANANA_EXC_RE   = re.compile(r"(harina|polvo|chips|frita|dulce|mermelada|batido|jugo|snack|pure|congelad[oa]|deshidratad[oa])")
CITRICOS_EXC_RE = re.compile(r"(jugo|mermelada|concentrad[oa]|esencia|sabor|pulpa|congelad[oa]|deshidratad[oa]|dulce|jarabe)")

def clasifica_producto(descripcion: str) -> str:
    d = normalize_text(descripcion)
    if not d:
        return ""
    if "tomate" in d and not TOMATE_EXC_RE.search(d):
        return "Tomate fresco"
    if any(k in d for k in ("morron","locote","pimiento","pimenton")) and "rojo" in d \
       and not re.search(r"(salsa|mole|pasta|conserva|encurtido|en\s+vinagre|en\s+lata|molid[oa]|deshidratad[oa]|congelad[oa]|pulpa|pate)", d):
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

def classify_group_subgroup(name: str) -> Tuple[str, str]:
    d = normalize_text(name)
    best_g, best_s, best_score = "", "", 0
    for g, subs in TAXONOMY.items():
        for s, pats in subs.items():
            score = 0
            for p in pats:
                if re.search(rf"\b{re.escape(p)}\b", d):
                    score += 3
                elif p in d:
                    score += 1
            if g == "Lacteos" and "leche" in d:
                score += 1
            if g == "Huevos" and ("docena" in d or "huevo" in d):
                score += 1
            if score > best_score:
                best_g, best_s, best_score = g, s, score
    if not best_g:
        for g, subs in TAXONOMY.items():
            if any(any(k in d for k in pats) for pats in subs.values()):
                best_g = g
                break
    return best_g, best_s

# ─────────────── 2.5. Exclusiones en nombre ─────────────────
EXCLUDE_PATTERNS = [r"\bcombo\b", r"\bpack\b\s*\b(oferta|ahorro|promo)\b", r"\bdisney\b"]
_ex_re = re.compile("|".join(EXCLUDE_PATTERNS), re.I)
def is_excluded(name: str) -> bool:
    return bool(_ex_re.search(name))

# ─────────────── 3. Unidades avanzadas ─────────────────────────────
RE_MULTIPACK = re.compile(
    r"(?:(?P<count>\d+)\s*[x×]\s*)?(?P<size>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilo|g|gr|l|lt|litro|ml|cc)\b",
    re.I
)
RE_FRACTION = re.compile(r"(?P<num>\d+)\s*/\s*(?P<den>\d+)\s*(?P<unit>kg|kilo|l|lt|litro)\b", re.I)
RE_UNITS    = re.compile(r"(?:\b(?:x|de)\s*)?(?P<count>\d+)\s*(?P<unit>uni(?:d)?|u|paq|paquete)s?\b", re.I)
RE_DOCENA   = re.compile(r"\b(1\/2\s+docena|media\s+docena|docena)\b", re.I)

def _to_float(num_str: str) -> float:
    s = str(num_str).replace('.', '').replace(',', '.')
    try: return float(s)
    except Exception: return 0.0

def _norm_unit_symbol(u: str) -> str:
    u = u.lower()
    if u in ("kg","kilo"): return "GR"
    if u in ("g","gr"):    return "GR"
    if u in ("l","lt","litro"): return "ML"
    if u in ("ml","cc"):   return "ML"
    if u in ("uni","unid","u"): return "UNID"
    if u in ("paq","paquete"):   return "PAQ"
    return u.upper()

def parse_unidad_corr(nombre: str) -> Tuple[str, Optional[float], str]:
    d = normalize_text(nombre)

    m = RE_MULTIPACK.search(d)
    if m:
        count = int(m.group('count')) if m.group('count') else 1
        size  = _to_float(m.group('size'))
        unit_sym = _norm_unit_symbol(m.group('unit'))
        if unit_sym == "GR":
            total = count * (size * (1000.0 if re.search(r"\bkg|kilo\b", m.group('unit'), re.I) else 1.0))
            return f"{int(round(total))}GR", float(total), "GR"
        if unit_sym == "ML":
            total = count * (size * (1000.0 if re.search(r"\bl|lt|litro\b", m.group('unit'), re.I) else 1.0))
            return f"{int(round(total))}ML", float(total), "ML"

    mf = RE_FRACTION.search(d)
    if mf:
        num  = _to_float(mf.group('num'))
        den  = _to_float(mf.group('den'))
        unit_sym = _norm_unit_symbol(mf.group('unit'))
        frac = (num/den) if den else 0.0
        if unit_sym in ("GR","ML"):
            total = frac*1000.0
            return f"{int(round(total))}{unit_sym}", float(total), unit_sym

    md = RE_DOCENA.search(d)
    if md:
        txt = md.group(1)
        if "1/2" in txt or "media" in txt:
            return "6UNID", 6.0, "UNID"
        else:
            return "12UNID", 12.0, "UNID"

    mu = RE_UNITS.search(d)
    if mu:
        count = int(mu.group('count'))
        unit_sym = _norm_unit_symbol(mu.group('unit'))
        return f"{count}{unit_sym}", float(count), "UNID" if unit_sym == "UNID" else unit_sym

    simple = re.search(r"(?P<size>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilo|g|gr|l|lt|litro|ml|cc)\b", d, re.I)
    if simple:
        size = _to_float(simple.group('size'))
        unit_sym = _norm_unit_symbol(simple.group('unit'))
        if unit_sym == "GR":
            total = size * (1000.0 if re.search(r"\bkg|kilo\b", simple.group('unit'), re.I) else 1.0)
            return f"{int(round(total))}GR", float(total), "GR"
        if unit_sym == "ML":
            total = size * (1000.0 if re.search(r"\bl|lt|litro\b", simple.group('unit'), re.I) else 1.0)
            return f"{int(round(total))}ML", float(total), "ML"

    return "", None, ""

def extract_unit_basic(name: str) -> str:
    u, _, _ = parse_unidad_corr(name)
    return u

def precio_comparable(precio: float, cantidad: Optional[float], unidad_sep: str) -> Optional[float]:
    try:
        p = float(precio)
    except Exception:
        return None
    if not cantidad or cantidad <= 0:
        return None
    u = (unidad_sep or "").upper()
    if u in ("GR","ML"):
        return p * (1000.0 / cantidad)  # por kg o por litro
    if u in ("UNID","PAQ"):
        return p / cantidad
    return None

# ─────────────── 4. Precio web ───────────────────────────
_price_selectors = [
    "[data-price]","[data-price-final]","[data-price-amount]",
    "meta[itemprop='price']","span.price ins span.amount","span.price > span.amount",
    "span.woocommerce-Price-amount","span.amount","bdi","div.price","p.price"
]
def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace('.', '').replace(',', '.')
    try: return float(txt)
    except Exception: return 0.0

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
            if p>0: return p
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

    def _assemble_row(self, nombre: str, precio: float) -> Optional[Dict]:
        if is_excluded(nombre):
            return None
        grp, sub = classify_group_subgroup(nombre)
        if not grp:
            return None
        clasif = clasifica_producto(nombre)
        unidad_simple = extract_unit_basic(nombre)
        unidad_corr, cantidad, unidades_sep = parse_unidad_corr(nombre)
        pcomp = precio_comparable(precio, cantidad, unidades_sep)
        return {
            'Supermercado': self.name,
            'Producto':     nombre.upper(),
            'Precio':       precio,
            'Unidad':       unidad_simple,
            'Unidad_corr':  unidad_corr,
            'Cantidad':     cantidad if cantidad is not None else "",
            'Unidades_Separado': unidades_sep,
            'Precio_comparable': pcomp if pcomp is not None else "",
            'Grupo':        grp,
            'Subgrupo':     sub,
            'ClasificaProducto': clasif
        }

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
                        if r and float(r.get("Precio", 0)) > 0:
                            r["FechaConsulta"] = ts
                            rows.append(r)
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
        kws = [t for lst in TAXONOMY["Verduras"].values() for t in lst] + \
              [t for lst in TAXONOMY["Frutas"].values() for t in lst] + \
              ["leche","yogur","queso","huevo","harina","arroz","aceite","azucar","agua","gaseosa","jugo"]
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
            nombre = el.get_text(' ', strip=True)
            precio = _first_price(card)
            row = self._assemble_row(nombre, precio)
            if row:
                out.append(row)
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
            cont = a.find_parent("div", class_="product-item")
            tag = (cont and cont.find("span", class_="price-label")) or a.find_next("span", class_="price-label")
            precio = norm_price(tag.get_text()) if tag else 0.0
            row = self._assemble_row(nombre, precio)
            if row:
                regs.append(row)
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
            if any(tok in h for tok in ("fruta","verdura","lacte","queso","yogur","leche","huevo","arroz","harina","aceite","azucar","bebida","gaseosa","jugo","agua")):
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
            nombre = inp_name.get('value', '')
            price_input = f.find('input', {'name': 'price'})
            precio = norm_price(price_input.get('value', '')) if price_input else 0.0
            row = self._assemble_row(nombre, precio)
            if row:
                out.append(row)
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
                if any(tok in h for tok in ("fruta","verdura","lacte","queso","yogur","leche","huevo","arroz","harina","aceite","azucar","bebida","jugo","agua","gaseosa")):
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
            nombre = el.get_text(' ', strip=True)
            precio = _first_price(card)
            row = self._assemble_row(nombre, precio)
            if row:
                out.append(row)
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
                nombre = it.get('name', '')
                precio = norm_price(it.get('price', 0))
                grp_det, sub = classify_group_subgroup(nombre)
                clasif = clasifica_producto(nombre)
                unidad_simple = extract_unit_basic(nombre)
                unidad_corr, cantidad, unidades_sep = parse_unidad_corr(nombre)
                pcomp = precio_comparable(precio, cantidad, unidades_sep)
                out.append({
                    'Supermercado': 'biggie',
                    'Producto':     nombre.upper(),
                    'Precio':       precio,
                    'Unidad':       unidad_simple,
                    'Unidad_corr':  unidad_corr,
                    'Cantidad':     cantidad if cantidad is not None else "",
                    'Unidades_Separado': unidades_sep,
                    'Precio_comparable': pcomp if pcomp is not None else "",
                    'Grupo':        grp_det or grp.capitalize(),
                    'Subgrupo':     sub,
                    'ClasificaProducto': clasif
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

def _get_existing_df(ws) -> pd.DataFrame:
    df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False).dropna(how='all')
    return df if not df.empty else pd.DataFrame(columns=ws.row_values(1))

def _align_df_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = "" if c not in ("Precio","Cantidad","Precio_comparable") else pd.NA
    return out[columns]

def _append_rows(ws, df: pd.DataFrame):
    if "FechaConsulta" in df.columns:
        df["FechaConsulta"] = pd.to_datetime(df["FechaConsulta"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    values = df.where(pd.notnull(df), "").values.tolist()
    chunk = 5000
    for i in range(0, len(values), chunk):
        ws.append_rows(values[i:i+chunk], value_input_option="USER_ENTERED")

def _shrink_grid(ws, nrows: int, ncols: Optional[int] = None):
    try:
        ws.resize(rows=max(1, nrows), cols=max(1, ncols or ws.col_count))
    except Exception:
        pass

def _ensure_required_columns_safe(ws, used_rows: int) -> Tuple[gspread.Worksheet, List[str]]:
    """
    Compacta filas ANTES de añadir columnas y luego garantiza el encabezado.
    Devuelve (worksheet posiblemente actualizado, header final).
    """
    # 1) Compactar filas a lo usado para liberar celdas
    try:
        if ws.row_count > max(1, used_rows):
            ws.resize(rows=max(1, used_rows), cols=ws.col_count)
    except Exception:
        pass

    # 2) Leer encabezado actual
    header = ws.row_values(1)
    header = [h for h in header if h]

    # 3) Si no hay encabezado, escribir el requerido (tras haber compactado filas)
    if not header:
        need_cols = max(ws.col_count, len(REQUIRED_COLUMNS))
        if ws.col_count < need_cols:
            try:
                ws.add_cols(need_cols - ws.col_count)
            except Exception:
                pass
        ws.update('A1', [REQUIRED_COLUMNS])
        return ws, REQUIRED_COLUMNS.copy()

    # 4) Agregar faltantes al final
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if not missing:
        return ws, header

    new_header = header + missing
    # Asegurar columnas necesarias después de haber reducido filas
    if ws.col_count < len(new_header):
        try:
            ws.add_cols(len(new_header) - ws.col_count)
        except gspread.exceptions.APIError:
            # Intento de última oportunidad: reducir aún más filas (mínimo encabezado)
            try:
                ws.resize(rows=max(1, used_rows), cols=ws.col_count)
                if ws.col_count < len(new_header):
                    ws.add_cols(len(new_header) - ws.col_count)
            except Exception:
                pass
    # Actualizar encabezado
    ws.update('A1', [new_header])
    return ws, new_header

def main() -> None:
    # 1) Ejecutar scrapers y recolectar
    all_rows: List[Dict] = []
    for _, cls in SCRAPERS.items():
        try:
            rows = cls().scrape()
            if rows:
                all_rows.extend(rows)
        except Exception:
            pass
    if not all_rows:
        return

    # 2) Consolidado y tipos
    df_all = pd.DataFrame(all_rows)
    for col in ("Precio","Cantidad","Precio_comparable"):
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
    df_all = df_all[df_all["Precio"] > 0]
    df_all["FechaConsulta"] = pd.to_datetime(df_all["FechaConsulta"], errors="coerce")
    df_all = df_all.drop_duplicates(subset=KEY_COLS, keep="last")

    # 3) Partición mensual
    if df_all["FechaConsulta"].notna().any():
        part = df_all["FechaConsulta"].dt.strftime("%Y%m").iloc[0]
    else:
        part = datetime.now(timezone.utc).strftime("%Y%m")
    target_title = f"{WORKSHEET_NAME}_{part}"

    # 4) Abrir workbook y hoja mensual
    sh = _authorize_sheet()
    ws = _ensure_worksheet(sh, target_title)

    # 5) LEER primero lo existente para conocer used_rows y luego compactar+encabezado
    prev_df = _get_existing_df(ws)
    used_rows = 1 + len(prev_df)  # encabezado + datos existentes (según valores reales)
    ws, header = _ensure_required_columns_safe(ws, used_rows=used_rows)

    # 6) Alinear dataframes a header final
    if prev_df.empty:
        prev_df = pd.DataFrame(columns=header)
    prev_df = _align_df_columns(prev_df, header)
    df_all  = _align_df_columns(df_all, header)

    # 7) Detección de nuevas filas por clave
    for c in KEY_COLS:
        if c not in prev_df.columns:
            prev_df[c] = ""
    prev_keys = set(prev_df[KEY_COLS].astype(str).agg('|'.join, axis=1)) if not prev_df.empty else set()
    all_keys  = df_all[KEY_COLS].astype(str).agg('|'.join, axis=1)
    new_rows  = df_all.loc[~all_keys.isin(prev_keys)].copy()
    if new_rows.empty:
        _shrink_grid(ws, nrows=len(prev_df)+1, ncols=len(header))
        return

    # 8) Append y compactación final
    cols_to_write = [c for c in header if c in new_rows.columns]
    new_rows = new_rows[cols_to_write]
    _append_rows(ws, new_rows)

    total_rows = 1 + len(prev_df) + len(new_rows)
    _shrink_grid(ws, nrows=total_rows, ncols=len(header))

if __name__ == "__main__":
    main()
