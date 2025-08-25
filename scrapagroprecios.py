# -*- coding: utf-8 -*-
import os
import re
import unicodedata
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import urljoin
import time
import logging
from typing import Tuple, Optional
import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
SHARE_WITH_EMAIL = os.environ.get("SHARE_WITH_EMAIL")

def _drive_service(cred):
    return build('drive', 'v3', credentials=cred, cache_discovery=False)

def _get_or_create_monthly_spreadsheet(part: str):
    """
    Crea o reutiliza un LIBRO mensual llamado '{WORKSHEET_NAME}_{part}' en Drive.
    Devuelve (sh, ws, file_id). ws es la hoja interna donde escribimos.
    """
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ]
    cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    gc = gspread.authorize(cred)
    drive = _drive_service(cred)

    book_title = f"{WORKSHEET_NAME}_{part}"

    # 1) Buscar si ya existe
    try:
        q = f"name = '{book_title}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
        res = drive.files().list(q=q, spaces='drive', fields='files(id,name)', pageSize=1).execute()
        files = res.get('files', [])
        if files:
            file_id = files[0]['id']
        else:
            # 2) Crear si no existe
            metadata = {
                'name': book_title,
                'mimeType': 'application/vnd.google-apps.spreadsheet'
            }
            if DRIVE_FOLDER_ID:
                metadata['parents'] = [DRIVE_FOLDER_ID]
            created = drive.files().create(body=metadata, fields='id').execute()
            file_id = created['id']

            # (opcional) Compartir contigo para que veas el libro en tu Drive
            if SHARE_WITH_EMAIL:
                drive.permissions().create(
                    fileId=file_id,
                    body={'type': 'user', 'role': 'writer', 'emailAddress': SHARE_WITH_EMAIL},
                    sendNotificationEmail=False
                ).execute()
    except HttpError as e:
        # Fallback mínimo: si falla Drive API, intenta crear con gspread (quedará en el Drive del SA)
        sh_tmp = gc.create(book_title)
        file_id = sh_tmp.id

    # 3) Abrir el libro por ID
    sh = gc.open_by_key(file_id)

    # 4) Asegurar una hoja interna (usa el mismo nombre del mes como hoja)
    sheet_title = f"{WORKSHEET_NAME}_{part}"
    try:
        ws = sh.worksheet(sheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_title, rows='2', cols=str(len(REQUIRED_COLUMNS)))
        ws.update('A1', [REQUIRED_COLUMNS])

    return sh, ws, file_id


# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─────────────────── 0. Configuración ────────────────────
BASE_DIR = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL")
CREDS_JSON = os.environ.get("CREDS_JSON_PATH", os.path.abspath("creds.json"))
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "precios_supermercados")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
REQ_TIMEOUT = int(os.environ.get("REQ_TIMEOUT", "20"))

# Esquema base requerido (añadimos campos nuevos solicitados)
REQUIRED_COLUMNS = [
    'Supermercado', 'Producto', 'Precio',
    'Unidad', 'Unidad_corr', 'Cantidad', 
    'Unidades_Separado', 'Precio_comparable',
    'Grupo', 'Subgrupo', 'ClasificaProducto',
    'FechaConsulta'
]
KEY_COLS = ['Supermercado', 'Producto', 'FechaConsulta']

# ────────────────── 1. Utilidades de texto ─────────────────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

def normalize_text(txt: str) -> str:
    return strip_accents(str(txt)).lower()

def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in re.findall(r"[a-záéíóúñü]+", str(txt), flags=re.I)]

# ─────────────── 2. Clasificación Grupo/Subgrupo ─────────────────
TAXONOMY = {
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

# Precompilar expresiones regulares para mejor rendimiento
EXCLUSIONES_GENERALES_RE = re.compile(
    r"\b(extracto|jugo|sabor|pulpa|pure|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|mermelada|chips|snack|polvo|humo)\b"
)
TOMATE_EXC_RE = re.compile(
    r"(arroz\s+con\s+tomate|en\s+tomate|salsa(\s+de)?\s+tomate|ketchup|tomate\s+en\s+polvo|tomate\s+en\s+lata|extracto|jugo|pulpa|pure|congelad[oa]|deshidratad[oa])"
)
CEBOLLA_EXC_RE = re.compile(r"(en\s+polvo|salsa|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pate|crema|sopa)")
PAPA_EXC_RE = re.compile(r"(chips|frita|fritas|chuño|pure|congelad[oa]|deshidratad[oa]|harina|sopa|snack)")
ZANAHORIA_EXC_RE = re.compile(r"(jugo|pure|conserva|congelad[oa]|deshidratad[oa]|salsa|tarta|pastel|mermelada)")
REMOLACHA_EXC_RE = re.compile(r"(en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|jugo|pulpa|mermelada)")
BANANA_EXC_RE = re.compile(r"(harina|polvo|chips|frita|dulce|mermelada|batido|jugo|snack|pure|congelad[oa]|deshidratad[oa])")
CITRICOS_EXC_RE = re.compile(r"(jugo|mermelada|concentrad[oa]|esencia|sabor|pulpa|congelad[oa]|deshidratad[oa]|dulce|jarabe)")

def clasifica_producto(descripcion: str) -> str:
    d = normalize_text(descripcion)
    if not d:
        return ""
    
    if "tomate" in d and not TOMATE_EXC_RE.search(d):
        return "Tomate fresco"
    
    tiene_morron = any(k in d for k in ("morron", "locote", "pimiento", "pimenton"))
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
    
    if any(k in d for k in ("banana", "banano", "platano", "guineo")) and not BANANA_EXC_RE.search(d):
        return "Banana fresca"
    
    if any(k in d for k in ("naranja", "mandarina", "pomelo", "limon", "apepu")) and not CITRICOS_EXC_RE.search(d):
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

# ─────────────── 2.5. Exclusiones genéricas en nombre ─────────────────
EXCLUDE_PATTERNS = [r"\bcombo\b", r"\bpack\b\s*\b(oferta|ahorro|promo)\b", r"\bdisney\b"]
_ex_re = re.compile("|".join(EXCLUDE_PATTERNS), re.I)

def is_excluded(name: str) -> bool:
    return bool(_ex_re.search(name))

# ─────────────── 3. Parsing avanzado de unidades ─────────────────────────────
RE_MULTIPACK = re.compile(
    r"(?:(?P<count>\d+)\s*[x×]\s*)?(?P<size>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilo|g|gr|l|lt|litro|ml|cc)\b",
    re.I
)
RE_FRACTION = re.compile(
    r"(?P<num>\d+)\s*/\s*(?P<den>\d+)\s*(?P<unit>kg|kilo|l|lt|litro)\b",
    re.I
)
RE_UNITS = re.compile(
    r"(?:\b(?:x|de)\s*)?(?P<count>\d+)\s*(?P<unit>uni(?:d)?|u|paq|paquete)s?\b",
    re.I
)
RE_DOCENA = re.compile(r"\b(1\/2\s+docena|media\s+docena|docena)\b", re.I)

def _to_float(num_str: str) -> float:
    s = str(num_str).replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return 0.0

def _norm_unit_symbol(u: str) -> str:
    u = u.lower()
    if u in ("kg", "kilo"):
        return "GR"
    if u in ("g", "gr"):
        return "GR"
    if u in ("l", "lt", "litro"):
        return "ML"
    if u in ("ml", "cc"):
        return "ML"
    if u in ("uni", "unid", "u"):
        return "UNID"
    if u in ("paq", "paquete"):
        return "PAQ"
    return u.upper()

def parse_unidad_corr(nombre: str) -> Tuple[str, Optional[float], str]:
    d = normalize_text(nombre)

    # 1) Multipack o tamaño directo
    m = RE_MULTIPACK.search(d)
    if m:
        count = int(m.group('count')) if m.group('count') else 1
        size = _to_float(m.group('size'))
        unit_sym = _norm_unit_symbol(m.group('unit'))
        if unit_sym == "GR":
            total = count * (size * (1000.0 if re.search(r"\bkg|kilo\b", m.group('unit'), re.I) else 1.0))
            total_str = str(int(round(total)))
            return f"{total_str}GR", float(total), "GR"
        if unit_sym == "ML":
            total = count * (size * (1000.0 if re.search(r"\bl|lt|litro\b", m.group('unit'), re.I) else 1.0))
            total_str = str(int(round(total)))
            return f"{total_str}ML", float(total), "ML"

    # 2) Fracciones de kg/l (1/2 kg, 1/4 kg, etc.)
    mf = RE_FRACTION.search(d)
    if mf:
        num = _to_float(mf.group('num'))
        den = _to_float(mf.group('den'))
        unit_sym = _norm_unit_symbol(mf.group('unit'))
        frac = (num / den) if den else 0.0
        if unit_sym in ("GR", "ML"):
            base = 1000.0
            total = frac * base
            total_str = str(int(round(total)))
            return f"{total_str}{unit_sym}", float(total), unit_sym

    # 3) Docenas y unidades
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
        return f"{count}{unit_sym}", float(count), "UNID" if unit_sym in ("UNID",) else unit_sym

    # 4) Tamaño simple sin 'x'
    simple = re.search(r"(?P<size>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilo|g|gr|l|lt|litro|ml|cc)\b", d, re.I)
    if simple:
        size = _to_float(simple.group('size'))
        unit_sym = _norm_unit_symbol(simple.group('unit'))
        if unit_sym == "GR":
            total = size * (1000.0 if re.search(r"\bkg|kilo\b", simple.group('unit'), re.I) else 1.0)
            total_str = str(int(round(total)))
            return f"{total_str}GR", float(total), "GR"
        if unit_sym == "ML":
            total = size * (1000.0 if re.search(r"\bl|lt|litro\b", simple.group('unit'), re.I) else 1.0)
            total_str = str(int(round(total)))
            return f"{total_str}ML", float(total), "ML"

    return "", None, ""

def extract_unit_basic(name: str) -> str:
    u, _, _sep = parse_unidad_corr(name)
    return u

def precio_comparable(precio: float, cantidad: Optional[float], unidad_sep: str) -> Optional[float]:
    try:
        p = float(precio)
    except Exception:
        return None
        
    if not cantidad or cantidad <= 0:
        return None
        
    u = (unidad_sep or "").upper()
    if u in ("GR", "ML"):
        return p * (1000.0 / cantidad)
    if u in ("UNID", "PAQ"):
        return p / cantidad
        
    return None

# ─────────────── 4. Precio web ───────────────────────────
_price_selectors = [
    "[data-price]", "[data-price-final]", "[data-price-amount]",
    "meta[itemprop='price']", "span.price ins span.amount", "span.price > span.amount",
    "span.woocommerce-Price-amount", "span.amount", "bdi", "div.price", "p.price"
]

def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace('.', '').replace(',', '.')
    try:
        return float(txt)
    except Exception:
        return 0.0

def _first_price(node: Tag) -> float:
    for attr in ("data-price", "data-price-final", "data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr]) > 0:
            return norm_price(node[attr])
            
    meta = node.select_one("meta[itemprop='price']")
    if meta and norm_price(meta.get('content', '')) > 0:
        return norm_price(meta.get('content', ''))
        
    for sel in _price_selectors:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get(sel, ''))
            if p > 0:
                return p
                
    return 0.0

# ─────────────── 5. HTTP session ───────────────────────────
def _build_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD")
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    })
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
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
            'Producto': nombre.upper(),
            'Precio': precio,
            'Unidad': unidad_simple,
            'Unidad_corr': unidad_corr,
            'Cantidad': cantidad if cantidad is not None else "",
            'Unidades_Separado': unidades_sep,
            'Precio_comparable': pcomp if pcomp is not None else "",
            'Grupo': grp,
            'Subgrupo': sub,
            'ClasificaProducto': clasif
        }

    def scrape(self) -> List[Dict]:
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        rows: List[Dict] = []
        urls = self.category_urls()
        
        if not urls:
            return rows
            
        with ThreadPoolExecutor(min(MAX_WORKERS, len(urls))) as pool:
            futures = {pool.submit(self.parse_category, u): u for u in urls}
            for fut in as_completed(futures):
                try:
                    result = fut.result()
                    for r in result:
                        if r and float(r.get("Precio", 0)) > 0:
                            r["FechaConsulta"] = ts
                            rows.append(r)
                except Exception as e:
                    logger.error(f"Error procesando categoría {futures[fut]}: {e}")
                    
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
        except Exception as e:
            logger.error(f"Error obteniendo categorías de Stock: {e}")
            return []
            
        kws = [t for lst in TAXONOMY["Verduras"].values() for t in lst] + \
              [t for lst in TAXONOMY["Frutas"].values() for t in lst] + \
              ["leche", "yogur", "queso", "huevo", "harina", "arroz", "aceite", "azucar", "agua", "gaseosa", "jugo"]
              
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
        except Exception as e:
            logger.error(f"Error accediendo a categoría {url}: {e}")
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
        except Exception as e:
            logger.error(f"Error obteniendo categorías de Superseis: {e}")
            return []
            
        soup = BeautifulSoup(r.text, "html.parser")
        return list({
            urljoin(self.base_url, a["href"])
            for a in soup.find_all("a", href=True, class_="collapsed")
            if "/category/" in a.get("href", "")
        })

    def parse_category(self, url: str) -> List[dict]:
        regs: List[dict] = []
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            logger.error(f"Error accediendo a categoría {url}: {e}")
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
        except Exception as e:
            logger.error(f"Error obteniendo categorías de Salemma: {e}")
            return []
            
        for a in BeautifulSoup(resp.content, 'html.parser').find_all('a', href=True):
            h = a['href'].lower()
            if any(tok in h for tok in ("fruta", "verdura", "lacte", "queso", "yogur", "leche", "huevo", "arroz", "harina", "aceite", "azucar", "bebida", "gaseosa", "jugo", "agua")):
                urls.add(urljoin(self.base_url, h))
                
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        out: List[Dict] = []
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error accediendo a categoría {url}: {e}")
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
        except Exception as e:
            logger.error(f"Error obteniendo categorías de Arete: {e}")
            return []
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        for sel in ('#departments-menu', '#menu-departments-menu-1'):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                h = a['href'].split('?')[0].lower()
                if any(tok in h for tok in ("fruta", "verdura", "lacte", "queso", "yogur", "leche", "huevo", "arroz", "harina", "aceite", "azucar", "bebida", "jugo", "agua", "gaseosa")):
                    urls.add(urljoin(self.base_url + '/', h))
                    
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        out: List[Dict] = []
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error accediendo a categoría {url}: {e}")
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
    API = 'https://api.app.biggie.com.py/api/articles'
    TAKE = 100
    GROUPS = ['huevos', 'lacteos', 'frutas', 'verduras', 'cereales', 'panificados']

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
            except Exception as e:
                logger.error(f"Error accediendo a API de Biggie: {e}")
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
                    'Producto': nombre.upper(),
                    'Precio': precio,
                    'Unidad': unidad_simple,
                    'Unidad_corr': unidad_corr,
                    'Cantidad': cantidad if cantidad is not None else "",
                    'Unidades_Separado': unidades_sep,
                    'Precio_comparable': pcomp if pcomp is not None else "",
                    'Grupo': grp_det or grp.capitalize(),
                    'Subgrupo': sub,
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
            try:
                for r in self.parse_category(grp):
                    r['FechaConsulta'] = ts
                    rows.append(r)
            except Exception as e:
                logger.error(f"Error procesando categoría {grp} en Biggie: {e}")
                
        return rows

# Registro de scrapers
SCRAPERS = {
    'stock': StockScraper,
    'superseis': SuperseisScraper,
    'salemma': SalemmaScraper,
    'arete': AreteScraper,
    'losjardines': JardinesScraper,
    'biggie': BiggieScraper,
}

# ─────────────── 13. Google Sheets (solución al error de límite de celdas) ─────────────────
def _authorize_sheet():
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ]
    try:
        cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
        gc = gspread.authorize(cred)
        sh = gc.open_by_url(SPREADSHEET_URL)
        return sh
    except Exception as e:
        logger.error(f"Error autorizando Google Sheets: {e}")
        raise

def _ensure_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        try:
            # Intentar crear nueva hoja
            return sh.add_worksheet(title=title, rows=100, cols=len(REQUIRED_COLUMNS))
        except Exception as e:
            logger.error(f"Error creando hoja {title}: {e}")
            # Si falla, usar la primera hoja
            return sh.sheet1

def _ensure_required_columns(ws) -> List[str]:
    try:
        header = ws.row_values(1)
        header = [h for h in header if h]
        
        if not header:
            # Hoja vacía, establecer encabezados
            ws.update([REQUIRED_COLUMNS], 'A1')
            return REQUIRED_COLUMNS
            
        # Verificar columnas faltantes
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        
        if missing:
            # Añadir columnas faltantes si hay espacio
            try:
                current_cols = ws.col_count
                needed_cols = len(header) + len(missing)
                
                if needed_cols > current_cols:
                    ws.add_cols(needed_cols - current_cols)
                
                # Actualizar encabezados
                new_header = header + missing
                ws.update([new_header], 'A1')
                return new_header
            except Exception as e:
                logger.warning(f"No se pudieron añadir columnas faltantes: {e}. Usando columnas existentes.")
                
        return header
    except Exception as e:
        logger.error(f"Error asegurando columnas requeridas: {e}")
        return REQUIRED_COLUMNS

def _align_df_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = "" if c not in ("Precio", "Cantidad", "Precio_comparable") else pd.NA
    return out[columns]

def _get_existing_df(ws) -> pd.DataFrame:
    try:
        df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False).dropna(how='all')
        return df if not df.empty else pd.DataFrame(columns=ws.row_values(1))
    except Exception as e:
        logger.error(f"Error obteniendo datos existentes: {e}")
        return pd.DataFrame()







from typing import Tuple, Optional
import gspread

def _append_rows(ws, df: pd.DataFrame):
    if "FechaConsulta" in df.columns:
        df["FechaConsulta"] = pd.to_datetime(df["FechaConsulta"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    values = df.where(pd.notnull(df), "").values.tolist()
    chunk = 5000
    for i in range(0, len(values), chunk):
        ws.append_rows(values[i:i+chunk], value_input_option="USER_ENTERED")

def _shrink_grid(ws, nrows: int, ncols: Optional[int] = None):
    # Nunca ampliar; sólo reducir.
    try:
        ws.resize(rows=max(1, nrows), cols=max(1, (ncols or ws.col_count)))
    except Exception:
        pass

def _get_existing_df(ws) -> pd.DataFrame:
    df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False).dropna(how='all')
    return df if not df.empty else pd.DataFrame(columns=ws.row_values(1))

def _ensure_required_columns_safe(ws, used_rows: int) -> Tuple[gspread.Worksheet, List[str]]:
    """
    Estrategia 'no expand': reduce filas/columnas si sobran y
    jamás intenta aumentar columnas. Si faltan campos requeridos
    pero no hay ancho disponible, se truncan (capping) y se trabaja
    sólo con la intersección.
    Devuelve (worksheet, header_final_que_realmente_existe).
    """
    # 1) Compactar filas a lo efectivamente usado (libera celdas de la hoja)
    try:
        if ws.row_count > max(1, used_rows):
            ws.resize(rows=max(1, used_rows), cols=ws.col_count)
    except Exception:
        pass

    # 2) Leer encabezado actual
    try:
        header = ws.row_values(1)
    except Exception:
        header = []
    header = [h for h in header if h]  # no vacíos
    ncols_current = ws.col_count
    ncols_used = len(header)

    # 3) Compactar columnas si hay columnas vacías al final (reduce celdas del workbook)
    if ncols_used > 0 and ncols_current > ncols_used:
        try:
            ws.resize(rows=ws.row_count, cols=ncols_used)
            ncols_current = ws.col_count
        except Exception:
            pass

    # 4) Si no hay encabezado, escribir uno truncado al ancho disponible (sin expandir)
    if not header:
        base = REQUIRED_COLUMNS[:ncols_current] if ncols_current else []
        if base:
            try:
                # Soporta firma nueva y actual de gspread
                try:
                    ws.update(values=[base], range_name="A1")
                except TypeError:
                    ws.update("A1", [base])
        # si ni siquiera esto se puede, retornamos el base calculado (puede ser vacío)
            except gspread.exceptions.APIError:
                pass
        return ws, base

    # 5) Agregar faltantes 'virtualmente' sin expandir: cap al ancho actual
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if not missing:
        final_header = header
    else:
        proposed = header + missing
        if len(proposed) > ncols_current:
            # No hay ancho disponible: cap al ancho actual
            final_header = proposed[:ncols_current]
        else:
            final_header = proposed

    # Actualizar fila 1 sólo si cambia y sin expandir
    if final_header != header:
        try:
            try:
                ws.update(values=[final_header], range_name="A1")
            except TypeError:
                ws.update("A1", [final_header])
        except gspread.exceptions.APIError:
            # Si falla, nos quedamos con el header antiguo (no ampliamos)
            final_header = header

    return ws, final_header

def _align_df_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Alinea el DF al conjunto real de columnas de la hoja (no forzamos expansión).
    Numéricas se dejan como NaN si no existen.
    """
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = "" if c not in ("Precio","Cantidad","Precio_comparable") else pd.NA
    # sólo devolvemos columnas existentes en la hoja
    return out[[c for c in columns]]


def _create_new_spreadsheet(creds, title: str):
    """Crea un nuevo libro de Google Sheets cuando el actual está lleno"""
    try:
        drive_service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
        }
        file = drive_service.files().create(body=file_metadata, fields='id').execute()
        
        sheets_service = build('sheets', 'v4', credentials=creds)
        spreadsheet_id = file.get('id')
        
        # Crear la hoja con los encabezados requeridos
        body = {
            'values': [REQUIRED_COLUMNS]
        }
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    except Exception as e:
        logger.error(f"Error creando nuevo libro: {e}")
        raise


def main() -> None:
    logger.info("Iniciando scraping de precios...")

    # 1) Ejecutar scrapers y recolectar
    all_rows: List[Dict] = []
    for name, cls in SCRAPERS.items():
        try:
            logger.info(f"Ejecutando scraper: {name}")
            scraper = cls()
            rows = scraper.scrape()
            if rows:
                logger.info(f"{name}: {len(rows)} productos encontrados")
                all_rows.extend(rows)
            else:
                logger.warning(f"{name}: No se encontraron productos")
        except Exception as e:
            logger.error(f"Error ejecutando scraper {name}: {e}")

    if not all_rows:
        logger.error("No se obtuvieron datos de ningún scraper")
        return

    # 2) Consolidado y tipos
    df_all = pd.DataFrame(all_rows)
    for col in ("Precio", "Cantidad", "Precio_comparable"):
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
    df_all = df_all[df_all["Precio"] > 0]
    df_all["FechaConsulta"] = pd.to_datetime(df_all["FechaConsulta"], errors="coerce")
    df_all = df_all.drop_duplicates(subset=KEY_COLS, keep="last")

    # 3) Determinar partición mensual
    if df_all["FechaConsulta"].notna().any():
        part = df_all["FechaConsulta"].dt.strftime("%Y%m").iloc[0]
    else:
        part = datetime.now(timezone.utc).strftime("%Y%m")
    target_title = f"{WORKSHEET_NAME}_{part}"

        # 4) Abrir o crear LIBRO mensual (nuevo libro por mes, no una hoja en el libro madre)
    try:
        sh, ws, file_id = _get_or_create_monthly_spreadsheet(part)
        header = _ensure_required_columns(ws)

        # 5) Leer existente y alinear
        prev_df = _get_existing_df(ws)
        if prev_df.empty:
            prev_df = pd.DataFrame(columns=header)
        prev_df = _align_df_columns(prev_df, header)
        df_all = _align_df_columns(df_all, header)

        # 6) Detección de nuevas filas por clave
        for c in KEY_COLS:
            if c not in prev_df.columns:
                prev_df[c] = ""

        prev_keys = set(prev_df[KEY_COLS].astype(str).agg('|'.join, axis=1)) if not prev_df.empty else set()
        all_keys = df_all[KEY_COLS].astype(str).agg('|'.join, axis=1)
        mask_new = ~all_keys.isin(prev_keys)
        new_rows = df_all.loc[mask_new].copy()

        if new_rows.empty:
            logger.info("No hay nuevas filas para agregar")
            _shrink_grid(ws, nrows=len(prev_df) + 1, ncols=len(header))
            logger.info(f"Libro mensual: https://docs.google.com/spreadsheets/d/{file_id}")
            return

        # 7) Ordenar columnas según encabezado y anexar
        cols_to_write = [c for c in header if c in new_rows.columns]
        new_rows = new_rows[cols_to_write]

        logger.info(f"Añadiendo {len(new_rows)} nuevas filas al libro mensual {file_id}")
        _append_rows(ws, new_rows)

        # 8) Compactar grilla
        total_rows = 1 + len(prev_df) + len(new_rows)
        _shrink_grid(ws, nrows=total_rows, ncols=len(header))

        logger.info(f"Proceso completado. Libro mensual: https://docs.google.com/spreadsheets/d/{file_id}")

    except gspread.exceptions.APIError as e:
        logger.error(f"Error de Google Sheets: {e}")
    except HttpError as e:
        logger.error(f"Error de Google Drive API: {e}")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")

        # 9) Ordenar columnas según encabezado y anexar
        cols_to_write = [c for c in header if c in new_rows.columns]
        new_rows = new_rows[cols_to_write]
        logger.info(f"Añadiendo {len(new_rows)} nuevas filas")
        _append_rows(ws, new_rows)

        # 10) Compactar grilla a lo justo (sin ampliar columnas)
        total_rows = 1 + len(prev_df) + len(new_rows)
        _shrink_grid(ws, nrows=total_rows, ncols=len(header))

        logger.info("Proceso completado exitosamente")

    except gspread.exceptions.APIError as e:
        # Con la ruta segura no debería ocurrir; se deja por robustez
        if "limit of 10000000 cells" in str(e):
            logger.error("Límite de celdas alcanzado. Creando nuevo libro...")
            try:
                cred = Credentials.from_service_account_file(
                    CREDS_JSON,
                    scopes=[
                        'https://www.googleapis.com/auth/drive',
                        'https://www.googleapis.com/auth/spreadsheets',
                    ],
                )
                new_url = _create_new_spreadsheet(cred, f"{WORKSHEET_NAME}_{part}_backup")
                logger.info(f"Nuevo libro creado: {new_url}")
            except Exception as create_error:
                logger.error(f"Error creando nuevo libro: {create_error}")
        else:
            logger.error(f"Error de API de Google Sheets: {e}")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")


if __name__ == "__main__":
    main()
