# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – Py 3.7+
Autor  : Diego B. Meza · Rev: 2025-06-29
Adaptado para ejecutarse en GitHub Actions
"""

import os
import sys
import glob
import re
import unicodedata
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Callable, Set
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────── Directorios y configuración ────────────────────
BASE_DIR = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))

# Montar credenciales en GitHub Actions
if os.environ.get("GITHUB_ACTIONS") == "true":
    creds_json = os.environ.get("GCP_CREDENTIALS_JSON")
    if creds_json:
        with open("creds.json", "w", encoding="utf-8") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("creds.json")

CREDS_JSON = os.environ.get(
    "CREDS_JSON_PATH",
    os.path.join(BASE_DIR, "creds.json")
)
SPREADSHEET_URL = os.environ.get(
    "SPREADSHEET_URL",
    "https://docs.google.com/spreadsheets/d/10zIOm2Ks2vVtg6JH_A9_IHdyAGzcAsN32azbfaxbVnk"
)
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "precios_supermercados")

FILE_TAG      = "frutihort"
PATTERN_DAILY = os.path.join(OUT_DIR, f"*canasta_{FILE_TAG}_*.csv")
MAX_WORKERS, REQ_TIMEOUT = 8, 10
KEY_COLS = ["Supermercado", "CategoríaURL", "Producto", "FechaConsulta"]

# ────────────────── 1. Normalización texto ────────────────────────────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")

_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ────────────────── 2. Clasificación en dos niveles ──────────────────────
BROAD_GROUP_KEYWORDS: Dict[str, List[str]] = {
    "Panificados": ["pan", "barr", "baguette", "tostada", "torta", "bizcochuelo", "madalena", "galleta", "masa"],
    "Frutas":      ["naranja", "manzana", "banana", "pera", "uva", "kiwi", "limon", "frutilla", "melon", "sandia"],
    "Verduras":    ["tomate", "cebolla", "papa", "zanahoria", "lechuga", "espinaca", "morron", "berenjena", "pepino"],
    "Cereales":    ["cereal", "granola", "avena", "trigo", "maiz", "copos", "muesli", "barrita", "bar"],
    "Huevos":      ["huevo", "huevos", "codorniz"],
    "Leches":      ["leche", "yogur", "yogurt", "bebible", "condensada", "polvo", "natalina"],
    "Quesos":      ["queso", "quesos", "rallado", "parmesano", "muzzarella", "sandwich", "paraguay"],
}
BROAD_GROUP_TOKENS: Dict[str, Set[str]] = {g: {strip_accents(w) for w in ws} for g, ws in BROAD_GROUP_KEYWORDS.items()}

SUBGROUP_KEYWORDS: Dict[str, List[str]] = {
    "Naranja":       ["naranja", "naranjas"],
    "Cebolla":       ["cebolla", "cebollas"],
    "Leche Bebible": ["leche", "bebible"],
    "Queso Sandwich":["queso", "sandwich"],
    "Queso Paraguay":["paraguay"],
    "Uva":           ["uva", "uvas"],
    "Huevo Gallina": ["huevo", "gallina"],
    "Huevo Codorniz":["codorniz"],
}
SUBGROUP_TOKENS: Dict[str, Set[str]] = {sg: {strip_accents(w) for w in ws} for sg, ws in SUBGROUP_KEYWORDS.items()}

def classify(name: str) -> tuple[str | None, str | None]:
    toks = set(tokenize(name))
    grp = next((g for g, ks in BROAD_GROUP_TOKENS.items() if toks & ks), None)
    sub = next((sg for sg, ks in SUBGROUP_TOKENS.items() if toks & ks), None)
    return grp, sub

# ────────────────── 3. Extracción de unidad ─────────────────────────────
_unit_re = re.compile(
    r"(?P<valor>\d+(?:[.,]\d+)?)\s*(?P<unidad>kg|g|gr|ml|l|lt|unid(?:ad)?s?|u|paq|stk)\b",
    re.IGNORECASE
)

def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    return f"{m.group('valor').replace(',', '.')}{m.group('unidad').upper().replace('LT','L')}" if m else ""

# ────────────────── 4. Normalización de precio ───────────────────────────
def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val))
    txt = txt.replace(".", "").replace(",", ".")
    try: return float(txt)
    except: return 0.0

def _first_price(node: BeautifulSoup, sels: List[str] | None = None) -> float:
    sels = sels or ["span.price ins span.amount","span.price > span.amount","span.woocommerce-Price-amount","span.amount","bdi","[data-price]"]
    for s in sels:
        el = node.select_one(s)
        if el:
            p = norm_price(el.get_text() or el.get("data-price",""))
            if p>0: return p
    return 0.0

# ────────────────── 5. Sesión HTTP robusta ───────────────────────────────
def _build_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.2,status_forcelist=(429,500,502,503,504),allowed_methods=("GET","HEAD"))
    ad = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("http://",ad); s.mount("https://",ad)
    return s

# ────────────────── 6. Clase base de scraper ─────────────────────────────
class HtmlSiteScraper:
    def __init__(self,name:str,base:str):
        self.name=name;self.base_url=base.rstrip("/");self.session=_build_session()
    def category_urls(self)->List[str]: raise NotImplementedError
    def parse_category(self,url:str)->List[Dict]: raise NotImplementedError
    def scrape(self)->List[Dict]:
        urls=self.category_urls();
        if not urls: return []
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out:List[Dict]=[]
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futs={pool.submit(self.parse_category,u):u for u in urls}
            for f in as_completed(futs):
                for row in f.result(): row["FechaConsulta"]=fecha; out.append(row)
        return out
    def save_csv(self,rows:List[Dict])->None:
        if not rows: return
        os.makedirs(OUT_DIR,exist_ok=True)
        fn=f"{self.name}_canasta_{FILE_TAG}_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR,fn),index=False)

# ────────────────── 7. Scrapers por sitio ─────────────────────────────────
class StockScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("stock","https://www.stock.com.py")
    def category_urls(self)->List[str]:
        try: r=self.session.get(self.base_url,timeout=REQ_TIMEOUT);r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.text,"html.parser");urls=set()
        for a in soup.select('a[href*="/category/"]'):
            href=a["href"].lower()
            if any(tok in href for grp in BROAD_GROUP_KEYWORDS.values() for tok in grp): urls.add(urljoin(self.base_url,href))
        return list(urls)
    def parse_category(self,url:str)->List[Dict]:
        try: r=self.session.get(url,timeout=REQ_TIMEOUT);r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.content,"html.parser");rows=[]
        for p in soup.select("div.product-item"):
            nm=p.select_one("h2.product-title")
            if not nm: continue
            name=nm.get_text(" ",strip=True);grp,sub=classify(name)
            if not grp: continue
            precio=_first_price(p,["span.price-label","span.price"])
            unidad=extract_unit(name)
            rows.append({"Supermercado":"Stock","CategoríaURL":url,"Producto":name.upper(),"Precio":precio,"Unidad":unidad,"Grupo":grp,"Subgrupo":sub or ""})
        return rows

# Superseis
class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("superseis","https://www.superseis.com.py")
    def category_urls(self)->List[str]:
        try: r=self.session.get(self.base_url,timeout=REQ_TIMEOUT); r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.text,"html.parser"); urls=set()
        for a in soup.select('a.collapsed[href*="/category/"]'):
            href=a["href"].lower()
            if any(tok in href for grp in BROAD_GROUP_KEYWORDS.values() for tok in grp): urls.add(urljoin(self.base_url,href))
        return list(urls)
    def parse_category(self,url:str)->List[Dict]:
        try: r=self.session.get(url,timeout=REQ_TIMEOUT); r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.content,"html.parser"); rows=[]
        for a in soup.select("a.product-title-link"):
            name=a.get_text(" ",strip=True); grp,sub=classify(name)
            if not grp: continue
            parent=a.find_parent("div.product-item") or a
            precio=_first_price(parent,["span.price-label","span.price"]); unidad=extract_unit(name)
            rows.append({"Supermercado":"Superseis","CategoríaURL":url,"Producto":name.upper(),"Precio":precio,"Unidad":unidad,"Grupo":grp,"Subgrupo":sub or ""})
        return rows

# Salemma
class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("salemma","https://www.salemmaonline.com.py")
    def category_urls(self)->List[str]:
        try: r=self.session.get(self.base_url,timeout=REQ_TIMEOUT); r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.text,"html.parser"); urls=set()
        for a in soup.find_all("a",href=True):
            href=a["href"].lower()
            if any(tok in href for grp in BROAD_GROUP_KEYWORDS.values() for tok in grp): urls.add(urljoin(self.base_url,href))
        return list(urls)
    def parse_category(self,url:str)->List[Dict]:
        try: r=self.session.get(url,timeout=REQ_TIMEOUT); r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.content,"html.parser"); rows=[]
        for f in soup.select("form.productsListForm"):
            name=f.find("input",{"name":"name"}).get("value",""); grp,sub=classify(name)
            if not grp: continue
            precio=norm_price(f.find("input",{"name":"price"}).get("value","")); unidad=extract_unit(name)
            rows.append({"Supermercado":"Salemma","CategoríaURL":url,"Producto":name.upper(),"Precio":precio,"Unidad":unidad,"Grupo":grp,"Subgrupo":sub or ""})
        return rows

# Arete
class AreteScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("arete","https://www.arete.com.py")
    def category_urls(self)->List[str]:
        try: r=self.session.get(self.base_url,timeout=REQ_TIMEOUT); r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.text,"html.parser"); urls=set()
        for sel in ("#departments-menu","#menu-departments-menu-1"):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                href=a["href"].split("?")[0].lower()
                if any(tok in href for grp in BROAD_GROUP_KEYWORDS.values() for tok in grp): urls.add(urljoin(self.base_url+"/",href))
        return list(urls)
    def parse_category(self,url:str)->List[Dict]:
        try: r=self.session.get(url,timeout=REQ_TIMEOUT); r.raise_for_status()
        except: return []
        soup=BeautifulSoup(r.content,"html.parser"); rows=[]
        for p in soup.select("div.product"):
            nm=p.select_one("h2.ecommercepro-loop-product__title")
            if not nm: continue
            name=nm.get_text(" ",strip=True); grp,sub=classify(name)
            if not grp: continue
            precio=_first_price(p); unidad=extract_unit(name)
            rows.append({"Supermercado":"Arete","CategoríaURL":url,"Producto":name.upper(),"Precio":precio,"Unidad":unidad,"Grupo":grp,"Subgrupo":sub or ""})
        return rows

# Jardines (hereda de Arete)
class JardinesScraper(AreteScraper):
    def __init__(self): super().__init__(); self.name="losjardines"; self.base_url="https://losjardinesonline.com.py"

# Biggie (API)
class BiggieScraper:
    name="biggie"; API="https://api.app.biggie.com.py/api/articles"; TAKE=100
    GROUPS=["huevos","lacteos","frutas","verduras","cereales","panificados"]
    session=_build_session()
    def fetch_group(self,grp:str)->List[Dict]:
        rows=[]; skip=0
        while True:
            resp=self.session.get(self.API,params={"take":self.TAKE,"skip":skip,"classificationName":grp},timeout=REQ_TIMEOUT)
            js=resp.json()
            for it in js.get("items",[]):
                name=it.get("name",""); price=norm_price(it.get("price",0)); unidad=extract_unit(name)
                broad,sub=classify(name)
                rows.append({"Supermercado":"Biggie","CategoríaURL":grp,"Producto":name.upper(),"Precio":price,"Unidad":unidad,"Grupo":broad or grp.capitalize(),"Subgrupo":sub or ""})
            skip+=self.TAKE
            if skip>=js.get("count",0): break
        return rows
    def scrape(self)->List[Dict]:
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S"); out=[]
        for g in self.GROUPS:
            for item in self.fetch_group(g): item["FechaConsulta"]=fecha; out.append(item)
        return out
    def save_csv(self,rows:List[Dict])->None:
        if not rows: return
        os.makedirs(OUT_DIR,exist_ok=True)
        fn=f"{self.name}_canasta_{FILE_TAG}_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR,fn),index=False)

# Mapeo final de scrapers
SCRAPERS: Dict[str, Callable[[], object]] = {
    "stock": StockScraper,
    "superseis": SuperseisScraper,
    "salemma": SalemmaScraper,
    "arete": AreteScraper,
    "losjardines": JardinesScraper,
    "biggie": BiggieScraper,
}

# ────────────────── 8. Orquestador y Google Sheets ──────────────────────
def _open_sheet():
    import gspread
    from gspread_dataframe import get_as_dataframe,set_with_dataframe
    from google.oauth2.service_account import Credentials
    scopes=["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/spreadsheets"]
    creds=Credentials.from_service_account_file(CREDS_JSON,scopes=scopes)
    sh=gspread.authorize(creds).open_by_url(SPREADSHEET_URL)
    try: ws=sh.worksheet(WORKSHEET_NAME)
    except: ws=sh.add_worksheet(title=WORKSHEET_NAME,rows="10000",cols="40")
    df=get_as_dataframe(ws,dtype=str,evaluate_formulas=False).dropna(how="all")
    return ws,df

def _write_sheet(ws,df:pd.DataFrame)->None:
    from gspread_dataframe import set_with_dataframe
    ws.clear(); set_with_dataframe(ws,df,include_index=False)

def main(argv: List[str] = None) -> int:
    try:
        args = _parse_args(argv if argv is not None else sys.argv[1:])
        all_records: List[Dict] = []
        for key in args:
            scraper = SCRAPERS[key]()
            rows = scraper.scrape()
            scraper.save_csv(rows)
            all_records.extend(rows)
        if not all_records:
            print("Sin datos nuevos.")
            return 0

        files = glob.glob(PATTERN_DAILY)
        if not files:
            print("⚠️ No se encontraron CSV para concatenar.")
            return 0

        df_combined = pd.concat(
            [pd.read_csv(f, dtype=str) for f in files],
            ignore_index=True,
            sort=False
        )
        df_combined["Precio"] = pd.to_numeric(df_combined["Precio"], errors="coerce")
        df_combined["FechaConsulta"] = pd.to_datetime(
            df_combined["FechaConsulta"], errors="coerce"
        )

        ws, prev_df = _open_sheet()
        merged = pd.concat([prev_df, df_combined], ignore_index=True, sort=False)
        merged.sort_values("FechaConsulta", inplace=True)
        merged["FechaConsulta"] = merged["FechaConsulta"].dt.strftime("%Y-%m-%d")
        merged.drop_duplicates(KEY_COLS, keep="first", inplace=True)

        if "ID" in merged.columns:
            merged.drop(columns=["ID"], inplace=True)
        merged.insert(0, "ID", range(1, len(merged) + 1))

        _write_sheet(ws, merged)
        print(f"✅ Hoja actualizada: {len(merged)} filas totales")
        return 0
    
    except Exception as e:
        print(f"⚠️ Error inesperado en scraper: {e}", file=sys.stderr)
        return 1
        
if __name__ == "__main__":
            sys.exit(main())
