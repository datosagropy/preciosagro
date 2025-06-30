# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – Py 3.7+ adaptado para GitHub Actions
Autor  : Diego B. Meza · Rev: 2025-06-29 corrección cuenta alterna

Este script:
1) Realiza scraping de supermercados Stock, Superseis, Salemma, Arete, Jardines y Biggie.
2) Clasifica cada producto en Grupo y Subgrupo.
3) Extrae unidad de medida (g, kg→g, ml, l→cc, unid., paq).
4) Registra FechaConsulta con fecha+hora+min+seg en zona UTC.
5) Consolida CSVs diarios y actualiza hoja de Google Sheets.
Columns: ['ID','Supermercado','Producto','Precio','Unidad','Grupo','Subgrupo','FechaConsulta']
"""
import os, sys, glob, re, json, unicodedata, tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
BASE_DIR       = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR        = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))
FILE_TAG       = "frutihort"
PATTERN_DAILY  = os.path.join(OUT_DIR, f"*canasta_{FILE_TAG}_*.csv")
SPREADSHEET_URL= os.environ.get("SPREADSHEET_URL")
CREDS_JSON     = os.environ.get("CREDS_JSON_PATH", os.path.abspath("creds.json"))
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "precios_supermercados")
MAX_WORKERS, REQ_TIMEOUT = 8, 10

# Columnas estándar y claves únicas
COLUMNS = ['Supermercado','Producto','Precio','Unidad','Grupo','Subgrupo','FechaConsulta']
KEY_COLS= ['Supermercado','Producto','FechaConsulta']

# Asegurar directorio de salida
os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Utilidades de texto ─────────────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)

def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ─────────────── 2. Clasificación Grupo/Subgrupo ─────────────────
BROAD_GROUP_KEYWORDS: Dict[str,List[str]] = {
    "Panificados": ["pan","baguette","bizcocho","galleta","masa"],
    "Frutas":      ["naranja","manzana","banana","pera","uva","frutilla"],
    "Verduras":    ["tomate","cebolla","papa","zanahoria","lechuga","espinaca"],
    "Huevos":      ["huevo","huevos","codorniz"],
    "Lácteos":     ["leche","yogur","queso","manteca","crema"],
}
BROAD_TOKENS = {g:{strip_accents(w) for w in ws} for g,ws in BROAD_GROUP_KEYWORDS.items()}

SUBGROUP_KEYWORDS: Dict[str,List[str]] = {
    "Naranja": ["naranja","naranjas"],
    "Cebolla": ["cebolla","cebollas"],
    "Leche Entera": ["entera"],
    "Leche Descremada": ["descremada"],
    "Queso Paraguay": ["paraguay"],
    "Huevo Gallina": ["gallina"],
    "Huevo Codorniz": ["codorniz"],
}
SUB_TOKENS = {sg:{strip_accents(w) for w in ws} for sg,ws in SUBGROUP_KEYWORDS.items()}

def classify(name: str) -> (str,str):
    toks = set(tokenize(name))
    grp = next((g for g,ks in BROAD_TOKENS.items() if toks & ks), "")
    sub = next((s for s,ks in SUB_TOKENS.items() if toks & ks), "")
    return grp, sub

# ─────────────── 3. Extracción de unidad ─────────────────────────────
_unit_re = re.compile(
    r"(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilos?|g|gr|ml|cc|l(?:itro)?s?|lt|unid(?:ad)?s?|u|paq|stk)\b",
    re.I
)

def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    if not m:
        return ""
    val = float(m.group('val').replace(',','.')) if m.group('val') else 1.0
    unit = m.group('unit').lower().rstrip('s')
    if unit in ('kg','kilo'):
        val *= 1000; unit_out = 'GR'
    elif unit in ('l','lt','litro'):
        val *= 1000; unit_out = 'CC'
    elif unit in ('g','gr'):
        unit_out = 'GR'
    elif unit in ('ml','cc'):
        unit_out = 'CC'
    else:
        unit_out = unit.upper()
    if val.is_integer(): val_str = str(int(val))
    else: val_str = f"{val:.2f}".rstrip('0').rstrip('.')
    return f"{val_str}{unit_out}"

# ─────────────── 4. Normalización de precio ───────────────────────────
_price_selectors = [
    "[data-price]","[data-price-final]","[data-price-amount]",
    "meta[itemprop='price']","span.price ins span.amount","span.price > span.amount",
    "span.woocommerce-Price-amount","span.amount","bdi","div.price","p.price"
]

def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]","",str(val)).replace('.', '').replace(',', '.')
    try: return float(txt)
    except: return 0.0

from bs4.element import Tag

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

# ─────────────── 5. HTTP session robusta ───────────────────────────────
def _build_session() -> requests.Session:
    retry = Retry(total=3,backoff_factor=1.2,status_forcelist=(429,500,502,503,504),allowed_methods=("GET","HEAD"))
    s = requests.Session()
    s.headers['User-Agent']='Mozilla/5.0'
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://',adapter); s.mount('https://',adapter)
    return s

# ─────────────── 6. Clase base scraper ───────────────────────────────
class HtmlSiteScraper:
    def __init__(self,name:str,base:str):
        self.name=name; self.base_url=base.rstrip('/'); self.session=_build_session()
    def category_urls(self)->List[str]: raise NotImplementedError
    def parse_category(self,url:str)->List[Dict]: raise NotImplementedError
    def scrape(self)->List[Dict]:
        ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        rows=[]
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futures = [pool.submit(self.parse_category,u) for u in self.category_urls()]
            for fut in as_completed(futures):
                for r in fut.result():
                    p=r.get('Precio',0)
                    if p<=0: continue
                    r['FechaConsulta']=ts
                    rows.append({c:r.get(c,'') if c!='Precio' else r.get('Precio',0) for c in COLUMNS})
        return rows
    def save_csv(self,rows:List[Dict]):
        if not rows: return
        fn=f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR,fn),index=False)

# ─────────────── 7. Stock ───────────────────────────────
class StockScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('stock','https://www.stock.com.py')
    def category_urls(self)->List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        kws=[tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [urljoin(self.base_url,a['href']) for a in soup.select('a[href*="/category/"]') if any(k in a['href'].lower() for k in kws)]
    def parse_category(self,url:str)->List[Dict]:
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for card in soup.select('div.product-item'):
            el=card.select_one('h2.product-title');
            if not el: continue
            nm=el.get_text(' ',strip=True)
            if is_excluded(nm): continue
            price=_first_price(card)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({'Supermercado':'stock','Producto':nm.upper(),'Precio':price,'Unidad':unit,'Grupo':grp,'Subgrupo':sub})
        return out

# ─────────────── 8. Superseis ───────────────────────────────
class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('superseis','https://www.superseis.com.py')
    def category_urls(self)->List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        kws=[tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [urljoin(self.base_url,a['href']) for a in soup.select('a.collapsed[href*="/category/"]') if any(k in a['href'].lower() for k in kws)]
    def parse_category(self,url:str)->List[Dict]:
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for link in soup.select('a.product-title-link'):
            nm=link.get_text(' ',strip=True)
            if is_excluded(nm): continue
            parent=link.find_parent('div.product-item') or link
            price=_first_price(parent)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({'Supermercado':'superseis','Producto':nm.upper(),'Precio':price,'Unidad':unit,'Grupo':grp,'Subgrupo':sub})
        return out

# ─────────────── 9. Salemma ───────────────────────────────
class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('salemma','https://www.salemmaonline.com.py')
    def category_urls(self)->List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        urls=set()
        for a in soup.find_all('a',href=True):
            h=a['href'].lower()
            if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst): urls.add(urljoin(self.base_url,h))
        return list(urls)
    def parse_category(self,url:str)->List[Dict]:
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for f in soup.select('form.productsListForm'):
            nm=f.find('input',{'name':'name'}).get('value','')
            if is_excluded(nm): continue
            price=norm_price(f.find('input',{'name':'price'}).get('value',''))
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({'Supermercado':'salemma','Producto':nm.upper(),'Precio':price,'Unidad':unit,'Grupo':grp,'Subgrupo':sub})
        return out

# ─────────────── 10. Arete y Jardines hereda ─────────────────────────
class AreteScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('arete','https://www.arete.com.py')
    def category_urls(self)->List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        urls=set()
        for sel in ('#departments-menu','#menu-departments-menu-1'):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                h=a['href'].split('?')[0].lower()
                if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst): urls.add(urljoin(self.base_url+'/',h))
        return list(urls)
    def parse_category(self,url:str)->List[Dict]:
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for card in soup.select('div.product'):
            el=card.select_one('h2.ecommercepro-loop-product__title')
            if not el: continue
            nm=el.get_text(' ',strip=True)
            if is_excluded(nm): continue
            price=_first_price(card)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({'Supermercado':'arete','Producto':nm.upper(),'Precio':price,'Unidad':unit,'Grupo':grp,'Subgrupo':sub})
        return out
class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name='losjardines'
        self.base_url='https://losjardinesonline.com.py'

# ─────────────── 11. Biggie (API) ─────────────────────────────
class BiggieScraper:
    name='biggie'; API='https://api.app.biggie.com.py/api/articles'; TAKE=100
    GROUPS=['huevos','lacteos','frutas','verduras','cereales','panificados']
    def __init__(self): self.session=_build_session()
    def category_urls(self): return self.GROUPS
    def parse_category(self,grp:str)->List[Dict]:
        out=[]; skip=0
        while True:
            js=self.session.get(self.API,params={'take':self.TAKE,'skip':skip,'classificationName':grp},timeout=REQ_TIMEOUT).json()
            for it in js.get('items',[]):
                nm=it.get('name','')
                price=norm_price(it.get('price',0))
                if price<=0: continue
                g,sub=classify(nm)
                unit=extract_unit(nm)
                out.append({'Supermercado':'biggie','Producto':nm.upper(),'Precio':price,'Unidad':unit,'Grupo':g or grp.capitalize(),'Subgrupo':sub})
            skip+=self.TAKE
            if skip>=js.get('count',0): break
        return out
    def scrape(self)->List[Dict]:
        ts=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        rows=[]
        for grp in self.GROUPS:
            for r in self.parse_category(grp): r['FechaConsulta']=ts; rows.append(r)
        return rows
    def save_csv(self,rows:List[Dict]):
        if not rows: return
        fn=f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR,fn),index=False)

# ─────────────── 12. Registro de scrapers ─────────────────────────
SCRAPERS: Dict[str,Callable] = {
    'stock': StockScraper,
    'superseis': SuperseisScraper,
    'salemma': SalemmaScraper,
    'arete': AreteScraper,
    'losjardines': JardinesScraper,
    'biggie': BiggieScraper,
}

# ─────────────── 13. Google Sheets & Orquestador ─────────────────────────
def _open_sheet():
    scopes=['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
    cred=Credentials.from_service_account_file(CREDS_JSON,scopes=scopes)
    gc=gspread.authorize(cred)
    sh=gc.open_by_url(SPREADSHEET_URL)
    try:
        ws=sh.worksheet(WORKSHEET_NAME)
        df=get_as_dataframe(ws,dtype=str,header=0,evaluate_formulas=False).dropna(how='all')
    except gspread.exceptions.WorksheetNotFound:
        ws=sh.add_worksheet(title=WORKSHEET_NAME,rows='10000',cols='40')
        df=pd.DataFrame(columns=COLUMNS)
    return ws,df

def _write_sheet(ws,df:pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws,df[COLUMNS],include_index=False)

def main():
    all_rows=[]
    for key,cls in SCRAPERS.items():
        inst=cls()
        rows=inst.scrape()
        inst.save_csv(rows)
        all_rows.extend(rows)
    if not all_rows:
        print('Sin datos nuevos.')
        return
    files=glob.glob(PATTERN_DAILY)
    df_all=pd.concat([pd.read_csv(f,dtype=str)[COLUMNS] for f in files],ignore_index=True)
    df_all['Precio']=pd.to_numeric(df_all['Precio'],errors='coerce').fillna(0.0)
    df_all['FechaConsulta']=pd.to_datetime(df_all['FechaConsulta'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    ws,df_prev=_open_sheet()
    merged=pd.concat([df_prev,df_all],ignore_index=True)
    merged.drop_duplicates(subset=KEY_COLS,keep='first',inplace=True)
    if 'ID' in merged.columns: merged.drop(columns=['ID'],inplace=True)
    merged.insert(0,'ID',range(1,len(merged)+1))
    _write_sheet(ws,merged)
    print(f'Hoja actualizada: {len(merged)} registros')

if __name__=='__main__':
    main()
