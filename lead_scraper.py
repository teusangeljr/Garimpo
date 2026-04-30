"""
lead_scraper.py — Motor de Prospecção de Leads
Fontes: Google Maps, CNPJ.biz, Encontrei, OLX, Mercado Livre, Jucesp
Enriquecimento: BrasilAPI (dados completos da Receita Federal)
Filtros: Porte, CNAE, Capital, Idade, Situação, Site, Redes Sociais
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
import re
import time
import json
import random
import socket
import urllib.parse
from datetime import datetime, date
from typing import List, Dict, Optional, Any
from dateutil.relativedelta import relativedelta
import os
import sys
import concurrent.futures
from script import EmailExtractor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# ─── Constantes ───────────────────────────────────────────────
EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

EMAIL_BLACKLIST = [
    'example.com', 'teste.com', 'test.com', 'domain.com', 'email.com',
    'sentry.io', 'wixpress.com', 'googleapis.com', 'gstatic.com',
    'schema.org', 'w3.org', 'yourcompany.com', 'company.com',
]

REDES_SOCIAIS = {
    'instagram': r'instagram\.com/(?!p/|explore/|accounts/)[a-zA-Z0-9_.]+',
    'facebook':  r'facebook\.com/(?!sharer|share|dialog|plugins)[a-zA-Z0-9_.]+',
    'linkedin':  r'linkedin\.com/(?:company|in)/[a-zA-Z0-9_-]+',
}

UF_MAP = {
    'AC':'Acre','AL':'Alagoas','AP':'Amapá','AM':'Amazonas','BA':'Bahia',
    'CE':'Ceará','DF':'Distrito Federal','ES':'Espírito Santo','GO':'Goiás',
    'MA':'Maranhão','MT':'Mato Grosso','MS':'Mato Grosso do Sul','MG':'Minas Gerais',
    'PA':'Pará','PB':'Paraíba','PR':'Paraná','PE':'Pernambuco','PI':'Piauí',
    'RJ':'Rio de Janeiro','RN':'Rio Grande do Norte','RS':'Rio Grande do Sul',
    'RO':'Rondônia','RR':'Roraima','SC':'Santa Catarina','SP':'São Paulo',
    'SE':'Sergipe','TO':'Tocantins',
}

BRASILAPI_BASE = "https://brasilapi.com.br/api/cnpj/v1"
ML_API_BASE    = "https://api.mercadolibre.com/sites/MLB/search"
RECEITAWS_BASE = "https://receitaws.com.br/v1/cnpj"

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'pt-BR,pt;q=0.9',
}


# ─── Helpers ───────────────────────────────────────────────────

def _is_valid_email(email: str) -> bool:
    return not any(bad in email.lower() for bad in EMAIL_BLACKLIST)


def _extract_emails(text: str) -> List[str]:
    found = re.findall(EMAIL_REGEX, text)
    return list({e.lower() for e in found if _is_valid_email(e)})


def _clean_cnpj(cnpj: str) -> str:
    return re.sub(r'\D', '', cnpj)


def _empty_lead(fonte: str) -> Dict:
    return {
        'nome': '', 'empresa': '', 'cnpj': '', 'email': '', 'site': '',
        'telefone': '', 'endereco': '', 'municipio': '', 'uf': '',
        'porte': '', 'natureza_juridica': '', 'capital_social': 0,
        'situacao_cadastral': '', 'data_abertura': '', 'cnae': '',
        'cnae_descricao': '', 'socios': [], 'instagram': '',
        'facebook': '', 'linkedin': '', 'tem_site': False,
        'fonte': fonte,
    }


# ─── Enriquecimento BrasilAPI ──────────────────────────────────

def enriquecer_cnpj(cnpj: str) -> Optional[Dict]:
    """
    Busca dados completos da Receita Federal via BrasilAPI (gratuito).
    Retorna dict com todos os campos disponíveis ou None em caso de erro.
    """
    cnpj_clean = _clean_cnpj(cnpj)
    if len(cnpj_clean) != 14:
        return None

    try:
        r = requests.get(
            f"{BRASILAPI_BASE}/{cnpj_clean}",
            headers=HEADERS, timeout=10
        )
        if r.status_code == 200:
            return r.json()
        # Fallback rápido: ReceitaWS
        r2 = requests.get(
            f"{RECEITAWS_BASE}/{cnpj_clean}",
            headers=HEADERS, timeout=5
        )
        if r2.status_code == 200:
            return r2.json()
    except Exception:
        pass
    return None


def _merge_cnpj_data(lead: Dict, cnpj_data: Dict) -> Dict:
    """Funde dados da Receita Federal no lead."""
    if not cnpj_data:
        return lead

    lead['cnpj']            = cnpj_data.get('cnpj', lead['cnpj'])
    lead['empresa']         = cnpj_data.get('razao_social') or lead['empresa']
    lead['nome']            = cnpj_data.get('nome_fantasia') or lead['empresa']
    lead['municipio']       = cnpj_data.get('municipio', lead['municipio'])
    lead['uf']              = cnpj_data.get('uf', lead['uf'])
    lead['situacao_cadastral'] = cnpj_data.get('situacao_cadastral', lead['situacao_cadastral'])
    lead['porte']           = cnpj_data.get('porte', lead['porte'])
    lead['natureza_juridica'] = cnpj_data.get('natureza_juridica', lead['natureza_juridica'])
    lead['capital_social']  = float(cnpj_data.get('capital_social', 0) or 0)
    lead['data_abertura']   = cnpj_data.get('data_inicio_atividade', lead['data_abertura'])
    lead['cnae']            = str(cnpj_data.get('cnae_fiscal', lead['cnae']))
    lead['cnae_descricao']  = cnpj_data.get('cnae_fiscal_descricao', lead['cnae_descricao'])
    lead['socios']          = [
        s.get('nome_socio', '') for s in (cnpj_data.get('qsa') or [])
    ]

    # Email da Receita
    email_receita = cnpj_data.get('email', '')
    if email_receita and _is_valid_email(email_receita) and not lead['email']:
        lead['email'] = email_receita.lower()

    # Telefone
    tel = cnpj_data.get('ddd_telefone_1', '')
    if tel and not lead['telefone']:
        lead['telefone'] = tel

    # Endereço
    logradouro = cnpj_data.get('logradouro', '')
    numero     = cnpj_data.get('numero', '')
    bairro     = cnpj_data.get('bairro', '')
    if logradouro and not lead['endereco']:
        lead['endereco'] = f"{logradouro}, {numero} — {bairro}".strip(' —,')

    return lead


# ─── Site / Redes Sociais helpers ─────────────────────────────

def verificar_site(url: str) -> bool:
    """Verifica se o domínio do site resolve via DNS."""
    if not url:
        return False
    try:
        domain = re.sub(r'^https?://', '', url).split('/')[0]
        socket.gethostbyname(domain)
        return True
    except Exception:
        return False


def detectar_redes_sociais(html: str) -> Dict[str, str]:
    """Detecta links de redes sociais no HTML do site."""
    resultado = {'instagram': '', 'facebook': '', 'linkedin': ''}
    for rede, pattern in REDES_SOCIAIS.items():
        match = re.search(pattern, html)
        if match:
            resultado[rede] = 'https://' + match.group()
    return resultado


# ─── CNPJ Filter Pipeline ─────────────────────────────────────

def _idade_empresa_anos(data_abertura: str) -> Optional[int]:
    """Calcula a idade da empresa em anos a partir da data_inicio_atividade."""
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            dt = datetime.strptime(data_abertura, fmt).date()
            return relativedelta(date.today(), dt).years
        except ValueError:
            continue
    return None


def aplicar_filtros(leads: List[Dict], filtros: Dict) -> List[Dict]:
    """
    Pipeline de qualificação. Filtros suportados:
      - apenas_ativa: bool
      - portes: list (ex: ['MEI', 'ME'])
      - max_idade_anos: int
      - capital_min: float
      - capital_max: float
      - cnae: str (começa-com)
      - uf: str
      - municipio: str (contém)
      - tem_site: bool
      - tem_redes_sociais: bool
      - apenas_com_email: bool
    """
    resultado = []
    for lead in leads:
        # Situação
        if filtros.get('apenas_ativa'):
            sit = lead.get('situacao_cadastral', '').upper()
            if sit and 'ATIVA' not in sit:
                continue

        # Porte
        portes = filtros.get('portes', [])
        if portes and lead.get('porte'):
            if not any(p.upper() in lead['porte'].upper() for p in portes):
                continue

        # Idade da empresa
        max_idade = filtros.get('max_idade_anos')
        if max_idade is not None and lead.get('data_abertura'):
            idade = _idade_empresa_anos(lead['data_abertura'])
            if idade is not None and idade > max_idade:
                continue

        # Capital social
        cap = lead.get('capital_social', 0) or 0
        if filtros.get('capital_min') and cap < filtros['capital_min']:
            continue
        if filtros.get('capital_max') and cap > filtros['capital_max']:
            continue

        # CNAE
        cnae_filtro = filtros.get('cnae', '').replace('-', '').replace('/', '')
        lead_cnae   = str(lead.get('cnae', '')).replace('-', '').replace('/', '')
        if cnae_filtro and not lead_cnae.startswith(cnae_filtro):
            continue

        # UF
        if filtros.get('uf') and lead.get('uf'):
            if lead['uf'].upper() != filtros['uf'].upper():
                continue

        # Município
        mun_filtro = filtros.get('municipio', '').upper()
        if mun_filtro and lead.get('municipio'):
            if mun_filtro not in lead['municipio'].upper():
                continue

        # Tem site
        if filtros.get('tem_site') and not lead.get('tem_site'):
            continue

        # Tem redes sociais
        if filtros.get('tem_redes_sociais'):
            tem = lead.get('instagram') or lead.get('facebook') or lead.get('linkedin')
            if not tem:
                continue

        # Apenas com email
        if filtros.get('apenas_com_email') and not lead.get('email'):
            continue

        resultado.append(lead)

    return resultado


# ══════════════════════════════════════════════════════════════
# LeadScraper — Classe Principal
# ══════════════════════════════════════════════════════════════

class LeadScraper:

    def __init__(self, headless: bool = True, browser: str = 'edge'):
        self.headless = headless
        self.browser  = browser.lower()
        self.driver   = None
        self.wait     = None
        self.session  = self._configurar_sessao()

    def _configurar_sessao(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(HEADERS)
        return session

    # ─── Driver ──────────────────────────────────────────────

    def _iniciar_driver(self):
        if sys.platform.startswith('linux'):
            self.browser = 'chrome'
            opts = webdriver.ChromeOptions()
            opts.binary_location = '/opt/render/project/src/.chrome/opt/google/chrome/google-chrome'
        elif self.browser == 'chrome':
            opts = webdriver.ChromeOptions()
        elif self.browser == 'firefox':
            opts = webdriver.FirefoxOptions()
        else:
            opts = webdriver.EdgeOptions()
            opts.use_chromium = True

        if self.headless:
            opts.add_argument('--headless=new')

        # Estratégia de carregamento rápido (não espera imagens/anúncios)
        opts.page_load_strategy = 'eager'

        # Preferências para economizar RAM e Banda
        prefs = {
            "profile.managed_default_content_settings.images": 2, # Bloqueia Imagens
            "profile.default_content_setting_values.notifications": 2,
            "profile.managed_default_content_settings.stylesheets": 2, # Opcional: Bloqueia CSS (pode quebrar alguns sites)
        }
        # Nota: Bloquear CSS pode quebrar sites que dependem de visibilidade. Vou manter apenas imagens por enquanto.
        prefs = {"profile.managed_default_content_settings.images": 2}
        opts.add_experimental_option("prefs", prefs)

        for arg in [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage', '--no-sandbox', '--disable-gpu',
            '--disable-extensions', '--disable-infobars', '--mute-audio',
            '--disable-browser-side-navigation',
            '--lang=pt-BR', '--window-size=1366,768',
        ]:
            opts.add_argument(arg)
        opts.add_argument(f'--user-agent={HEADERS["User-Agent"]}')

        if self.browser == 'chrome':
            # No Linux (Render), usa o ChromeDriver baixado pelo render-build.sh
            chromedriver_path = '/opt/render/project/src/.chrome/chromedriver'
            if sys.platform.startswith('linux') and os.path.exists(chromedriver_path):
                print(f"   Usando ChromeDriver local: {chromedriver_path}")
                self.driver = webdriver.Chrome(
                    service=ChromeService(executable_path=chromedriver_path),
                    options=opts
                )
            else:
                self.driver = webdriver.Chrome(options=opts)
        elif self.browser == 'firefox':
            self.driver = webdriver.Firefox(options=opts)
        else:
            self.driver = webdriver.Edge(options=opts)

        self.driver.set_page_load_timeout(15)
        self.wait = WebDriverWait(self.driver, 8)

    def _fechar_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    # ─── Email extraction from website ───────────────────────

    def _email_do_site(self, url: str) -> Optional[str]:
        if not url or not url.startswith('http'):
            return None
        try:
            self.driver.get(url)
            time.sleep(0.8)
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith('mailto:'):
                    candidate = href.replace('mailto:', '').split('?')[0].strip()
                    if _is_valid_email(candidate):
                        return candidate.lower()

            emails = _extract_emails(soup.get_text())
            if emails:
                return emails[0]

            # Try contact page
            for a in soup.find_all('a', href=True):
                text = (a.get_text() or '').lower()
                link = a['href'].lower()
                if any(k in text or k in link
                       for k in ['contato', 'contact', 'fale-conosco']):
                    href = a['href']
                    if not href.startswith('http'):
                        base = '/'.join(url.split('/')[:3])
                        href = base + ('' if href.startswith('/') else '/') + href
                    if href == url:
                        continue
                    try:
                        self.driver.get(href)
                        time.sleep(0.8)
                        html2 = self.driver.page_source
                        soup2 = BeautifulSoup(html2, 'html.parser')
                        for a2 in soup2.find_all('a', href=True):
                            if a2['href'].startswith('mailto:'):
                                c = a2['href'].replace('mailto:', '').split('?')[0].strip()
                                if _is_valid_email(c):
                                    return c.lower()
                        emails2 = _extract_emails(soup2.get_text())
                        if emails2:
                            return emails2[0]
                    except Exception:
                        pass
                    break
        except Exception:
            pass
        return None

    def _enriquecer_lead_com_site(self, lead: Dict) -> Dict:
        """Visita o site para extrair email e redes sociais."""
        if not lead.get('site'):
            return lead
        try:
            lead['tem_site'] = verificar_site(lead['site'])
            if not lead['tem_site']:
                return lead

            if not lead.get('email'):
                email = self._email_do_site(lead['site'])
                lead['email'] = email or ''

            # Redes sociais
            try:
                self.driver.get(lead['site'])
                time.sleep(0.6)
                redes = detectar_redes_sociais(self.driver.page_source)
                lead.update(redes)
            except Exception:
                pass
        except Exception:
            pass
        return lead

    # ══════════════════════════════════════════════════════════
    # FONTES DE DADOS
    # ══════════════════════════════════════════════════════════

    # ─── 1. Google Maps ──────────────────────────────────────

    def buscar_google_maps(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        leads = []
        query = f"{nicho} em {localizacao}"
        print(f"\n🗺️  Google Maps: '{query}'")

        try:
            self._iniciar_driver() # Lazy init
            url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"
            self.driver.get(url)
            
            # Espera carregar o feed
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]')))
            except:
                pass

            # Scroll mais eficiente
            for _ in range(4):
                try:
                    painel = self.driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
                    self.driver.execute_script("arguments[0].scrollTop += 2000;", painel)
                    time.sleep(0.5)
                except Exception:
                    break

            # Variedade: Pega mais resultados e escolhe aleatoriamente
            pool_results = self.driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
            random.shuffle(pool_results)
            print(f"   {len(pool_results)} negócios encontrados (Pool: {len(pool_results)})")

            for card in pool_results[:max_results]:
                lead = _empty_lead('Google Maps')
                try:
                    nome = card.get_attribute('aria-label') or ''
                    lead['nome'] = nome
                    lead['empresa'] = nome
                    
                    # Usa JS para clicar e evitar problemas de visibilidade
                    self.driver.execute_script("arguments[0].click();", card)
                    time.sleep(0.5) # Espera mínima para carregar detalhes

                    # Site
                    try:
                        site_el = self.driver.find_elements(By.CSS_SELECTOR, 'a[data-item-id="authority"]')
                        if site_el:
                            lead['site'] = site_el[0].get_attribute('href') or ''
                    except: pass

                    # Telefone
                    try:
                        tel_el = self.driver.find_elements(By.CSS_SELECTOR, 'button[data-item-id^="phone"]')
                        if tel_el:
                            lead['telefone'] = (tel_el[0].get_attribute('aria-label') or '').replace('Telefone: ', '')
                    except: pass

                    # Endereço
                    try:
                        addr_el = self.driver.find_elements(By.CSS_SELECTOR, 'button[data-item-id="address"]')
                        if addr_el:
                            lead['endereco'] = addr_el[0].get_attribute('aria-label') or ''
                    except: pass

                    leads.append(lead)
                    print(f"   ✅ {lead['empresa']}")

                except Exception as e:
                    print(f"   ⚠️  Maps item: {str(e)[:40]}")
                    continue

        except Exception as e:
            print(f"   ❌ Google Maps erro: {str(e)[:80]}")

        return leads

    def buscar_sites_de_url_google_maps(self, url: str) -> List[str]:
        """Abre uma URL direta do Google Maps e extrai os sites listados nela."""
        sites = []
        print(f"\n🗺️  Google Maps Direto: '{url[:60]}...'")

        try:
            self._iniciar_driver() # Lazy init
            self.driver.get(url)
            time.sleep(2) # Espera carregar inicial
            
            # Tenta verificar se é uma página de um lugar específico (já aberto)
            try:
                site_el = self.driver.find_elements(By.CSS_SELECTOR, 'a[data-item-id="authority"]')
                if site_el:
                    href = site_el[0].get_attribute('href')
                    if href:
                        sites.append(href)
                        print(f"   Encontrado site direto do lugar: {href}")
                        # Pode haver outros na tela de "Lugares parecidos", mas priorizamos o principal
                        if len(self.driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')) == 0:
                            return sites
            except: pass

            # Se não for um lugar específico ou for uma busca
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]')))
            except:
                pass

            # Scroll no painel para carregar resultados
            for _ in range(6):
                try:
                    painel = self.driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
                    self.driver.execute_script("arguments[0].scrollTop += 3000;", painel)
                    time.sleep(0.5)
                except Exception:
                    break

            pool_results = self.driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
            print(f"   {len(pool_results)} negócios encontrados no mapa")

            for card in pool_results[:25]: # Extrai até 25 sites para não demorar demais
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                    time.sleep(0.1)
                    self.driver.execute_script("arguments[0].click();", card)
                    time.sleep(0.8) # Espera carregar detalhes
                    
                    try:
                        site_els = self.driver.find_elements(By.CSS_SELECTOR, 'a[data-item-id="authority"]')
                        if site_els:
                            href = site_els[0].get_attribute('href')
                            if href and href not in sites and 'google.com' not in href:
                                sites.append(href)
                                print(f"   ✅ Extraído: {href}")
                    except: pass
                except Exception:
                    continue

        except Exception as e:
            print(f"   ❌ Google Maps URL erro: {str(e)[:80]}")
        finally:
            self._fechar_driver()

        return sites

    # ─── 2. CNPJ.biz (normal + por CNAE) ────────────────────

    def buscar_cnpj_biz(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        leads = []
        print(f"\n🏢 CNPJ.biz: '{nicho}' em '{localizacao}'")
        try:
            query = urllib.parse.quote(f"{nicho} {localizacao}")
            url = f"https://cnpj.biz/procura/{query}"
            
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                print(f"   ⚠️ CNPJ.biz retornou status {r.status_code}")
                return leads

            soup = BeautifulSoup(r.text, 'html.parser')
            links = soup.select('a[href*="/cnpj/"]')
            random.shuffle(links)
            print(f"   {len(links)} resultados encontrados (Pool)")

            for link in links[:max_results]:
                lead = _empty_lead('CNPJ.biz')
                try:
                    nome = link.get_text(strip=True)
                    if not nome:
                        continue
                    lead['empresa'] = nome
                    lead['nome'] = nome

                    href = link['href']
                    emp_url = href if href.startswith('http') else f"https://cnpj.biz{href}"

                    # Extrai CNPJ da URL
                    cnpj_match = re.search(r'/(\d{14})$', href)
                    if cnpj_match:
                        lead['cnpj'] = cnpj_match.group(1)

                    # Tenta pegar detalhes via requests
                    re_emp = self.session.get(emp_url, timeout=10)
                    if re_emp.status_code == 200:
                        emp_soup = BeautifulSoup(re_emp.text, 'html.parser')
                        
                        for a in emp_soup.find_all('a', href=True):
                            href_a = a['href']
                            if (href_a.startswith('http') and
                                    'cnpj.biz' not in href_a and
                                    'google' not in href_a and
                                    'facebook' not in href_a and
                                    'instagram' not in href_a):
                                lead['site'] = href_a
                                break

                        emails = _extract_emails(emp_soup.get_text())
                        if emails:
                            lead['email'] = emails[0]

                    leads.append(lead)
                    print(f"   ✅ {lead['empresa']} | email: {lead['email'] or '—'}")

                except Exception as e:
                    print(f"   ⚠️  {str(e)[:50]}")
                    continue

        except Exception as e:
            print(f"   ❌ CNPJ.biz erro: {str(e)[:80]}")

        return leads

    def buscar_por_cnae(self, cnae: str, uf: str, municipio: str = '', max_results: int = 20) -> List[Dict]:
        """Busca empresas por CNAE + UF no CNPJ.biz."""
        leads = []
        cnae_clean = re.sub(r'\D', '', cnae)[:4]  # Primeiros 4 dígitos
        print(f"\n📊 CNAE Search: {cnae} | {uf} | {municipio}")

        try:
            # QueroCNPJ por CNAE
            mun_q = urllib.parse.quote(municipio) if municipio else ''
            url = (
                f"https://querocnpj.com.br/empresas?cnae={cnae_clean}"
                f"&uf={uf.upper()}"
                + (f"&municipio={mun_q}" if municipio else '')
            )
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                print(f"   ⚠️ CNAE Search retornou status {r.status_code}")
                return leads

            soup = BeautifulSoup(r.text, 'html.parser')

            # Tenta extrair CNPJs ou nomes de empresa
            cnpj_links = soup.select('a[href*="cnpj"]')

            for link in cnpj_links[:max_results]:
                lead = _empty_lead('CNAE/QueroCNPJ')
                try:
                    nome = link.get_text(strip=True)
                    lead['empresa'] = nome
                    lead['nome'] = nome

                    # Extrai CNPJ se presente na URL
                    cnpj_match = re.search(r'(\d{14})', link.get('href', ''))
                    if cnpj_match:
                        lead['cnpj'] = cnpj_match.group(1)

                    leads.append(lead)
                except Exception:
                    continue

            print(f"   {len(leads)} empresas encontradas via CNAE")

        except Exception as e:
            print(f"   ❌ CNAE Search erro: {str(e)[:80]}")

        return leads

    # ─── 3. Encontrei.com.br ─────────────────────────────────

    def buscar_encontrei(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        leads = []
        print(f"\n📒 Encontrei: '{nicho}' em '{localizacao}'")
        try:
            nicho_s = nicho.lower().replace(' ', '-')
            loc_s   = localizacao.lower().replace(' ', '-')
            url = f"https://www.encontrei.com/{nicho_s}/{loc_s}"
            
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                print(f"   ⚠️ Encontrei retornou status {r.status_code}")
                return leads

            soup = BeautifulSoup(r.text, 'html.parser')
            # Variedade: Pega os resultados e embaralha antes de limitar
            cards = soup.select('a[href*="/empresa/"], a[href*="/negocio/"]')
            cards += soup.select('div.empresa-card, article.business-card, .listing-item')
            random.shuffle(cards)
            print(f"   {len(cards)} resultados encontrados (Pool)")

            for card in cards[:max_results]:
                lead = _empty_lead('Encontrei.com.br')
                try:
                    nome_el = card.select_one('h2, h3, .nome, strong')
                    if nome_el:
                        lead['empresa'] = nome_el.get_text(strip=True)
                        lead['nome'] = lead['empresa']

                    link_el = card if card.name == 'a' else card.select_one('a[href]')
                    if link_el:
                        href = link_el.get('href', '')
                        if href:
                            emp_url = href if href.startswith('http') else f"https://www.encontrei.com{href}"
                            
                            r_emp = self.session.get(emp_url, timeout=10)
                            if r_emp.status_code == 200:
                                emp_soup = BeautifulSoup(r_emp.text, 'html.parser')

                                emails = _extract_emails(emp_soup.get_text())
                                if emails:
                                    lead['email'] = emails[0]

                                for a in emp_soup.find_all('a', href=True):
                                    href_a = a['href']
                                    if (href_a.startswith('http') and
                                            'encontrei.com' not in href_a and
                                            'facebook' not in href_a and
                                            'instagram' not in href_a):
                                        lead['site'] = href_a
                                        break

                    if lead['empresa']:
                        leads.append(lead)
                        print(f"   ✅ {lead['empresa']} | email: {lead['email'] or '—'}")

                except Exception as e:
                    print(f"   ⚠️  {str(e)[:50]}")
                    continue

        except Exception as e:
            print(f"   ❌ Encontrei erro: {str(e)[:80]}")

        return leads

    # ─── 4. OLX ──────────────────────────────────────────────

    def buscar_olx(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        """Scrapa anúncios de negócios no OLX."""
        leads = []
        print(f"\n🛒 OLX: '{nicho}' em '{localizacao}'")

        try:
            query = urllib.parse.quote(nicho)
            url = f"https://www.olx.com.br/brasil?q={query}"
            
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                print(f"   ⚠️ OLX retornou status {r.status_code}")
                return leads

            soup = BeautifulSoup(r.text, 'html.parser')

            # Links de anúncio
            ad_links = soup.select('a[href*=".olx.com.br/"]')
            ad_links = [
                a['href'] for a in ad_links
                if re.search(r'/\d+\.html$', a['href'])
            ]
            ad_links = list(dict.fromkeys(ad_links))  # dedup
            print(f"   {len(ad_links)} anúncios encontrados")

            for link in ad_links[:max_results]:
                lead = _empty_lead('OLX')
                try:
                    r_ad = self.session.get(link, timeout=10)
                    if r_ad.status_code == 200:
                        page_soup = BeautifulSoup(r_ad.text, 'html.parser')

                        # Título
                        titulo = page_soup.select_one('h1')
                        if titulo:
                            lead['empresa'] = titulo.get_text(strip=True)
                            lead['nome']    = lead['empresa']

                        # Descrição / texto da página
                        texto = page_soup.get_text()

                        # Email
                        emails = _extract_emails(texto)
                        if emails:
                            lead['email'] = emails[0]

                        # Telefone
                        tel = re.search(r'\(?\d{2}\)?\s?\d{4,5}[-\s]?\d{4}', texto)
                        if tel:
                            lead['telefone'] = tel.group()

                    if lead['empresa']:
                        leads.append(lead)
                        print(f"   ✅ {lead['empresa']} | email: {lead['email'] or '—'}")

                except Exception as e:
                    print(f"   ⚠️  {str(e)[:50]}")
                    continue

        except Exception as e:
            print(f"   ❌ OLX erro: {str(e)[:80]}")

        return leads

    # ─── 5. Mercado Livre (API Pública) ──────────────────────

    def buscar_mercado_livre(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        """Usa a API pública do Mercado Livre para buscar sellers."""
        leads = []
        print(f"\n🛍️  Mercado Livre: '{nicho}'")

        # Mapa de estados BR para IDs do ML
        uf_ml = {
            'SP': 'BR-SP', 'RJ': 'BR-RJ', 'MG': 'BR-MG', 'RS': 'BR-RS',
            'PR': 'BR-PR', 'SC': 'BR-SC', 'BA': 'BR-BA', 'PE': 'BR-PE',
            'CE': 'BR-CE', 'GO': 'BR-GO', 'PA': 'BR-PA', 'AM': 'BR-AM',
            'DF': 'BR-DF', 'MT': 'BR-MT', 'MS': 'BR-MS', 'ES': 'BR-ES',
        }

        # Detecta UF da localização
        state_id = None
        for uf_code in uf_ml:
            if uf_code in localizacao.upper():
                state_id = uf_ml[uf_code]
                break

        try:
            params = {
                'q': nicho,
                'limit': min(max_results, 50),
            }
            if state_id:
                params['state_id'] = state_id

            r = requests.get(ML_API_BASE, params=params, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")

            data = r.json()
            items = data.get('results', [])
            print(f"   {len(items)} itens encontrados")

            sellers_seen = set()

            for item in items:
                seller = item.get('seller', {})
                seller_id = seller.get('id')
                if not seller_id or seller_id in sellers_seen:
                    continue
                sellers_seen.add(seller_id)

                lead = _empty_lead('Mercado Livre')
                lead['empresa'] = seller.get('nickname', '')
                lead['nome']    = lead['empresa']

                # Detalhes do vendedor via API de usuários
                try:
                    user_url = f"https://api.mercadolibre.com/users/{seller_id}"
                    ru = requests.get(user_url, headers=HEADERS, timeout=8)
                    if ru.status_code == 200:
                        udata = ru.json()
                        lead['municipio'] = udata.get('city', '')
                        lead['uf']       = udata.get('state', {}).get('id', '').replace('BR-', '')
                        link = udata.get('permalink', '')
                        if link:
                            lead['site'] = link
                except Exception:
                    pass

                if lead['empresa']:
                    leads.append(lead)
                    print(f"   ✅ {lead['empresa']} | uf: {lead['uf'] or '—'}")

                if len(leads) >= max_results:
                    break

        except Exception as e:
            print(f"   ❌ Mercado Livre erro: {str(e)[:80]}")

        return leads

    # ─── 6. Jucesp (SP — Empresas Novas) ─────────────────────

    def buscar_jucesp(self, nicho: str = '', max_results: int = 20) -> List[Dict]:
        """Scrapa empresas recém-abertas na Jucesp (Estado de SP)."""
        leads = []
        print(f"\n🏛️  Jucesp: empresas recém-abertas em SP")

        try:
            query = urllib.parse.quote(nicho) if nicho else ''
            url = f"https://www.jucesponline.sp.gov.br/{('?q=' + query) if query else ''}"
            self.driver.get(url)
            time.sleep(1.6)

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            rows = soup.select('table tr, .empresa-row, .result-row')

            for row in rows[:max_results]:
                lead = _empty_lead('Jucesp/SP')
                try:
                    cells = row.select('td')
                    if cells:
                        lead['empresa'] = cells[0].get_text(strip=True)
                        lead['nome']    = lead['empresa']
                        if len(cells) > 1:
                            lead['cnpj'] = _clean_cnpj(cells[1].get_text())
                        if len(cells) > 2:
                            lead['data_abertura'] = cells[2].get_text(strip=True)

                    if lead['empresa']:
                        leads.append(lead)
                except Exception:
                    continue

            print(f"   {len(leads)} empresas encontradas")

        except Exception as e:
            print(f"   ❌ Jucesp erro: {str(e)[:80]}")

        return leads

    # ─── 7. Yelp (Australia / Spain) ─────────────────────────

    def buscar_yelp(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        """Scrapa anúncios de negócios no Yelp (AU/ES)."""
        leads = []
        
        # Decide domain based on location or UF
        domain = "yelp.com.au" if "AU" in localizacao.upper() or "AUSTRALIA" in localizacao.upper() else "yelp.es"
        print(f"\n🥘 Yelp ({domain}): '{nicho}' em '{localizacao}'")

        try:
            query = urllib.parse.quote(nicho)
            loc_q = urllib.parse.quote(localizacao)
            url = f"https://www.{domain}/search?find_desc={query}&find_loc={loc_q}"
            
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                print(f"   ⚠️ Yelp retornou status {r.status_code}")
                return leads

            soup = BeautifulSoup(r.text, 'html.parser')
            # Selectors for businesses in search results
            biz_links = soup.select('a[href*="/biz/"]')
            # Filter unique biz links
            seen_hrefs = set()
            
            for link in biz_links:
                href = link.get('href', '')
                if href in seen_hrefs or '/biz/' not in href or 'osq=' not in href:
                    continue
                seen_hrefs.add(href)
                
                lead = _empty_lead(f'Yelp {domain}')
                try:
                    biz_url = f"https://www.{domain}{href}"
                    r_biz = self.session.get(biz_url, timeout=10)
                    if r_biz.status_code == 200:
                        biz_soup = BeautifulSoup(r_biz.text, 'html.parser')
                        
                        titulo = biz_soup.find('h1')
                        if titulo:
                            lead['empresa'] = titulo.get_text(strip=True)
                            lead['nome']    = lead['empresa']
                        
                        # Site
                        for a in biz_soup.find_all('a', href=True):
                            if '/biz_redir' in a['href']:
                                parsed_url = urllib.parse.parse_qs(urllib.parse.urlparse(a['href']).query)
                                if 'url' in parsed_url:
                                    lead['site'] = parsed_url['url'][0]
                                    break
                        
                        # Telefone
                        tel_tag = biz_soup.find(string=re.compile(r'Phone number|Teléfono'))
                        if tel_tag:
                            parent = tel_tag.find_parent()
                            if parent:
                                lead['telefone'] = parent.get_text(strip=True).replace('Phone number', '').replace('Teléfono', '')

                        # Endereço
                        addr_tag = biz_soup.find(string=re.compile(r'Get Directions|Cómo llegar'))
                        if addr_tag:
                            parent = addr_tag.find_parent('p')
                            if parent:
                                lead['endereco'] = parent.get_text(strip=True).replace('Get Directions', '').replace('Cómo llegar', '')

                    if lead['empresa']:
                        # Extração de email do site se houver
                        if lead['site'] and not lead['email']:
                            lead['tem_site'] = verificar_site(lead['site'])
                        
                        leads.append(lead)
                        print(f"   ✅ {lead['empresa']} | email: {lead['email'] or '—'}")

                    if len(leads) >= max_results:
                        break

                except Exception as e:
                    print(f"   ⚠️  Erro no Yelp biz: {str(e)[:50]}")
                    continue

        except Exception as e:
            print(f"   ❌ Yelp erro: {str(e)[:80]}")

        return leads

    # ─── 8. LinkedIn Sales Navigator (Draft) ───────────────

    def buscar_linkedin(self, nicho: str, localizacao: str, max_results: int = 20) -> List[Dict]:
        """
        Scrapa leads do LinkedIn Sales Navigator. 
        IMPORTANTE: Requer que o driver já esteja logado ou use cookies.
        """
        leads = []
        print(f"\n🔗 LinkedIn Sales Navigator: '{nicho}' em '{localizacao}'")

        try:
            # Tenta carregar cookies se existirem
            cookies_path = os.path.join(os.getcwd(), 'backend', 'linkedin_cookies.json')
            if os.path.exists(cookies_path):
                self.driver.get("https://www.linkedin.com")
                with open(cookies_path, 'r') as f:
                    cookies = json.load(f)
                    for cookie in cookies:
                        self.driver.add_cookie(cookie)
                print("   🍪 Cookies do LinkedIn carregados")
            
            query = urllib.parse.quote(f"{nicho} {localizacao}")
            url = f"https://www.linkedin.com/sales/search/people?keywords={query}"
            
            self.driver.get(url)
            time.sleep(2.0)

            if "login" in self.driver.current_url:
                print("   ⚠️  Redirecionado para Login. LinkedIn requer sessão ativa.")
                return leads

            # Scroll para carregar resultados
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.8)

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            # Variedade: Pega os resultados e embaralha antes de limitar
            items = soup.select('li.artdeco-list__item')
            random.shuffle(items)
            print(f"   {len(items)} perfis encontrados (Pool)")

            for item in items[:max_results]:
                lead = _empty_lead('LinkedIn')
                try:
                    nome_el = item.select_one('.result-lockup__full-name')
                    if nome_el:
                        lead['nome'] = nome_el.get_text(strip=True)
                    
                    cargo_el = item.select_one('.result-lockup__highlight-keyword')
                    if cargo_el:
                        lead['cargo'] = cargo_el.get_text(strip=True)
                        
                    empresa_el = item.select_one('.result-lockup__position-company')
                    if empresa_el:
                        lead['empresa'] = empresa_el.get_text(strip=True)
                    
                    if lead['nome']:
                        # LinkedIn leads rarely have public email directly in search
                        # Usually requires clicking to view profile or guessing based on company
                        leads.append(lead)
                        print(f"   ✅ {lead['nome']} | {lead['empresa']}")

                except Exception:
                    continue

        except Exception as e:
            print(f"   ❌ LinkedIn erro: {str(e)[:80]}")

        return leads

    # ══════════════════════════════════════════════════════════
    # Busca Consolidada
    # ══════════════════════════════════════════════════════════

    def buscar_leads(
        self,
        nicho: str            = '',
        localizacao: str      = '',
        cargo: str            = '',
        cnae: str             = '',
        uf: str               = '',
        municipio: str        = '',
        usar_google_maps: bool  = True,
        usar_cnpj_biz: bool     = True,
        usar_encontrei: bool    = True,
        usar_olx: bool          = False,
        usar_mercado_livre: bool = False,
        usar_jucesp: bool       = False,
        usar_cnae_search: bool  = False,
        usar_yelp: bool         = False,
        usar_linkedin: bool     = False,
        headless: bool          = True,
        enriquecer_cnpj: bool   = True,
        max_por_fonte: int      = 15,
        filtros: Optional[Dict] = None,
    ) -> Dict:
        """
        Busca e consolida leads de todas as fontes selecionadas,
        enriquece com dados da Receita Federal e aplica filtros.
        """
        print(f"\n{'='*60}")
        print(f"🔍 Prospecção iniciada (MODO VELOZ)")
        print(f"   Nicho: {nicho or '—'}  |  Local: {localizacao or '—'}")
        print(f"   CNAE: {cnae or '—'}  |  UF: {uf or '—'}  |  Município: {municipio or '—'}")
        print(f"{'='*60}")

        todos = []
        
        # ─── Fontes que não precisam de Selenium (Paralelas) ───
        tarefas_requests = []
        
        if usar_cnpj_biz and nicho and localizacao:
            tarefas_requests.append((self.buscar_cnpj_biz, (nicho, localizacao, max_por_fonte)))
        
        if usar_encontrei and nicho and localizacao:
            tarefas_requests.append((self.buscar_encontrei, (nicho, localizacao, max_por_fonte)))
            
        if usar_olx and nicho:
            tarefas_requests.append((self.buscar_olx, (nicho, localizacao, max_por_fonte)))
            
        if usar_mercado_livre and nicho:
            tarefas_requests.append((self.buscar_mercado_livre, (nicho, localizacao, max_por_fonte)))
            
        if usar_cnae_search and cnae:
            uf_search = uf or (localizacao[-2:].upper() if localizacao else '')
            tarefas_requests.append((self.buscar_por_cnae, (cnae, uf_search, municipio, max_por_fonte)))
            
        if usar_yelp and nicho and localizacao:
            tarefas_requests.append((self.buscar_yelp, (nicho, localizacao, max_por_fonte)))

        if tarefas_requests:
            print(f"\n🚀 Iniciando {len(tarefas_requests)} fontes em paralelo...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(tarefas_requests)) as executor:
                futuros = [executor.submit(func, *args) for func, args in tarefas_requests]
                for f in concurrent.futures.as_completed(futuros):
                    try:
                        todos.extend(f.result())
                    except Exception as e:
                        print(f"   ⚠️ Erro em fonte paralela: {e}")

        # ─── Fontes que precisam de Selenium (Sequenciais para evitar conflito de driver) ───
        try:
            if usar_google_maps and nicho and localizacao:
                todos.extend(self.buscar_google_maps(nicho, localizacao, max_por_fonte))

            if usar_jucesp:
                todos.extend(self.buscar_jucesp(nicho, max_por_fonte))

            if usar_linkedin and nicho:
                todos.extend(self.buscar_linkedin(nicho, localizacao, max_por_fonte))
        finally:
            self._fechar_driver()

        # ── Ramdomização dos resultados ──────────────────────
        if todos:
            print(f"\n🎲 Randomizando {len(todos)} leads encontrados...")
            random.shuffle(todos)

        # ── Enriquecimento CNPJ via BrasilAPI (Concorrente) ──
        if enriquecer_cnpj:
            leads_com_cnpj = [l for l in todos if l.get('cnpj')]
            print(f"\n📡 Enriquecendo {len(leads_com_cnpj)} leads com CNPJ via BrasilAPI...")
            
            def worker_cnpj(lead):
                try:
                    data = enriquecer_cnpj(lead['cnpj'])
                    if data:
                        _merge_cnpj_data(lead, data)
                except Exception:
                    pass
                
                
            # Limitado a 5 workers para não engasgar a BrasilAPI (+Sleep de 0.2s)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                list(executor.map(worker_cnpj, leads_com_cnpj))

        # ── Verificação de site e Extração de Emails em Lote ──
        print(f"\n🌐 Checando {len(todos)} leads para verificação de site...")
        def check_site(l):
            if l.get('site') and not l.get('tem_site'):
                l['tem_site'] = verificar_site(l['site'])
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(check_site, todos))
            
        leads_para_extrair = [l for l in todos if l.get('site') and l.get('tem_site') and not l.get('email')]
        if leads_para_extrair:
            # Limite para não travar a requisição síncrona
            leads_para_extrair = leads_para_extrair[:8] 
            print(f"\n📧 Extraindo emails em lote de {len(leads_para_extrair)} sites (limite síncrono)...")
            extrator = EmailExtractor()
            urls_para_extrair = [l['site'] for l in leads_para_extrair]
            
            # Timeout rígido de 25 segundos para a extração total
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    futuros = {executor.submit(extrator.extrair_emails_site, url): url for url in urls_para_extrair}
                    done, not_done = concurrent.futures.wait(futuros, timeout=25)
                    
                    for f in done:
                        res = f.result()
                        # Procura o lead correspondente
                        for lead in leads_para_extrair:
                            if lead['site'] == res['url'] and res['sucesso'] and res['emails']:
                                lead['email'] = res['emails'][0]
                                break
                    
                    # Cancela os que não terminaram
                    for f in not_done:
                        f.cancel()
            except Exception as e:
                print(f"⚠️ Erro parcial na extração de emails: {e}")

        # ── Deduplicação ──────────────────────────────────────
        seen_email = set()
        seen_company = set()
        unicos = []
        for lead in todos:
            ek = lead['email'].lower().strip() if lead['email'] else None
            ck = lead['empresa'].lower().strip() if lead['empresa'] else None
            # Prioriza quem tem email
            if ek and ek in seen_email:
                continue
            if not ek and ck and ck in seen_company:
                continue
            if ek:
                seen_email.add(ek)
            if ck:
                seen_company.add(ck)
            unicos.append(lead)

        # ── Filtros ───────────────────────────────────────────
        filtros_final = filtros or {}
        # Injeta filtros de UF/municipio se fornecidos
        if uf and 'uf' not in filtros_final:
            filtros_final['uf'] = uf
        if municipio and 'municipio' not in filtros_final:
            filtros_final['municipio'] = municipio
        if cnae and 'cnae' not in filtros_final:
            filtros_final['cnae'] = cnae

        filtrados = aplicar_filtros(unicos, filtros_final) if filtros_final else unicos

        com_email = [l for l in filtrados if l['email']]
        sem_email = [l for l in filtrados if not l['email']]

        print(f"\n{'='*60}")
        print(f"📊 RESULTADO: {len(filtrados)} leads | {len(com_email)} com email")
        print(f"{'='*60}")

        return {
            'leads': filtrados,
            'total': len(filtrados),
            'com_email': len(com_email),
            'sem_email': len(sem_email),
            'nicho': nicho,
            'localizacao': localizacao,
            'cargo': cargo,
            'cnae': cnae,
            'filtros_aplicados': filtros_final,
            'timestamp': datetime.now().isoformat(),
        }
