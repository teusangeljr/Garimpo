import requests
from bs4 import BeautifulSoup
import re
import time
from typing import List, Set, Dict, Optional
import json
from datetime import datetime
import concurrent.futures
from urllib.parse import urljoin, urlparse

# Configurações
EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}"

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'pt-BR,pt;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
}

class EmailExtractor:
    """Classe para extrair emails de múltiplos sites de forma concorrente e veloz."""
    
    def __init__(self, headless: bool = False, browser: str = 'edge'):
        """
        Inicializa o extrator de emails.
        Nota: headless e browser são mantidos por compatibilidade de assinatura,
        mas o extrator foi modernizado para usar rotinas HTTP velozes ao invés de Selenium.
        """
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
    def _iniciar_driver(self):
        pass # Deprecated in favor of requests Session
        
    def _fechar_driver(self):
        pass # Deprecated in favor of requests Session
    
    def _extrair_emails_do_texto(self, texto: str) -> Set[str]:
        """Extrai emails de um texto usando regex"""
        emails = re.findall(EMAIL_REGEX, texto)
        # Filtra emails comuns de placeholder/exemplo e imagens incorretas (ex: email@2x.png)
        emails_validos = set()
        for email in emails:
            email_lower = email.lower()
            if any(email_lower.endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                continue
            if not any(placeholder in email_lower for placeholder in 
                      ['example.com', 'teste.com', 'test.com', 'domain.com', 'email.com', 'sentry.io', 'wixpress.com']):
                emails_validos.add(email_lower)
        return emails_validos
    
    def _buscar_paginas_contato(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Busca URLs prováveis de contato ou trabalhe conosco no HTML."""
        links_potenciais = []
        palavras_chave = ['contato', 'contact', 'fale-conosco', 'faleconosco', 'trabalhe', 'careers']
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            texto = a.get_text().lower()
            
            # Se for um mailto, ignora aqui (já é pego no fluxo principal)
            if href.startswith('mailto:'):
                continue
                
            match_texto = any(p in texto for p in palavras_chave)
            match_href = any(p in href.lower() for p in palavras_chave)
            
            if match_texto or match_href:
                # Constrói URL completa
                url_completa = urljoin(base_url, href)
                # Garante que pertence ao mesmo domínio
                if urlparse(url_completa).netloc == urlparse(base_url).netloc:
                    if url_completa not in links_potenciais:
                        links_potenciais.append(url_completa)
                        
        return links_potenciais[:3] # Limita a 3 variações para não gastar muito tempo

    def extrair_emails_site(self, url: str, tentar_paginas_contato: bool = True) -> Dict:
        """Extrai emails de um site específico usando requests."""
        resultado = {
            'url': url,
            'emails': set(),
            'sucesso': False,
            'erro': None,
            'timestamp': datetime.now().isoformat()
        }
        
        if not url.startswith('http'):
            url = 'http://' + url
            
        print(f"🔍 Proc: {url}")
        
        try:
            response = self.session.get(url, timeout=12, allow_redirects=True)
            response.raise_for_status()
            
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            
            # 1. Tenta extrair da página principal
            emails = self._extrair_emails_do_texto(html)
            
            # Pega mailtos explicitamente
            links_mailto = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
            for link in links_mailto:
                email_match = re.search(EMAIL_REGEX, link.get('href', ''))
                if email_match:
                    emails.add(email_match.group().lower())
                    
            resultado['emails'].update(emails)
            
            # 2. Se não encontrou, navega nas sub-páginas de contato
            if tentar_paginas_contato and not emails:
                urls_contato = self._buscar_paginas_contato(soup, response.url)
                for pag in urls_contato:
                    if pag == response.url: continue
                    try:
                        r_contato = self.session.get(pag, timeout=10)
                        html_contato = r_contato.text
                        soup_contato = BeautifulSoup(html_contato, 'html.parser')
                        
                        emails_contato = self._extrair_emails_do_texto(html_contato)
                        for lm in soup_contato.find_all('a', href=re.compile(r'^mailto:', re.I)):
                            em = re.search(EMAIL_REGEX, lm.get('href', ''))
                            if em: emails_contato.add(em.group().lower())
                            
                        if emails_contato:
                            resultado['emails'].update(emails_contato)
                            break # Achou, não precisa olhar outras páginas de contato
                    except Exception:
                        continue
                        
            resultado['sucesso'] = True
            resultado['emails'] = list(resultado['emails'])
            
            if resultado['emails']:
                print(f"✅ {url} ({len(resultado['emails'])})")
            
        except Exception as e:
            res_erro = str(e)[:100]
            resultado['erro'] = res_erro
            print(f"❌ {url} - {res_erro}")
            
        return resultado
    
    def extrair_emails_multiplos_sites(self, urls: List[str], 
                                       tentar_paginas_contato: bool = True,
                                       salvar_json: bool = True) -> List[Dict]:
        """Processamento concorrente para extrair de vários sites."""
        resultados = []
        
        print(f"\n🚀 Extração Rápida de emails ({len(urls)} sites)")
        print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Usa ThreadPoolExecutor para processamento em paralelo
        max_workers = min(15, max(3, len(urls)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {
                executor.submit(self.extrair_emails_site, url, tentar_paginas_contato): url 
                for url in urls
            }
            
            for index, futuro in enumerate(concurrent.futures.as_completed(futuros)):
                resultado = futuro.result()
                resultados.append(resultado)
        
        # Resumo
        total_emails = sum(len(r['emails']) for r in resultados)
        sites_sucesso = sum(1 for r in resultados if r['sucesso'])
        print(f"\n📊 RESUMO:")
        print(f"Sites sucesso/total: {sites_sucesso}/{len(urls)}")
        print(f"Total leads emails: {total_emails}")
        
        if salvar_json:
            nome_arquivo = f"emails_extraidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(resultados, f, indent=2, ensure_ascii=False)
            print(f"💾 {nome_arquivo}")
        
        return resultados

def main():
    sites = [
        'https://google.com',
        'https://microsoft.com',
    ]
    extrator = EmailExtractor()
    extrator.extrair_emails_multiplos_sites(sites, True, False)

if __name__ == "__main__":
    main()