from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import time
from typing import List, Set, Dict, Optional
import json
from datetime import datetime
import sys

# Configurações
EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

class EmailExtractor:
    """Classe para extrair emails de múltiplos sites"""
    
    def __init__(self, headless: bool = False, browser: str = 'edge'):
        """
        Inicializa o extrator de emails
        
        Args:
            headless: Se True, executa sem abrir janela do navegador
            browser: Navegador a usar ('edge', 'chrome', 'firefox')
        """
        self.headless = headless
        self.browser = browser.lower()
        self.driver = None
        
    def _iniciar_driver(self):
        """Inicializa o driver do navegador (usa Selenium Manager automaticamente)"""
        
        # Configura opções baseado no navegador escolhido
        if sys.platform.startswith('linux'):
            self.browser = 'chrome'
            options = webdriver.ChromeOptions()
            options.binary_location = '/opt/render/project/src/.chrome/opt/google/chrome/google-chrome'
        elif self.browser == 'chrome':
            options = webdriver.ChromeOptions()
        elif self.browser == 'firefox':
            options = webdriver.FirefoxOptions()
        else:  # edge (padrão)
            options = webdriver.EdgeOptions()
            options.use_chromium = True
        
        if self.headless:
            options.add_argument("--headless=new")
        
        # Opções adicionais para melhor performance e evitar detecção
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        
        # Selenium Manager baixa o driver automaticamente
        if self.browser == 'chrome':
            self.driver = webdriver.Chrome(options=options)
        elif self.browser == 'firefox':
            self.driver = webdriver.Firefox(options=options)
        else:
            self.driver = webdriver.Edge(options=options)
            
        self.driver.set_page_load_timeout(30)
        
    def _fechar_driver(self):
        """Fecha o driver do navegador"""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def _extrair_emails_do_texto(self, texto: str) -> Set[str]:
        """
        Extrai emails de um texto usando regex
        
        Args:
            texto: Texto para buscar emails
            
        Returns:
            Set com emails encontrados
        """
        emails = re.findall(EMAIL_REGEX, texto)
        # Filtra emails comuns de placeholder/exemplo
        emails_validos = {
            email.lower() for email in emails 
            if not any(placeholder in email.lower() for placeholder in 
                      ['example.com', 'teste.com', 'test.com', 'domain.com', 'email.com'])
        }
        return emails_validos
    
    def _tentar_clicar_botoes_contato(self):
        """
        Tenta encontrar e clicar em botões comuns de contato/revelar email
        """
        # Seletores comuns de botões de contato
        seletores_botoes = [
            "button[class*='contato']",
            "button[class*='contact']",
            "a[class*='contato']",
            "a[class*='contact']",
            "button[id*='contato']",
            "button[id*='contact']",
            "[class*='show-email']",
            "[class*='reveal-email']",
            "[class*='mostrar-email']",
        ]
        
        for seletor in seletores_botoes:
            try:
                elementos = self.driver.find_elements(By.CSS_SELECTOR, seletor)
                for elemento in elementos:
                    try:
                        if elemento.is_displayed() and elemento.is_enabled():
                            elemento.click()
                            time.sleep(1)  # Aguarda o conteúdo carregar
                    except:
                        continue
            except:
                continue
    
    def _encontrar_link_contato(self) -> Optional[str]:
        """
        Tenta encontrar link de página de contato
        
        Returns:
            URL da página de contato ou None
        """
        links_contato = [
            # Contato em português
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contato')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'fale conosco')]",
            "//a[contains(@href, 'contato')]",
            "//a[contains(@href, 'fale-conosco')]",
            
            # Contact em inglês
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact us')]",
            "//a[contains(@href, 'contact')]",
        ]
        
        for xpath in links_contato:
            try:
                elementos = self.driver.find_elements(By.XPATH, xpath)
                for elemento in elementos[:1]:  # Pega apenas o primeiro
                    try:
                        href = elemento.get_attribute('href')
                        if href and href != self.driver.current_url:
                            return href
                    except:
                        continue
            except:
                continue
        
        return None
    
    def _encontrar_link_trabalhe_conosco(self) -> Optional[str]:
        """
        Tenta encontrar link de página de trabalhe conosco/careers
        
        Returns:
            URL da página de trabalhe conosco ou None
        """
        links_trabalhe = [
            # Trabalhe Conosco em português
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'trabalhe conosco')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'trabalhe')]",
            "//a[contains(@href, 'trabalhe')]",
            "//a[contains(@href, 'trabalhe-conosco')]",
            
            # Careers em inglês
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'careers')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'jobs')]",
            "//a[contains(@href, 'careers')]",
            "//a[contains(@href, 'jobs')]",
        ]
        
        for xpath in links_trabalhe:
            try:
                elementos = self.driver.find_elements(By.XPATH, xpath)
                for elemento in elementos[:1]:  # Pega apenas o primeiro
                    try:
                        href = elemento.get_attribute('href')
                        if href and href != self.driver.current_url:
                            return href
                    except:
                        continue
            except:
                continue
        
        return None
    
    def extrair_emails_site(self, url: str, tentar_paginas_contato: bool = True) -> Dict:
        """
        Extrai emails de um site específico
        
        LÓGICA SEQUENCIAL:
        1. Procura na página principal
        2. Se não encontrar -> vai para "Contato"
        3. Se ainda não encontrar -> volta para principal e vai para "Trabalhe Conosco"
        4. Se não encontrar em nenhum lugar -> tudo bem, retorna vazio
        
        Args:
            url: URL do site
            tentar_paginas_contato: Se True, tenta navegar para páginas de contato
            
        Returns:
            Dicionário com informações da extração
        """
        resultado = {
            'url': url,
            'emails': set(),
            'sucesso': False,
            'erro': None,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"\n{'='*60}")
        print(f"🔍 Processando: {url}")
        print(f"{'='*60}")
        
        try:
            # Navega para o site
            self.driver.get(url)
            url_principal = self.driver.current_url
            time.sleep(3)  # Aguarda carregamento inicial
            
            # 1. Extrai emails da página principal
            html_inicial = self.driver.page_source
            soup = BeautifulSoup(html_inicial, 'html.parser')
            texto_pagina = soup.get_text()
            emails_encontrados = self._extrair_emails_do_texto(texto_pagina)
            resultado['emails'].update(emails_encontrados)
            
            print(f"📧 Emails na página principal: {len(emails_encontrados)}")
            
            # 2. Tenta clicar em botões de contato
            self._tentar_clicar_botoes_contato()
            html_apos_cliques = self.driver.page_source
            soup_apos = BeautifulSoup(html_apos_cliques, 'html.parser')
            emails_apos_cliques = self._extrair_emails_do_texto(soup_apos.get_text())
            novos_emails = emails_apos_cliques - resultado['emails']
            resultado['emails'].update(novos_emails)
            
            if novos_emails:
                print(f"📧 Emails após clicar botões: {len(novos_emails)}")
            
            # 3. BUSCA SEQUENCIAL: Contato -> Trabalhe Conosco (APENAS se não encontrou emails)
            if tentar_paginas_contato and len(resultado['emails']) == 0:
                print(f"⚠️  Nenhum email encontrado na página principal")
                print(f"🔍 Iniciando busca sequencial...")
                
                # PASSO 1: Tenta encontrar e acessar página de CONTATO
                link_contato = self._encontrar_link_contato()
                if link_contato:
                    print(f"📍 Encontrou link de Contato: {link_contato}")
                    try:
                        self.driver.get(link_contato)
                        time.sleep(2)
                        html_contato = self.driver.page_source
                        soup_contato = BeautifulSoup(html_contato, 'html.parser')
                        emails_contato = self._extrair_emails_do_texto(soup_contato.get_text())
                        
                        if emails_contato:
                            resultado['emails'].update(emails_contato)
                            print(f"✅ Encontrou {len(emails_contato)} email(s) na página de Contato!")
                            for email in emails_contato:
                                print(f"   - {email}")
                        else:
                            print(f"⚠️  Nenhum email encontrado na página de Contato")
                    except Exception as e:
                        print(f"❌ Erro ao acessar página de Contato: {str(e)[:50]}")
                else:
                    print(f"⚠️  Não encontrou link de Contato")
                
                # PASSO 2: Se AINDA não encontrou emails, volta para principal e tenta TRABALHE CONOSCO
                if len(resultado['emails']) == 0:
                    print(f"🔄 Voltando para página principal...")
                    try:
                        self.driver.get(url_principal)
                        time.sleep(2)
                    except:
                        pass
                    
                    link_trabalhe = self._encontrar_link_trabalhe_conosco()
                    if link_trabalhe:
                        print(f"📍 Encontrou link de Trabalhe Conosco: {link_trabalhe}")
                        try:
                            self.driver.get(link_trabalhe)
                            time.sleep(2)
                            html_trabalhe = self.driver.page_source
                            soup_trabalhe = BeautifulSoup(html_trabalhe, 'html.parser')
                            emails_trabalhe = self._extrair_emails_do_texto(soup_trabalhe.get_text())
                            
                            if emails_trabalhe:
                                resultado['emails'].update(emails_trabalhe)
                                print(f"✅ Encontrou {len(emails_trabalhe)} email(s) na página de Trabalhe Conosco!")
                                for email in emails_trabalhe:
                                    print(f"   - {email}")
                            else:
                                print(f"⚠️  Nenhum email encontrado na página de Trabalhe Conosco")
                        except Exception as e:
                            print(f"❌ Erro ao acessar página de Trabalhe Conosco: {str(e)[:50]}")
                    else:
                        print(f"⚠️  Não encontrou link de Trabalhe Conosco")
                    
                    # Se ainda não encontrou nada
                    if len(resultado['emails']) == 0:
                        print(f"⚠️  Nenhum email encontrado em nenhuma página. Tudo bem, seguindo em frente...")
            
            elif len(resultado['emails']) > 0:
                print(f"✅ Emails encontrados na página principal, pulando busca em outras páginas")
            
            # 4. Busca emails em atributos HTML (mailto:, data-email, etc)
            links_mailto = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
            for link in links_mailto:
                email_match = re.search(EMAIL_REGEX, link.get('href', ''))
                if email_match:
                    resultado['emails'].add(email_match.group().lower())
            
            resultado['sucesso'] = True
            resultado['emails'] = list(resultado['emails'])
            
            print(f"\n✅ Total de emails encontrados: {len(resultado['emails'])}")
            if resultado['emails']:
                for email in resultado['emails']:
                    print(f"   - {email}")
            
        except TimeoutException:
            erro = f"Timeout ao carregar o site (>30s)"
            resultado['erro'] = erro
            print(f"❌ Erro: {erro}")
        except WebDriverException as e:
            erro = f"Erro do WebDriver: {str(e)[:100]}"
            resultado['erro'] = erro
            print(f"❌ Erro: {erro}")
        except Exception as e:
            erro = f"Erro inesperado: {str(e)[:100]}"
            resultado['erro'] = erro
            print(f"❌ Erro: {erro}")
        
        return resultado
    
    def extrair_emails_multiplos_sites(self, urls: List[str], 
                                       tentar_paginas_contato: bool = True,
                                       salvar_json: bool = True) -> List[Dict]:
        """
        Extrai emails de múltiplos sites
        
        Args:
            urls: Lista de URLs para processar
            tentar_paginas_contato: Se True, tenta navegar para páginas de contato
            salvar_json: Se True, salva resultados em JSON
            
        Returns:
            Lista com resultados de cada site
        """
        resultados = []
        
        print(f"\n🚀 Iniciando extração de emails de {len(urls)} site(s)")
        print(f"⏰ Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        try:
            self._iniciar_driver()
            
            for i, url in enumerate(urls, 1):
                print(f"\n[{i}/{len(urls)}] Processando site...")
                resultado = self.extrair_emails_site(url, tentar_paginas_contato)
                resultados.append(resultado)
                
                # Pequena pausa entre sites para evitar sobrecarga
                if i < len(urls):
                    time.sleep(2)
            
        finally:
            self._fechar_driver()
        
        # Resumo final
        print(f"\n{'='*60}")
        print(f"📊 RESUMO FINAL")
        print(f"{'='*60}")
        total_emails = sum(len(r['emails']) for r in resultados)
        sites_sucesso = sum(1 for r in resultados if r['sucesso'])
        print(f"Sites processados: {len(urls)}")
        print(f"Sites com sucesso: {sites_sucesso}")
        print(f"Total de emails encontrados: {total_emails}")
        
        # Salva em JSON se solicitado
        if salvar_json:
            nome_arquivo = f"emails_extraidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(resultados, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Resultados salvos em: {nome_arquivo}")
        
        return resultados


def main():
    """Função principal - exemplo de uso"""
    
    # Lista de sites para extrair emails
    sites = [
        'https://www.exemplo.com.br',
        'https://www.outrosite.com',
        # Adicione mais sites aqui
    ]
    
    # Cria o extrator (não precisa mais do driver_path!)
    extrator = EmailExtractor(
        headless=False,  # Mude para True para executar sem abrir janela
        browser='edge'   # Opções: 'edge', 'chrome', 'firefox'
    )
    
    # Extrai emails de todos os sites
    resultados = extrator.extrair_emails_multiplos_sites(
        urls=sites,
        tentar_paginas_contato=True,  # Tenta navegar para páginas de contato
        salvar_json=True  # Salva resultados em JSON
    )
    
    # Você pode processar os resultados como quiser
    # Por exemplo, criar uma lista única de todos os emails:
    todos_emails = set()
    for resultado in resultados:
        if resultado['sucesso']:
            todos_emails.update(resultado['emails'])
    
    print(f"\n📧 Lista única de todos os emails encontrados:")
    for email in sorted(todos_emails):
        print(f"   - {email}")


if __name__ == "__main__":
    main()