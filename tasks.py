from celery_app import celery
from lead_scraper import LeadScraper
from script import EmailExtractor
import json
import os
from datetime import datetime

RESULTS_DIR = 'resultados'

@celery.task(bind=True)
def task_buscar_leads(self, params):
    """
    Tarefa em background para buscar leads.
    """
    try:
        self.update_state(state='PROGRESS', meta={'status': 'Iniciando busca...'})
        
        headless = params.get('headless', True)
        browser = params.get('browser', 'edge')
        
        scraper = LeadScraper(headless=headless, browser=browser)
        
        # Injetar função de progresso customizada se necessário (o scraper atual não suporta callbacks de progresso facilmente)
        # Mas podemos atualizar o estado do celery entre fontes de busca
        
        resultado = scraper.buscar_leads(
            nicho=params.get('nicho', ''),
            localizacao=params.get('localizacao', ''),
            cargo=params.get('cargo', ''),
            cnae=params.get('cnae', ''),
            uf=params.get('uf', ''),
            municipio=params.get('municipio', ''),
            usar_google_maps=params.get('usar_google_maps', True),
            usar_cnpj_biz=params.get('usar_cnpj_biz', True),
            usar_encontrei=params.get('usar_encontrei', True),
            usar_olx=params.get('usar_olx', False),
            usar_mercado_livre=params.get('usar_mercado_livre', False),
            usar_jucesp=params.get('usar_jucesp', False),
            usar_cnae_search=params.get('usar_cnae_search', False),
            usar_yelp=params.get('usar_yelp', False), # Nova fonte
            usar_linkedin=params.get('usar_linkedin', False), # Nova fonte
            enriquecer_cnpj=params.get('enriquecer_cnpj', True),
            max_por_fonte=params.get('max_por_fonte', 15),
            filtros=params.get('filtros'),
        )
        
        # Salva resultado em arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"leads_{timestamp}.json"
        filepath = os.path.join(RESULTS_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, indent=2, ensure_ascii=False)
            
        resultado['arquivo_salvo'] = filename
        return resultado

    except Exception as e:
        self.update_state(state='FAILURE', meta={'erro': str(e)})
        raise e

@celery.task(bind=True)
def task_extrair_emails(self, params):
    """
    Tarefa em background para extrair emails de URLs.
    """
    try:
        self.update_state(state='PROGRESS', meta={'status': 'Iniciando extração...'})
        
        urls = params.get('urls', [])
        tentar_paginas_contato = params.get('tentar_paginas_contato', True)
        headless = params.get('headless', True)
        
        extrator = EmailExtractor(headless=headless, browser='edge')
        
        resultados = extrator.extrair_emails_multiplos_sites(
            urls=urls,
            tentar_paginas_contato=tentar_paginas_contato,
            salvar_json=False
        )
        
        # Coleta todos os emails únicos
        emails_unicos = set()
        for res in resultados:
            if res['sucesso'] and res['emails']:
                emails_unicos.update(res['emails'])
                
        resposta = {
            'resultados': resultados,
            'emails_unicos': sorted(list(emails_unicos)),
            'timestamp': datetime.now().isoformat(),
            'total_sites': len(urls),
            'sites_sucesso': sum(1 for r in resultados if r['sucesso']),
            'total_emails': len(emails_unicos)
        }
        
        # Salva resultado
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"emails_extraidos_{timestamp}.json"
        filepath = os.path.join(RESULTS_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(resposta, f, indent=2, ensure_ascii=False)
            
        resposta['arquivo_salvo'] = filename
        return resposta

    except Exception as e:
        self.update_state(state='FAILURE', meta={'erro': str(e)})
        raise e
