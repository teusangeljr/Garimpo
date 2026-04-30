from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from script import EmailExtractor
from email_sender import EmailSender
from lead_scraper import LeadScraper
import json
from datetime import datetime
import os
import threading
import time
import requests
from werkzeug.utils import secure_filename
# from celery_app import celery
# import tasks

app = Flask(__name__)
CORS(app)

# Diretório para armazenar resultados temporários
RESULTS_DIR = 'resultados'
UPLOADS_DIR = 'uploads'

for directory in [RESULTS_DIR, UPLOADS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

@app.route('/')
def index():
    """API status endpoint"""
    return jsonify({
        'status': 'Garimpo API is running',
        'endpoints': ['/processar', '/upload-anexo', '/enviar-emails', '/buscar-leads', '/download', '/health', '/api/job/<job_id>']
    })

@app.route('/processar', methods=['POST'])
def processar():
    """
    Endpoint assíncrono para processar URLs e extrair emails
    """
    try:
        data = request.get_json()
        if not data or 'urls' not in data:
            return jsonify({'erro': 'URLs não fornecidas'}), 400
        
        urls = data.get('urls', [])
        tentar_paginas_contato = data.get('tentar_paginas_contato', True)
        
        urls_finais = []
        maps_urls = []
        
        for u in urls:
            if 'google.com/maps' in u.lower():
                maps_urls.append(u)
            else:
                urls_finais.append(u)
                
        if maps_urls:
            print(f"Detectadas {len(maps_urls)} URLs do Google Maps. Extraindo sites...")
            scraper = LeadScraper(headless=True)
            for m_url in maps_urls:
                sites_extraidos = scraper.buscar_sites_de_url_google_maps(m_url)
                urls_finais.extend(sites_extraidos)
            
            # Remove duplicatas preservando os novos sites
            urls_finais = list(set(urls_finais))
            print(f"Extração do Maps concluída. Total de URLs a processar: {len(urls_finais)}")
            
        if not urls_finais:
             return jsonify({'erro': 'Nenhuma URL de site (com domínio válido) pôde ser extraída.'}), 400
        
        extrator = EmailExtractor()
        resultados = extrator.extrair_emails_multiplos_sites(
            urls=urls_finais,
            tentar_paginas_contato=tentar_paginas_contato,
            salvar_json=False
        )
        
        # Coleta todos os emails e whatsapps únicos
        emails_unicos = set()
        whatsapp_unicos = set()
        
        # Sanitizar resultados para garantir que não haja sets
        for res in resultados:
            if 'emails' in res and isinstance(res['emails'], set):
                res['emails'] = list(res['emails'])
            if 'whatsapp' in res and isinstance(res['whatsapp'], set):
                res['whatsapp'] = list(res['whatsapp'])
                
            if res.get('sucesso'):
                if res.get('emails'): emails_unicos.update(res['emails'])
                if res.get('whatsapp'): whatsapp_unicos.update(res['whatsapp'])
                
        # Função auxiliar para remover todos os sets recursivamente
        def convert_sets_to_lists(obj):
            if isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, dict):
                return {k: convert_sets_to_lists(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_sets_to_lists(v) for v in obj]
            return obj


        resposta = convert_sets_to_lists({
            'resultados': resultados,
            'emails_unicos': sorted(list(emails_unicos)),
            'whatsapp_unicos': sorted(list(whatsapp_unicos)),
            'timestamp': datetime.now().isoformat(),
            'total_sites': len(urls_finais),
            'total_emails': len(emails_unicos),
            'total_whatsapp': len(whatsapp_unicos)
        })
        
        return jsonify(resposta)
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Erro na extração: {e}")
        return jsonify({'erro': str(e)}), 500

@app.route('/upload-anexo', methods=['POST'])
def upload_anexo():
    """Endpoint para upload de arquivos anexos"""
    try:
        if 'arquivo' not in request.files:
            return jsonify({'erro': 'Nenhum arquivo enviado'}), 400
            
        arquivo = request.files['arquivo']
        if arquivo.filename == '':
            return jsonify({'erro': 'Nome de arquivo vazio'}), 400
            
        filename = secure_filename(arquivo.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{filename}"
        filepath = os.path.join(UPLOADS_DIR, filename)
        
        arquivo.save(filepath)
        
        return jsonify({
            'mensagem': 'Arquivo enviado com sucesso',
            'caminho': filepath,
            'nome_original': arquivo.filename
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/enviar-emails', methods=['POST'])
def enviar_emails():
    """Endpoint para envio de emails"""
    try:
        data = request.get_json()
        
        # Validação básica
        required_fields = ['email_remetente', 'senha_app', 'destinatarios', 'assunto_padrao', 'corpo_padrao']
        for field in required_fields:
            if field not in data:
                return jsonify({'erro': f'Campo obrigatório faltando: {field}'}), 400
                
        email_sender = EmailSender(data['email_remetente'], data['senha_app'])
        
        resultados = email_sender.enviar_lote(
            lista_destinatarios=data['destinatarios'],
            assunto_padrao=data['assunto_padrao'],
            corpo_padrao=data['corpo_padrao'],
            caminho_anexo=data.get('caminho_anexo')
        )
        
        return jsonify({
            'mensagem': 'Processo de envio finalizado',
            'resultados': resultados,
            'total_enviados': sum(1 for r in resultados if r['sucesso']),
            'total_falhas': sum(1 for r in resultados if not r['sucesso'])
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/buscar-leads', methods=['POST'])
def buscar_leads():
    """Endpoint assíncrono para busca de leads"""
    try:
        data = request.get_json()
        if not data.get('nicho') and not data.get('cnae') and not data.get('localizacao') and not data.get('uf'):
             return jsonify({'erro': 'Parâmetros de busca insuficientes'}), 400

        scraper = LeadScraper(
            headless=data.get('headless', True),
            browser=data.get('browser', 'edge')
        )
        
        resultado = scraper.buscar_leads(
            nicho=data.get('nicho', ''),
            localizacao=data.get('localizacao', ''),
            cargo=data.get('cargo', ''),
            cnae=data.get('cnae', ''),
            uf=data.get('uf', ''),
            municipio=data.get('municipio', ''),
            usar_google_maps=data.get('usar_google_maps', True),
            usar_cnpj_biz=data.get('usar_cnpj_biz', True),
            usar_encontrei=data.get('usar_encontrei', True),
            usar_olx=data.get('usar_olx', False),
            usar_mercado_livre=data.get('usar_mercado_livre', False),
            usar_jucesp=data.get('usar_jucesp', False),
            usar_cnae_search=data.get('usar_cnae_search', False),
            usar_yelp=data.get('usar_yelp', False),
            usar_linkedin=data.get('usar_linkedin', False),
            enriquecer_cnpj=data.get('enriquecer_cnpj', True),
            max_por_fonte=data.get('max_por_fonte', 15),
            filtros=data.get('filtros'),
        )
        
        return jsonify(resultado)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Erro na busca de leads: {e}")
        return jsonify({'erro': str(e)}), 500

@app.route('/api/job/<job_id>')
def get_job_status(job_id):
    """Consulta o status de um job no Celery"""
    from celery.result import AsyncResult
    job = AsyncResult(job_id, app=celery)

    response = {
        'job_id': job_id,
        'state': job.state,
        'status': job.info.get('status', '') if isinstance(job.info, dict) else '',
        'error': job.info.get('erro', '') if job.state == 'FAILURE' and isinstance(job.info, dict) else ''
    }
    
    if job.state == 'SUCCESS':
        response['result'] = job.result
    
    return jsonify(response)

@app.route('/api/job/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Cancela uma tarefa no Celery"""
    try:
        from celery.result import AsyncResult
        # Revoga a tarefa. terminate=True envia SIGTERM para o processo worker.
        celery.control.revoke(job_id, terminate=True)
        return jsonify({'status': 'cancelled', 'job_id': job_id})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/download/<filename>')
def download(filename):
    """
    Endpoint para download de arquivo de resultados
    """
    try:
        filepath = os.path.join(RESULTS_DIR, filename)
        if os.path.exists(filepath):
            return send_file(filepath, as_attachment=True)
        else:
            return jsonify({'erro': 'Arquivo não encontrado'}), 404
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

@app.route('/ping')
def ping():
    """Lightweight ping endpoint for monitoring"""
    return 'pong', 200

def keep_awake():
    """Thread function to ping self and keep the app from sleeping on Render"""
    url = os.environ.get('RENDER_EXTERNAL_URL')
    if not url:
        print("Keep-awake: RENDER_EXTERNAL_URL não configurada. Ignorando...")
        return
        
    print(f"Keep-awake: Iniciando pinger para {url}")
    while True:
        try:
            # Espera 10 minutos (Render dorme em 15)
            time.sleep(600)
            requests.get(f"{url}/ping")
            print(f"Keep-awake: Ping enviado às {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Keep-awake: Erro no ping: {e}")

if __name__ == '__main__':
    # Inicia thread de keep-awake apenas em produção (Render)
    if os.environ.get('RENDER_EXTERNAL_URL'):
        threading.Thread(target=keep_awake, daemon=True).start()
    
    port = int(os.environ.get("PORT", 5000))
    print(f"Servidor Flask iniciado na porta {port}!")
    app.run(debug=False, host='0.0.0.0', port=port)
