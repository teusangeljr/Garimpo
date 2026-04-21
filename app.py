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
import uuid
import requests
from werkzeug.utils import secure_filename

app = Flask(__name__)
CORS(app)

# ─── Diretórios ───────────────────────────────────────────────
RESULTS_DIR = 'resultados'
UPLOADS_DIR = 'uploads'

for directory in [RESULTS_DIR, UPLOADS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# ─── Job Store em Memória (substitui Redis/Celery) ────────────
# Estrutura: { job_id: { 'state': str, 'status': str, 'result': dict|None, 'error': str } }
_jobs: dict = {}
_jobs_lock = threading.Lock()

def _set_job(job_id: str, state: str, status: str = '', result=None, error: str = ''):
    with _jobs_lock:
        _jobs[job_id] = {
            'state': state,
            'status': status,
            'result': result,
            'error': error,
        }

def _create_job() -> str:
    job_id = str(uuid.uuid4())
    _set_job(job_id, 'PENDING', 'Aguardando início...')
    return job_id

# ─── Workers em Thread ────────────────────────────────────────

def _run_buscar_leads(job_id: str, params: dict):
    """Executa a busca de leads em uma thread de background."""
    try:
        _set_job(job_id, 'PROGRESS', 'Iniciando busca de leads...')
        print(f"[Job {job_id[:8]}] Iniciando busca: {params.get('nicho')} em {params.get('localizacao')}")

        scraper = LeadScraper(
            headless=params.get('headless', True),
            browser=params.get('browser', 'chrome'),
        )

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
            usar_yelp=params.get('usar_yelp', False),
            usar_linkedin=params.get('usar_linkedin', False),
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
        _set_job(job_id, 'SUCCESS', 'Concluído!', result=resultado)
        print(f"[Job {job_id[:8]}] ✅ Sucesso — {resultado.get('total', 0)} leads encontrados")

    except Exception as e:
        err_msg = str(e)
        print(f"[Job {job_id[:8]}] ❌ Erro: {err_msg}")
        _set_job(job_id, 'FAILURE', 'Erro durante a busca.', error=err_msg)


def _run_extrair_emails(job_id: str, params: dict):
    """Executa a extração de emails em uma thread de background."""
    try:
        _set_job(job_id, 'PROGRESS', 'Iniciando extração de emails...')
        urls = params.get('urls', [])
        print(f"[Job {job_id[:8]}] Extraindo emails de {len(urls)} URLs")

        extrator = EmailExtractor(
            headless=params.get('headless', True),
            browser='chrome',
        )

        resultados = extrator.extrair_emails_multiplos_sites(
            urls=urls,
            tentar_paginas_contato=params.get('tentar_paginas_contato', True),
            salvar_json=False,
        )

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
            'total_emails': len(emails_unicos),
        }

        # Salva resultado em arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"emails_extraidos_{timestamp}.json"
        filepath = os.path.join(RESULTS_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(resposta, f, indent=2, ensure_ascii=False)

        resposta['arquivo_salvo'] = filename
        _set_job(job_id, 'SUCCESS', 'Concluído!', result=resposta)
        print(f"[Job {job_id[:8]}] ✅ Sucesso — {len(emails_unicos)} emails encontrados")

    except Exception as e:
        err_msg = str(e)
        print(f"[Job {job_id[:8]}] ❌ Erro: {err_msg}")
        _set_job(job_id, 'FAILURE', 'Erro durante a extração.', error=err_msg)


# ─── Endpoints ────────────────────────────────────────────────

@app.route('/')
def index():
    """API status endpoint"""
    return jsonify({
        'status': 'Garimpo API is running',
        'broker': 'threading (sem Redis)',
        'endpoints': ['/processar', '/upload-anexo', '/enviar-emails', '/buscar-leads', '/download', '/health', '/api/job/<job_id>']
    })


@app.route('/processar', methods=['POST'])
def processar():
    """Endpoint assíncrono para processar URLs e extrair emails"""
    try:
        data = request.get_json()
        if not data or 'urls' not in data:
            return jsonify({'erro': 'URLs não fornecidas'}), 400

        job_id = _create_job()
        t = threading.Thread(target=_run_extrair_emails, args=(job_id, data), daemon=True)
        t.start()
        return jsonify({'job_id': job_id, 'status': 'queued'})

    except Exception as e:
        print(f"Erro ao disparar extração: {e}")
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

        job_id = _create_job()
        t = threading.Thread(target=_run_buscar_leads, args=(job_id, data), daemon=True)
        t.start()
        return jsonify({'job_id': job_id, 'status': 'queued'})

    except Exception as e:
        print(f"Erro ao disparar busca de leads: {e}")
        return jsonify({'erro': str(e)}), 500


@app.route('/api/job/<job_id>')
def get_job_status(job_id):
    """Consulta o status de um job (no job store em memória)"""
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        return jsonify({'job_id': job_id, 'state': 'NOT_FOUND', 'status': 'Job não encontrado.'}), 404

    response = {
        'job_id': job_id,
        'state': job['state'],
        'status': job['status'],
        'error': job.get('error', ''),
    }
    if job['state'] == 'SUCCESS':
        response['result'] = job['result']

    return jsonify(response)


@app.route('/api/job/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Marca um job como cancelado (não interrompe thread em execução, mas para o polling)"""
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]['state'] = 'REVOKED'
            _jobs[job_id]['status'] = 'Cancelado pelo usuário.'
    return jsonify({'status': 'cancelled', 'job_id': job_id})


@app.route('/download/<filename>')
def download(filename):
    """Endpoint para download de arquivo de resultados"""
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
    with _jobs_lock:
        total_jobs = len(_jobs)
        active_jobs = sum(1 for j in _jobs.values() if j['state'] in ('PENDING', 'PROGRESS'))
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'jobs_total': total_jobs,
        'jobs_active': active_jobs,
    })


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
            time.sleep(600)
            requests.get(f"{url}/ping", timeout=10)
            print(f"Keep-awake: Ping enviado às {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Keep-awake: Erro no ping: {e}")


# ─── Startup ──────────────────────────────────────────────────

def _startup():
    print("=" * 40)
    print(" Garimpo API iniciando...")
    print(f" Broker: Threading nativo (sem Redis)")
    print(f" Timestamp: {datetime.now().isoformat()}")
    print("=" * 40)

    if os.environ.get('RENDER_EXTERNAL_URL'):
        threading.Thread(target=keep_awake, daemon=True).start()
        print("Keep-awake: thread iniciada.")


_startup()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    print(f"Servidor Flask iniciado na porta {port}!")
    app.run(debug=False, host='0.0.0.0', port=port)
