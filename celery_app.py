from celery import Celery
import os

def make_celery(app_name=__name__):
    # Procura pela URL do Redis em variáveis comuns (Render, Heroku, etc)
    redis_url = os.environ.get('REDIS_URL') or os.environ.get('REDIS_TLS_URL')
    
    if not redis_url:
        print("⚠️ AVISO: REDIS_URL não encontrada. Usando localhost (Modo Desenvolvimento).")
        redis_url = 'redis://localhost:6379/0'
    else:
        # Garante que a URL não termine com espaços ou caracteres invisíveis
        redis_url = redis_url.strip()
        print(f"✅ Conectando ao Redis: {redis_url[:15]}...") # Log parcial por segurança
    
    celery = Celery(
        app_name,
        broker=redis_url,
        backend=redis_url,
        include=['tasks']
    )
    
    celery.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        task_track_started=True,
        task_time_limit=3600,  # 1 hour limit
    )
    
    return celery

celery = make_celery()
