import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
import time
from typing import List, Dict, Optional

class EmailSender:
    """Classe para envio de emails usando Gmail SMTP"""
    
    def __init__(self, email_remetente: str, senha_app: str):
        """
        Inicializa o enviador de emails
        
        Args:
            email_remetente: Email do Gmail
            senha_app: Senha de Aplicativo (App Password) do Gmail
        """
        self.email_remetente = email_remetente
        self.senha_app = senha_app
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        
    def enviar_email(self, 
                     destinatario: str, 
                     assunto: str, 
                     corpo: str, 
                     caminho_anexo: Optional[str] = None) -> Dict:
        """
        Envia um único email
        
        Args:
            destinatario: Email do destinatário
            assunto: Assunto do email
            corpo: Corpo do email (pode ser HTML)
            caminho_anexo: Caminho opcional para arquivo anexo
            
        Returns:
            Dict com status do envio
        """
        resultado = {
            'destinatario': destinatario,
            'sucesso': False,
            'erro': None
        }
        
        try:
            # Cria a mensagem
            msg = MIMEMultipart()
            msg['From'] = self.email_remetente
            msg['To'] = destinatario
            msg['Subject'] = assunto
            
            # Adiciona corpo
            msg.attach(MIMEText(corpo, 'plain'))
            
            
            # Adiciona anexo se houver
            if caminho_anexo and os.path.exists(caminho_anexo):
                with open(caminho_anexo, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(caminho_anexo))
                
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(caminho_anexo)}"'
                msg.attach(part)
            
            # Conecta e envia
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_remetente, self.senha_app)
            server.send_message(msg)
            server.quit()
            
            resultado['sucesso'] = True
            
        except smtplib.SMTPAuthenticationError as e:
            if e.smtp_code == 534:
                resultado['erro'] = "Erro de Autenticação: O Gmail exige uma Senha de Aplicativo. Ative a Verificação em 2 Etapas na sua conta Google e gere uma Senha de App."
            else:
                resultado['erro'] = f"Erro de Autenticação: Verifique seu email e senha. Detalhes: {e}"
        except Exception as e:
            resultado['erro'] = str(e)
            
        return resultado

    def enviar_lote(self, 
                    lista_destinatarios: List[Dict], 
                    assunto_padrao: str, 
                    corpo_padrao: str, 
                    caminho_anexo: Optional[str] = None) -> List[Dict]:
        """
        Envia emails em lote com suporte a personalização
        
        Args:
            lista_destinatarios: Lista de dicts com {'email': '...', 'assunto': '...', 'corpo': '...'}
                               Os campos assunto e corpo são opcionais na lista (usam padrão se não houver)
            assunto_padrao: Assunto usado se não houver personalizado
            corpo_padrao: Corpo usado se não houver personalizado
            caminho_anexo: Arquivo para anexar em todos os emails
            
        Returns:
            Lista de resultados
        """
        resultados = []
        
        for i, item in enumerate(lista_destinatarios):
            email = item.get('email')
            if not email:
                continue
                
            # Usa personalizado ou padrão
            assunto = item.get('assunto', assunto_padrao)
            corpo = item.get('corpo', corpo_padrao)
            
            print(f"Enviando para {email}...")
            resultado = self.enviar_email(email, assunto, corpo, caminho_anexo)
            resultados.append(resultado)
            
            # Pausa para evitar bloqueio (rate limiting)
            if i < len(lista_destinatarios) - 1:
                time.sleep(2)
                
        return resultados
