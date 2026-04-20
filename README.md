# Garimpo ⚒️

O **Garimpo** é uma ferramenta poderosa de prospecção automatizada e extração de leads, projetada para minerar dados valiosos e facilitar o outreach via email de forma inteligente.

## 🚀 O que o app faz?
O Garimpo automatiza três pilares principais de vendas B2B:
1.  **Extração de Emails**: Varre sites específicos em busca de contatos.
2.  **Prospecção de Leads**: Busca empresas por nicho, localização ou CNAE em múltiplas fontes públicas.
3.  **Envio Automatizado**: Dispara emails personalizados em lote para os leads minerados.

---

## ✨ Funcionalidades Principais

-   **Interface Premium**: Design moderno em Dark Mode com foco em usabilidade.
-   **Enriquecimento de Dados**: Integração com a Receita Federal (BrasilAPI) para obter Capital Social, Sócios, Porte e Situação Cadastral.
-   **Filtros de Qualificação**: Filtre por idade da empresa (slider), porte (MEI/ME/EPP), capital social e presença digital.
-   **IA de Detecção**: Identifica automaticamente sites e redes sociais (Instagram, Facebook, LinkedIn) dos leads.
-   **Gestão de Anexos**: Suporte para envio de PDFs, CVs ou apresentações comerciais fixas no disparo.
-   **Logs em Tempo Real**: Terminal integrado para acompanhar o status de cada extração e envio.

---

## 🔍 Fontes de Busca (Mining Sources)
O sistema "garimpa" dados em 7 fontes simultâneas:
-   **Google Maps**: Negócios locais e estabelecimentos físicos.
-   **CNPJ.biz**: Diretório nacional de empresas por segmento.
-   **Encontrei**: Portal brasileiro de serviços e comércio.
-   **OLX**: Leads de pequenos negócios e prestadores de serviço ativos.
-   **Mercado Livre**: Vendedores e sellers com perfil comercial.
-   **Jucesp**: Empresas recém-abertas no estado de São Paulo (**Hot Leads**).
-   **Busca por CNAE**: Prospecção técnica por código de atividade econômica.

---

## 🛠️ Tecnologias Utilizadas

-   **Backend**: Python (Flask)
-   **Automação/Scraping**: Selenium, BeautifulSoup4, Webdriver Manager
-   **API Externa**: BrasilAPI (Receita Federal)
-   **Check de Rede**: Verificação de DNS e status de sites
-   **Frontend**: HTML5, CSS3 (Custom Design System), JavaScript (Vanilla)
-   **Envio de Email**: Protocolo SMTP (especializado para Gmail/App Passwords)

---

## 📦 Como Instalar e Rodar

1.  **Requisitos**: Python 3.10+
2.  **Instalar Dependências**:
    ```bash
    pip install flask selenium beautifulsoup4 python-dateutil requests webdriver-manager
    ```
3.  **Iniciar o App**:
    ```bash
    python app.py
    ```
4.  **Acessar**: Abra o navegador em `http://localhost:5000`

---

## 🛡️ Segurança
O Garimpo foi construído priorizando a segurança:
-   Não armazena credenciais (utiliza Senhas de Aplicativo do Google).
-   Executa scraping de forma ética e respeitando os limites de cada plataforma.
-   Funciona em modo **Headless** (segundo plano) para não interferir no uso do computador.

---

## ☁️ Deploy no Render (Plano Gratuito)

Para colocar o Garimpo online no Render e evitar que ele "durma", siga estas etapas:

1.  **Crie o Web Service** no Render conectando seu repositório.
2.  **Build Command**:
    ```bash
    ./render-build.sh
    ```
3.  **Start Command**:
    ```bash
    gunicorn app:app
    ```
4.  **Variáveis de Ambiente (Environment Variables)**:
    -   `RENDER_EXTERNAL_URL`: Sua URL do Render (ex: `https://garimpo.onrender.com`) - **Essencial para o Keep-Awake!**
    -   `PYTHON_VERSION`: `3.10.0` (opcional)

---
**Desenvolvido para minerar o que há de melhor no mercado.** ⚒️✨
