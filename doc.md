AUTOMA-O_DE_PROSPEC-O_CANDIDATURA_AUTOM-TICA
Envio de E-mails Personalizados e Automatizados via SMTP do Gmail.

Esta aplicação web permite o upload de listas de destinatários (URLs ou documentos) e automatiza o disparo de e-mails altamente personalizados, com a opção de anexos, utilizando o servidor SMTP seguro do Gmail.

✨ Funcionalidades
Upload de Lista: Suporte para subir uma lista (ex: arquivo CSV) contendo endereços de e-mail e dados variáveis.

Envio Personalizado (Mail Merge): Opção de usar campos variáveis da lista (como Assunto e Corpo do E-mail) para personalizar cada mensagem.

Anexo Universal: Possibilidade de anexar um arquivo (ex: CV, portfólio) que será incluído em todos os e-mails da automação.

Autenticação Segura: Utiliza Chave de Aplicativo (App Password) do Google para se conectar ao Gmail/SMTP, garantindo que sua senha principal nunca seja exposta.

Opções de Saída: Após o processamento/envio, oferece a opção de Download do log de resultados ou Visualização na interface.

🛠️ Requisitos de Instalação (Simulação)
Para que o script de automação funcione corretamente (especialmente se for um script em Python/Node.js/Backend), você precisará:

Node.js / Python (ou o ambiente de backend escolhido): Para executar a lógica de automação e envio.

Servidor Web Local (Opcional): Para servir o HTML/CSS localmente durante o desenvolvimento.

🔑 Configuração de Segurança (Obrigatório)
Para que a aplicação possa enviar e-mails pelo seu Gmail, você deve configurar uma Chave de Aplicativo (App Password), pois ela é a forma segura de autenticação.

Passos para Gerar a Chave de Aplicativo:

Certifique-se de que a Verificação em Duas Etapas (2FA) esteja ativada na sua conta Google.

Acesse as Configurações de Segurança da sua conta Google.

Vá em "Verificação em Duas Etapas" e procure por "Senhas de app" (ou "Chaves de Aplicativo").

Crie uma nova chave para este aplicativo, dando um nome como "App Email Automation".

A chave gerada (uma sequência de 16 caracteres, ex: abcd efgh ijkl mnop) é a que você deve inserir no campo de "Chave de Aplicativo" da interface.

⚙️ Como Usar
Abra o index.html no seu navegador (ou execute o servidor local, se aplicável).

Na seção de Credenciais de Envio, insira seu endereço de e-mail e a Chave de Aplicativo gerada.

Prepare sua Lista de Dados:

Crie um arquivo CSV (ou similar) onde a primeira coluna deve ser o email_destinatario.

(Opcional) Adicione colunas para assunto_personalizado e corpo_personalizado se for usar a opção avançada.

Faça o Upload da sua lista na seção de "Upload de Lista".

Preencha os Campos Padrões (Assunto e Corpo do E-mail).

(Opcional) Anexe o Documento (CV, etc.) a ser enviado.

Clique em "Iniciar Automação".

Acompanhe o status na Área de Resultado. Ao finalizar, use as opções Download Log ou Visualizar Resultados.

📄 Estrutura do Projeto
/
├── index.html          # Interface principal do usuário (HTML/CSS/JS)
├── style.css           # Estilização da interface
└── script.js (ou backend) # Lógica de leitura de dados e comunicação SMTP