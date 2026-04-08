# CNPJ Intel

Sistema completo de inteligência empresarial — coleta, enriquece e vende dados de CNPJs.

## Estrutura

```
/
├── api.py              ← API FastAPI (servidor principal)
├── database.py         ← Banco de dados SQLite
├── extrator.py         ← Extrai CNPJs da base da Receita Federal
├── requirements.txt    ← Dependências Python
├── Procfile            ← Comando de start para Railway
├── railway.json        ← Config do Railway
├── agent/
│   └── agent.py        ← Agente de coleta e enriquecimento
├── app/
│   └── index.html      ← Dashboard do cliente (área logada)
└── landing/
    └── index.html      ← Página de vendas (Netlify)
```

## Deploy rápido

### 1. Landing page (Netlify)
- Arraste a pasta `landing/` em netlify.com/drop
- Pronto — sua página de vendas está no ar

### 2. API + Dashboard (Railway)
- Crie conta em railway.app
- New Project → Deploy from GitHub → selecione este repositório
- Configure as variáveis de ambiente:
  - `TOKENS=token1,token2,token3`  (um por cliente)
  - `ADMIN_TOKEN=sua_senha_admin`
- Railway detecta o Procfile e sobe automaticamente

### 3. Conectar landing → app
No arquivo `landing/index.html`, substitua o `alert()` da função `submeterLead()` por:
```javascript
window.location.href = "https://SEU-APP.railway.app?token=TOKEN_DO_CLIENTE";
```

## Como liberar acesso para um novo cliente

1. Gere um token único (ex: `cliente_joao_2026`)
2. Adicione na variável `TOKENS` no Railway
3. Envie o link: `https://SEU-APP.railway.app?token=cliente_joao_2026`

## Extrair CNPJs da Receita Federal

1. Baixe um arquivo em: https://arquivos.receitafederal.gov.br
2. Coloque na pasta raiz
3. Rode:
```bash
python extrator.py --arquivo Estabelecimentos0.zip --uf MG --limite 50000
python agent/agent.py
```

## Variáveis de ambiente

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `TOKENS` | Tokens de acesso dos clientes | `abc123,xyz789` |
| `ADMIN_TOKEN` | Token do administrador | `minha_senha` |
| `GOOGLE_API_KEY` | Google Places (opcional) | `AIza...` |
| `HUNTER_API_KEY` | Hunter.io para e-mails (opcional) | `abc...` |
| `PORT` | Porta do servidor (Railway define auto) | `8000` |
