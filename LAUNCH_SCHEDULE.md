# Launch Schedule — CNPJ Intel

**Target launch: 2 semanas a partir de 2026-05-05**

---

## Estado Atual
**Última atualização: 2026-05-08**

| Área | Status |
|------|--------|
| Instagram @cnpjintel | ✅ Criado |
| LinkedIn (perfil + conteúdo do post) | ✅ Pronto — publicar post |
| Facebook (página @cnpjintel) | ✅ Criado |
| Vídeo demo | 🟡 Roteiro pronto — gravar |
| Grupo WhatsApp early adopters | 🟡 Template pronto — criar grupo |
| Conteúdo gerado | `docs/launch/qui-08-05-conteudo.md` |

---

## Semana 1 — Produto pronto para vender

### Ter 06/05 (hoje)
- [X] Deploy da nova landing page no Netlify (upload LP1/index.html)
- [X] Testar fluxo completo: acesso → dashboard → filtro → exportação CSV
- [X] Confirmar que `/api/public-stats` retorna dados reais na landing page

### Qua 07/05 — Pagamento real
- [ ] Fazer uma compra de teste real no Kiwify (Básico R$50) com cartão próprio
- [ ] Verificar se webhook dispara e plano sobe automaticamente no Railway
- [ ] Testar fluxo de login Google OAuth com conta nova (nunca usou)
- [ ] Se webhook falhar: debugar e corrigir `api.py` antes de qualquer marketing

### Qui 08/05 — Conteúdo base
- [ ] Gravar 1 vídeo de demonstração curto (2-3 min): filtrar → exportar → abrir no WhatsApp ← roteiro pronto em `docs/launch/qui-08-05-conteudo.md` (OBS + ElevenLabs + CapCut)
- [X] Criar conta no Instagram @cnpjintel
- [X] Criar página no Facebook @cnpjintel
- [X] Criar conta no LinkedIn e configurar perfil — conteúdo do primeiro post pronto em `docs/launch/qui-08-05-conteudo.md` (publicar ainda)
- [X] Criar grupo WhatsApp de early adopters — template pronto em `docs/launch/qui-08-05-conteudo.md`

### Sex 09/05 — Prospecção manual
- [ ] Listar 20 grupos de vendedores no WhatsApp/Telegram onde pode postar
- [ ] Redigir mensagem de divulgação (não spam: oferecer o grátis, pedir feedback)
- [ ] Enviar para 5 conhecidos que trabalham com vendas B2B pedindo teste

### Sáb 10/05 — SEO e distribuição
- [ ] Criar perfil no Product Hunt (não lançar ainda, só preparar)
- [ ] Escrever post para LinkedIn: "Como gero listas de prospecção em 10 segundos" (com vídeo)
- [ ] Configurar Google Analytics / Plausible na landing page (1 linha de código)

### Dom 11/05 — Buffer
- [ ] Resolver qualquer bug crítico encontrado durante a semana
- [ ] Responder todos os feedbacks de early adopters
- [ ] Ajustar copy da landing page se necessário

---

## Semana 2 — Lançamento e crescimento

### Seg 12/05 — Canais pagos (pequeno teste)
- [ ] Criar 1 anúncio no Meta Ads (R$20/dia): público vendedores SP/MG, objetivo tráfego
- [ ] Headline do anúncio: "Telefone e e-mail de qualquer empresa em 10 segundos"
- [ ] Landing page como destino (não o app direto)

### Ter 13/05 — Comunidades
- [ ] Postar em grupos do Facebook: "Vendedores B2B Brasil", "SDR Brasil", etc.
- [ ] Postar em comunidades do Reddit (r/empreendedorismo) se aplicável
- [ ] Publicar no LinkedIn novamente: resultados reais com print do dashboard

### Qua 14/05 — Lançamento Product Hunt
- [ ] Publicar no Product Hunt às 8h UTC (5h Brasília)
- [ ] Pedir upvotes para rede de contatos no dia do lançamento
- [ ] Monitorar e responder todos os comentários no dia

### Qui 15/05 — E-mail / WhatsApp marketing
- [ ] Enviar e-mail/mensagem para todos os usuários gratuitos cadastrados: "Novo plano Pro"
- [ ] Oferecer desconto de 1º mês para quem converter essa semana
- [ ] Configurar e-mail automático de boas-vindas no Railway (ou via Resend)

### Sex 16/05 — Análise e iteração
- [ ] Analisar métricas: visitantes landing page, cadastros, conversões pagas
- [ ] Identificar onde as pessoas abandonam o funil
- [ ] Ajustar CTA ou preço se conversão estiver abaixo de 2%

### Sáb 17/05 — Lançamento oficial
- [ ] Post "estamos ao vivo" no LinkedIn + Instagram
- [ ] Anunciar nos grupos de WhatsApp
- [ ] Responder todas as dúvidas

---

## KPIs da semana de lançamento
- 500 visitantes na landing page
- 50 cadastros gratuitos
- 5 assinaturas pagas (R$250–R$500 MRR)
- 1 feedback qualitativo por dia de usuário real

---

## Checklist técnico antes do lançamento (não pule)
- [ ] Webhook Kiwify testado com pagamento real
- [ ] Google OAuth testado com conta nova
- [ ] `/api/public-stats` retornando dados corretos
- [ ] Exportação CSV funcionando no plano Básico e Pro
- [ ] Railway com restart automático configurado
- [ ] Não há `console.error` no dashboard com usuário logado

---

## Mensagem de divulgação (template WhatsApp)
```
Oi [nome]! Criei uma ferramenta que gera listas de prospecção
de empresas brasileiras com telefone e e-mail em segundos.
Está gratuito para testar: cnpjintel.netlify.app
Me diz o que acha?
```
