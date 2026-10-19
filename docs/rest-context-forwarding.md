# Repasse do tenant em chamadas REST entre serviços

`goframework.RestRequest` repassa sozinho os headers do contexto da requisição de origem
quando o destino é uma **API interna**. O serviço não precisa passar nenhum header.

## O que é repassado

`X-Tenant-Id`, `X-Author`, `X-Author-Id`, `X-Correlation-Id` e `X-CreatedAt`: os mesmos que o
producer Kafka já copia para as mensagens. O contexto pode ser um `*gin.Context` (lê os headers
da requisição), um `*ConsumerContext` (lê os headers da mensagem) ou um `context.Context` com os
valores. Valores ausentes não são enviados.

Regras:
- **O que o chamador passa nos headers vence.** O repasse só preenche o que faltou, inclusive
  um header passado com valor vazio.
- **O `Authorization` não é repassado.** Continua sendo responsabilidade do chamador.
- O trace do OpenTelemetry segue sendo propagado como antes, para qualquer destino.

## O que é "interno"

A regra é fechada: o que não casar é externo e **nunca** recebe esses headers.

| Interno | Exemplos |
|---|---|
| Loopback e IPs privados (RFC 1918, `fc00::/7`) | `localhost`, `127.0.0.1`, `10.100.12.191` |
| DNS do Kubernetes | `x.ns.svc`, `x.ns.svc.cluster.local`, `x.cluster.local` |
| Nomes curtos da plataforma (todo rótulo começa com `tilid-`) | `tilid-provider-service.tilid-provider-service`, `tilid-provider-fleet` |
| `*.apitilid.com.br` | `service-dev.apitilid.com.br` |

Não há palpite por quantidade de rótulos: `exp.host` parece um nome curto de cluster e é uma API
externa. Também são externos: `maps.googleapis.com`, `api.edenai.run`, `graph.facebook.com`,
`tilid-foo.com`, `*.conectahub360.com.br`, `auth.auth`.

### Configuração (opcional)

| Variável | Efeito |
|---|---|
| `INTERNAL_HOSTS` | Acrescenta hosts internos (não substitui os padrões). Separados por vírgula, espaço ou `;`. Cada item é um host exato, `*.dominio` (sufixo) ou `prefixo-*` (nome curto). Ex.: `INTERNAL_HOSTS="*.conectahub360.com.br,auth.auth"` |
| `REST_FORWARD_CONTEXT` | `false` desliga o repasse automático. |

## Redirecionamentos

Se um host interno responder com redirecionamento para um host **externo**, os headers
repassados automaticamente são removidos da nova requisição. O Go já remove o `Authorization`
em redirecionamentos entre domínios, mas não headers customizados como o `X-Tenant-Id`.

## No destino

O `AddTenant` do serviço que recebe usa o `X-Tenant-Id` da requisição quando ele existe. Sem ele,
usa o `tenant_id` do JWT; sem JWT, o tenant zero. Por isso o repasse resolve as chamadas vindas
de consumers Kafka (que não têm `Authorization`) e os métodos que só repassavam o JWT.

Atenção: o `X-Tenant-Id` recebido tem precedência sobre o JWT. O gateway deve descartar esse
header em requisições vindas de fora.
