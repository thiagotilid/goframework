# Graceful shutdown

Desde a v4.8.0, `GoFramework.Start()` trata `SIGTERM` e `SIGINT`. Os serviços só precisam
atualizar a versão: nenhuma mudança de código é necessária.

## O que acontece no sinal

1. `/health/ready` passa a responder `503` (o `/health` e o `/health/live` não mudam).
2. Os consumers Kafka param de ler. A mensagem em processamento termina, é commitada e o
   consumer sai do grupo (`Close()`), então o Kafka redistribui as partições na hora.
3. O servidor HTTP para de aceitar conexões e espera as requisições em andamento.
4. O OpenTelemetry exporta o que faltava (até 3s).
5. `Start()` retorna `nil`. Se o `ListenAndServe` falhar por outro motivo, retorna o erro,
   como antes.

Um segundo sinal encerra o processo imediatamente.

## Configuração (opcional)

Lida pelo Viper: variável de ambiente ou arquivo de config.

| Variável | Padrão | Significado |
|---|---|---|
| `SHUTDOWN_TIMEOUT_SECONDS` | 20 | Tempo total de drenagem (HTTP e Kafka em paralelo, incluindo o atraso abaixo). Passado o prazo, o processo sai mesmo assim. |
| `SHUTDOWN_DELAY_SECONDS` | 0 | Espera, no início da drenagem, antes de fechar o listener. Útil em serviços sem `preStop`. |

O padrão de 20s cabe no `terminationGracePeriodSeconds` padrão do Kubernetes (30s).

## Endpoints de health

| Rota | Resposta |
|---|---|
| `/health` | Igual a sempre: verifica as dependências registradas. |
| `/health/live` | Sempre `200` enquanto o processo roda. Não depende de Mongo, Redis ou Kafka. Use como liveness. |
| `/health/ready` | Verifica as dependências e responde `503` assim que o desligamento começa. Use como readiness. |

`/health/live` e `/health/ready` não são gravadas na coleção `routes`.

## Kubernetes recomendado

```yaml
strategy:
  rollingUpdate: { maxUnavailable: 0, maxSurge: 1 }
template:
  spec:
    terminationGracePeriodSeconds: 70   # >= preStop + SHUTDOWN_TIMEOUT_SECONDS + ~10s de folga
    containers:
    - lifecycle:
        preStop: { exec: { command: ["sleep", "15"] } }   # remoção do pod dos endpoints
      env:
      - { name: SHUTDOWN_TIMEOUT_SECONDS, value: "45" }
      livenessProbe:  { httpGet: { path: /health/live,  port: 8081 } }
      readinessProbe: { httpGet: { path: /health/ready, port: 8081 } }
```

A conta do orçamento: `grace >= preStop + SHUTDOWN_TIMEOUT_SECONDS + folga`. A folga cobre o
flush do OpenTelemetry.

## Garantias e limites

- **Kafka é at-least-once.** O commit acontece depois do processamento. Se o processo for
  morto à força (SIGKILL, timeout), a mensagem em andamento é entregue de novo. Os handlers
  devem ser idempotentes.
- **O tenant não é afetado.** Ele continua viajando como antes: `*gin.Context` (header
  `X-Tenant-Id`) no HTTP e `*ConsumerContext` (headers da mensagem) no Kafka. O contexto de
  desligamento nunca é passado aos handlers. Os testes que fixam isso estão em
  `tenant_test.go`.
- **Mongo e Redis não são fechados** pelo framework; o fim do processo libera as conexões.
- **Goroutines do próprio serviço** (`go func()` soltos) não são rastreadas pelo framework.
- Um consumer registrado depois do início do desligamento não é iniciado.
- Falhas de commit passam a aparecer no log: `goframework: kafka commit failed: ...`.
