# CLAUDE.md

## Objetivo atual

Implementar **graceful shutdown** no goframework (branch `dev/graceful_shutdown`, linha **v4**, módulo `github.com/thiagotilid/goframework/v4`, base `v4.7.7`), para que pods em rolling update no Kubernetes não percam requisições HTTP nem mensagens Kafka em processamento.

Entrega prevista: tag **v4.8.0**. Cada serviço só troca a versão no `go.mod`, sem alterar código.

## Status (2026-10-16): implementado e validado, ainda não commitado

Fases 0 a 7 do plano prontas na branch `dev/graceful_shutdown`. Falta: commit, PR, tag `v4.8.0-rc.1` e adoção nos serviços (nada disso foi feito).

- Código: `lifecycle.go` (novo), `Start()` e health em `tlGoFramework.go`, consumers em `kafka.go`, seam do DLQ em `kafka_consumer.go`.
- Testes: `tenant_test.go` (caracterização, também passa no v4.7.7), `lifecycle_test.go`, `kafka_shutdown_test.go`, `compat_test.go`, `config_test.go`, `main_test.go`. `go test -race ./...` passa, repetido várias vezes.
- Documentação: `docs/graceful-shutdown.md`.
- Validação com cópia do `service` contra o DEV (`teste/`, ignorada pelo git): v4.7.7 passa 8 de 18 cenários de shutdown/health (as falhas são exatamente o problema: requisição em voo cortada, exit -15, sem `/health/live` e `/health/ready`); o código novo passa 18 de 18, e os testes de tenant (HTTP e Kafka, escrita no Mongo) têm resultado idêntico nos dois. Rolling restart com o mesmo consumer group: o novo processo assume as partições em 6s (v4.7.7: 45s, o session timeout). Os testes do próprio `service` passam igual nos dois.
- Mudança de comportamento além do shutdown: falha de commit Kafka agora é logada (antes era silenciosa); log de recebimento do sinal; `time.Local` só é escrito se ainda não for UTC.

**Critério de sucesso inegociável: o TenantId continua sendo passado via context exatamente como hoje.** Se o tenant parar de chegar, o sistema todo quebra.

Como o tenant trafega (não pode mudar). Ele depende do **tipo concreto** do contexto, via type switch em `GetContextHeader`, `ToContext`, `AddToContext`, `helperContext` e `helperContextKafka` em `helper.go`:
- `*gin.Context`: lê de `c.Request.Header` (`X-Tenant-Id`), gravado pelo middleware `AddTenant` a partir do JWT (ou `00000000-0000-0000-0000-000000000000` sem `Authorization`).
- `*ConsumerContext`: lê de `Msg.Headers`.
- Outro `context.Context`: lê de `ctx.Value(chave)`.

Invariantes: o handler HTTP continua recebendo o `*gin.Context` do mesmo engine (`gf.server`); o handler Kafka continua recebendo `*ConsumerContext` como ponteiro com `Msg` preenchido; nenhum contexto de ciclo de vida é passado a handlers nem os embrulha; `AddTenant` e `helper.go` não são alterados.

Origem: incidente de 2026-08-05 (proposta Notro 02720436: o rolling update matou o pod do `service` no meio do `ReceiveProposal`) e o documento `conectahub360/back/conectahub_docs/overview/deploy-graceful-shutdown.md`, que descreve a causa, o patch proposto e a configuração do Kubernetes.

## Onde estão os serviços (atenção)

**Somente leitura: não alterar nada em `conectahub360/back`.** Qualquer teste com um serviço usa uma cópia dentro deste repositório (`teste/`, ignorada pelo git, porque os configs têm segredos).

Os serviços reais ficam em `/Users/thiago/Documents/tilid/apphub/conectahub360/back/`. **Não usar** `/Users/thiago/Documents/backups/devops/Prestadores`: são backups de dez/2024 em goframework v1.61 (API antiga, sem relação com o estado atual).

## Análise do estado atual do framework

| Ponto | Problema |
|---|---|
| `Start()` em `tlGoFramework.go` | Usa `gf.server.Run()`, que não trata sinal nem drena requisições. O `otelShutdown` depois dele é código morto, porque o `Run` não retorna no SIGTERM. Traces, métricas e logs finais se perdem. |
| Consumers em `kafka.go` | `Consumer`, `ConsumerWithSettings`, `ConsumerMultiRoutine` e `ConsumerWithWorker` rodam em `go func` com `for { ReadMessage(-1) }`. Não há como parar, nem `Close()`, nem espera pelo processamento em andamento. O pod não sai do consumer group e o rebalance só ocorre após o session timeout. |
| `/health` | Serve de readiness e liveness. Não reflete que o pod está encerrando. |
| `worker` em `kafka.go` | **Bug:** o `<-messages` no fim do processamento consome outra mensagem do canal sem processá-la, e o commit é duplicado. Baixa prioridade: ninguém usa `ConsumerWithWorker` nos serviços verificados. |
| Criação do consumer | O código de `kafka.ConfigMap` e de `kafka.NewConsumer` está duplicado nos quatro métodos. |

O `Publish` do producer é **síncrono** (espera o canal de entrega). Uma publicação em andamento termina durante a drenagem do HTTP ou do handler Kafka, então não é preciso flush do producer.

## Funcionalidades implementadas

- **Configuração por variável de ambiente (Viper).** `initializeViper()` em `tlGoFramework.go` lê primeiro a variável de ambiente e, na ausência dela, o arquivo `configs/<env>.json`.
  - Mapeamento: o ponto da chave vira `_` e o nome fica em maiúsculas. `redis.pass` lê `REDIS_PASS`, `kafka.groupid` lê `KAFKA_GROUPID`.
  - Uso pretendido: senhas só por variável de ambiente, removidas do arquivo. Funciona porque o framework usa `Get*` e nunca `Unmarshal`.
  - Arquivo de config ausente é tolerado. Arquivo inválido ainda causa `panic`.
  - Limitação: `GetStringSlice` por variável separa por espaço, não por vírgula.
  - Testes em `config_test.go`.
  - **Efeito colateral a conhecer:** ao subir para v4.8.0, uma variável de ambiente que já exista no pod com o nome de uma chave passa a sobrescrever o arquivo. Improvável, mas é a única mudança de comportamento que não é puramente aditiva.

## Restrição de compatibilidade (inegociável)

Vários serviços já usam o goframework. A atualização deve ser **só trocar a versão**.

Levantamento nos serviços reais (~60 repositórios em `conectahub360/back`):
- **Versão:** a maioria está em `goframework/v4 v4.7.7` (`go 1.25`). Alguns jobs estão em `v4.4.0` (`absencejob`, `audit`, `financejob`, `fipejob`, `parkedvehiclejob`, `pdfgenerator`, `servicesjob`, `telemetry`, `whatsapp`), `autointegration` e `mcp` em `v4.7.6`, e o `tilid.provider.consumers` em `v3.0.31`. A mudança será testada contra a API de v4.7.7; os serviços em v4.4.0 precisam de validação extra.
- **Padrão do `main`:** `NewGoFramework()`, `RegisterDbMongo`, `RegisterKafka`, `RegisterProducer(goframework.NewKafkaProducer)`, `RegisterRedis`, `Register*`, `server.Invoke(initialize.Start)` e `server.Start()`.
- **O retorno do `Start()` é usado em alguns serviços:** `autoaccept` e `integrationgate` fazem `if err := server.Start(); err != nil { panic(err) }`. **`Start()` tem de devolver `nil` em encerramento limpo**, senão o SIGTERM vira panic.
- **`Start()` é chamado em goroutine** em `notifiedjob` (`go func(){ server.Start() }()`, com `server.Invoke(startupJob)` no `main`).
- **Trabalho concorrente ao `Start()`:** `go runInitializers(server)` (`autoaccept`) e um bloco de boot em goroutine (`integrationgate`).
- **Consumers registrados depois do início do `Start()`:** o `integrationgate` chama `registerConsumer(server)` dentro dessa goroutine de boot. O registro de consumers não pode assumir que termina antes do sinal.
- **Uso de API (contagem de chamadas):** `.Consumer(` 416, `ConsumerWithSettings` 48, `ConsumerMultiRoutine` 28. Os três precisam de shutdown.
- **Redis:** vem de `RegisterRedis` (18 usos) ou de construtor próprio do serviço. Mongo e Redis **não** serão fechados pelo framework (ver plano).
- **Probes e manifestos:** os manifestos Kubernetes não estão nos repositórios dos serviços. O `integrationgate` tem rota própria `GET /ready` na raiz. Nenhum serviço registra `/health/live` ou `/health/ready`, então esses caminhos novos não colidem (o gin entra em `panic` com rota duplicada).
- **Dockerfile do `service`:** `ENTRYPOINT ["cmd"]` em formato exec, base `debian:bookworm` (tem `sleep` para o `preStop`). O binário é o PID 1 e recebe o SIGTERM direto.

Superfície pública que **não pode mudar**: `NewGoFramework`, `GoFrameworkOptions`, `Start()` (assinatura, bloqueio e retorno), `GetConfig`, `Register*`, `Invoke`, `GoKafka` (`Consumer`, `ConsumerWithSettings`, `ConsumerMultiRoutine`, `ConsumerWithWorker`, `NewKafkaConfigMap`), `ConsumerContext`, `ConsumerFunc`, `ToContext`, `AddToContext`, `NewKafkaProducer` e a rota `/health`.

## Plano

0. **Fase 0, antes de qualquer mudança: testes de caracterização do tenant** escritos e passando sobre o código atual (v4.7.7), para fixar o comportamento. Rodam depois de cada fase e no gate final. Ver "Pendente: fase de testes".
1. **Sinal dentro do `Start()`.** `signal.NotifyContext(gf.mainCtx, SIGTERM, SIGINT)` criado no `Start()`. O `NewGoFramework` não muda para o serviço.
2. **HTTP.** `Start()` usa `http.Server{Addr, Handler: gf.server}` em goroutine. Ao receber o sinal, chama `srv.Shutdown(ctx)`. `http.ErrServerClosed` vira `nil`. Qualquer outro erro do `ListenAndServe` é devolvido, como hoje.
3. **Tempo.** `SHUTDOWN_TIMEOUT_SECONDS` (nome do documento do incidente), padrão 20s, vale para HTTP e Kafka juntos, que drenam em paralelo. Padrão seguro dentro do grace period padrão do Kubernetes (30s), sem alterar o manifesto.
   - `SHUTDOWN_DELAY_SECONDS` (padrão 0): espera interna opcional entre o sinal e o fechamento do listener, para serviços sem `preStop`. Com o `preStop` do manifesto recomendado, fica em 0.
   - O flush do OTel roda depois da drenagem, com teto de 3s.
   - **Orçamento recomendado no manifesto:** `terminationGracePeriodSeconds` ≥ preStop + `SHUTDOWN_TIMEOUT_SECONDS` + 10s. Os números do documento (grace 60, preStop 15, timeout 45) não deixam folga para o flush do OTel; usar timeout 35 ou grace 70.
4. **Kafka.** Sem mudar assinatura nem a semântica de `panic` do handler (retry e DLQ).
   - O `*GoKafka` único (criado em `RegisterKafka` e injetado pelo dig) guarda o contexto de ciclo de vida e um registro de consumers protegido por mutex, com flag de encerramento. Consumer registrado depois do início do shutdown não inicia. O valor zero do `GoKafka` continua utilizável (`NewKafkaConfigMap` é exportado) e cai no comportamento antigo.
   - `ReadMessage(-1)` vira `ReadMessage(timeout curto)` verificando o sinal. **`kafka.ErrTimedOut` não pode ser logado como erro**, senão há uma linha de log por segundo por consumer.
   - Ao parar: termina a mensagem atual, commita, `Close()` (sai do grupo na hora; o `cooperative-sticky` já está configurado) e sinaliza o `WaitGroup`.
   - Contexto do processamento (risco do tenant): **não muda.** Os quatro métodos já extraem o trace sobre `context.Background()`, então o handler já não é cancelado pelo sinal. O contexto de ciclo de vida só controla o loop de leitura e nunca é passado ao handler (por isso `context.WithoutCancel` não é necessário). O tenant viaja em `ConsumerContext.Msg.Headers`, lido pelo `ToContext`, e o `ConsumerContext` continua sendo criado como ponteiro com `Msg` preenchido, porque `ToContext` e `AddToContext` fazem type switch em `*ConsumerContext`.
   - Testabilidade: o loop de leitura depende de uma interface interna pequena (`ReadMessage(timeout)`, `CommitMessage`, `Close`), implementada por `*kafka.Consumer`, para testar com um fake sem broker.
   - `MultiRoutine` e `WithWorker`: esperar as goroutines em voo e fechar o canal `messages`.
   - Helper interno para a criação do consumer, hoje duplicada nos quatro métodos.
5. **Mongo e Redis não são fechados pelo framework.** Há inicializações em goroutine (`autoaccept`, `integrationgate`) usando esses clientes durante o shutdown, e o encerramento do processo já libera as conexões. Fechar traria risco sem ganho.
6. **Health, só aditivo.**
   - `/health`: comportamento idêntico ao atual, inclusive durante o shutdown.
   - `/health/live`: sempre 200.
   - `/health/ready`: roda os `healthCheck` e responde 503 a partir do sinal (flag atômica).
   - As duas rotas novas ficam fora da coleção `user.routes` gravada por `RegisterRoutes`.
7. **Bug do `worker`:** corrigir o `<-messages` e o commit duplicado.
8. **Documentação:** README e skill `tilid-gobackend` com as variáveis, as probes recomendadas, `maxUnavailable: 0` e `maxSurge: 1`. Atualizar também o `deploy-graceful-shutdown.md` com os números de orçamento.
9. **Testes.** Os de tenant (seção Pendente), mais:
   - `/health/ready` vira 503 no sinal, `/health/live` e `/health` inalterados;
   - requisição lenta que termina após o SIGTERM;
   - `Start()` devolve `nil` no encerramento limpo;
   - handler Kafka que commita antes da saída, incluindo `panic` indo para DLQ durante o shutdown;
   - consumer registrado depois do início do shutdown não inicia;
   - timeout estourando sem travar;
   - `Start()` chamado em goroutine;
   - compatibilidade da API pública (um `main` no estilo do `service` compilando sem alterações).
10. **Validação local com uma cópia do `service`, contra o ambiente de DEV (substitui a varredura nos ~50 serviços).**
   - Cópia em `teste/tilid.provider.service/`, sem `.git`, com `teste/` no `.gitignore` deste repositório. O original não é tocado.
   - **Decisão: a cópia continua apontando para o DEV**, como no `cmd/configs/local.json`: Mongo Atlas (`clusterdev`, banco `providerservices`), Kafka `kafka-dev.apitilid.com.br:31522`, Redis `redis-dev.apitilid.com.br:6379` e as APIs `*-dev.apitilid.com.br`. O motivo é haver dados reais para testar. Conectividade com o Kafka de dev verificada em 2026-10-16; o SRV do Mongo resolve. Não há Docker nem infra local.
   - Cuidados mantidos mesmo no dev:
     - **Consumer group exclusivo** (`providerservices-teste-<sufixo>`), com os offsets posicionados no fim antes da primeira execução. Com o mesmo group id a cópia roubaria partições do pod de dev e seus testes de SIGTERM provocariam rebalance nele. Com um group novo e `auto.offset.reset=earliest` (fixo no framework) ela reprocessaria o histórico inteiro dos tópicos.
     - **Tenant de teste `d7b8d4da-2da7-458b-8870-3b18fc13e368`** (definido pelo usuário) em todas as requisições e mensagens, para identificar o que for gravado no Mongo e no Redis de dev (as chaves de dedup do Redis incluem o tenant).
     - Os handlers publicam eventos nos tópicos de dev, que outros serviços de dev consomem. Isso foi aceito. Ficam de fora os cenários que dependem de integrações externas (bucket OCI, Eden, Google).
   - `go.mod` da cópia com `replace github.com/thiagotilid/goframework/v4 => ../..`.
   - Roteiro: (a) linha de base com v4.7.7 (sem o `replace`), (b) mesmo roteiro com o código novo, (c) comparar. Cenários: sobe e `/health`, `/health/live` e `/health/ready`; requisição HTTP com JWT e tenant X que grava no Mongo com o tenant X; mensagem Kafka com o header do tenant X cujo consumer grava no Mongo com o tenant X; sem tenant (UUID zero) e mensagem sem header; SIGTERM com requisição lenta em andamento (rota de teste adicionada **só na cópia**) termina com 200; SIGTERM durante consumo (offset commitado, consumer sai do grupo, nada perdido); `Start()` retorna `nil` (exit code 0); consumers sob volume sem os logs de timeout por segundo; segundo SIGTERM mata na hora.
   - Sem limpeza dos dados de teste no Mongo e no Redis de dev: o ambiente de dev é para isso (decisão do usuário). Os consumer groups de teste (um por processo) não são apagados: o `confluent-kafka-go` v1.9.2 não tem API de exclusão, e grupos vazios expiram sozinhos.
   - **Achado da validação:** os tópicos de produção do dev também são consumidos pelos serviços de dev implantados, então escrever neles não prova o que a cópia faz. Os cenários Kafka usam tópicos criados por execução (`zz-graceful-test-*`) e um consumer só da cópia (`TESTE_TOPIC`).
   - Resultados em `teste/result-*.json`; ferramentas em `teste/tilid.provider.service/teste_tools` e `teste/scenario.py`.
   - Executar a suíte de testes do próprio serviço na cópia, se existir.

## Decisões

- Entrega na linha v4 (v4.8.0). Nenhuma mudança de assinatura pública. Tudo aditivo e com padrão seguro.
- Os três métodos de consumer em uso são alterados, não só o `Consumer`.
- Timeout único para HTTP e Kafka, com o nome de variável do documento (`SHUTDOWN_TIMEOUT_SECONDS`).
- Commit depois do processamento (at-least-once). Os handlers dos serviços devem ser idempotentes.
- Fora do escopo: a chave de dedup do `ReceiveProposal` (Redis setada antes do Insert e não revertida), descrita no documento do incidente, é correção do `service`.

## Pendente: fase de testes

- [ ] **Validar que o tenant-id passado pelo context não se perdeu.** Isso é o coração do goframework: se o tenant parar de ser repassado via context, o sistema todo quebra. Conferir após as mudanças de shutdown:
  - o fluxo HTTP: `AddTenant` em `tlGoFramework.go` grava `X-Tenant-Id` no header, e `GetContextHeader` em `helper.go` o lê;
  - o repositório: as chamadas `GetContextHeader(ctx, XTENANTID, TTENANTID)` em `repository.go`;
  - o consumer Kafka: o `ConsumerContext` e o contexto sem cancelamento no processamento, onde o tenant precisa continuar disponível;
  - o producer Kafka: a propagação do `X-Tenant-Id` nos headers em `kafka_producer.go`.
  - Atenção especial: o processamento Kafka usa `context.WithoutCancel`, não `context.Background()` puro. Teste obrigatório: publicar com tenant X, consumir durante e depois do shutdown, e confirmar que o repositório recebe o tenant X; e que mensagens sem header de tenant não passam a ser tratadas como outro tenant.
