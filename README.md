# STTP Latency Calculator

Ferramenta em Python para **cálculo de latência de dados STTP (C37.118)**, com:
- processamento concorrente por janelas temporais,
- estatísticas agregadas por PPA,
- publicação opcional **tick-a-tick** via HTTP,
- arquitetura enxuta (core.py / infra),
- suporte a execução como **script Python** ou **executável (PyInstaller)**.

---

## 📌 Visão Geral

O sistema se conecta a um **servidor STTP**, recebe medições em tempo real e calcula a latência como:

latência (ms) = tempo_de_chegada - tempo_da_medida

markdown
Copiar código

A aplicação produz dois tipos de saída:

1. **Relatório por janela de tempo**
   - Estatísticas agregadas (média, máximo, último valor)
   - Top-N maiores latências
   - Métricas de fila e descarte

2. **Publicação tick-a-tick (opcional)**
   - Envio HTTP de latência individual por PPA
   - Mapeamento flexível de PPA origem → PPA destino

---

##  Arquitetura

STTP Server
↓
SttpSubscriber (infra)
↓
LatencyPipeline (core.py)
├─ ShardedWindowProcessor (core.py)
├─ ReportSink (infra)
└─ TickSink (HTTP) (infra)

yaml
Copiar código

### Separação de camadas

| Camada  | Responsabilidade |
|-------|------------------|
| `core.py` | Regras de negócio, modelos, pipeline e concorrência |
| `infra/` | STTP, HTTP, relógio, sinks, mapeamentos |
| `main.py` | Composition Root (injeção de dependências) |

> O núcleo (**core.py**) **não depende** de STTP, HTTP ou threads.

---

##  Requisitos

### Execução como script Python
- Python **3.10+**
- Dependências:
  - `httpx`
  - `pyyaml`
  - biblioteca STTP (`sttp` / `gsf`, conforme ambiente)

### Execução como executável
- Windows (PyInstaller)
- Nenhum Python instalado no destino

---

##  Build do Executável (Windows)

### Criar o executável

powershell
pyinstaller --onefile --name sttp_latency_calculator main.py

2. Copiar o arquivo de configuração
powershell
Copiar código
Copy-Item .\config.yaml .\dist\config.yaml -Force
Estrutura final esperada

Copiar código
dist/
 ├─ sttp_latency_calculator.exe
 └─ config.yaml
 O executável não embute o config.yaml.
O usuário deve editar esse arquivo manualmente.

### Execução
Execução direta (Python)
bash
Copiar código
python main.py
Execução via executável
bash
Copiar código
sttp_latency_calculator.exe
O programa procura o arquivo de configuração na seguinte ordem:

Caminho definido em CONFIG_PATH

Diretório atual

Diretório do executável

### Configuração (config.yaml)
Exemplo completo
yaml
Copiar código
hostname: "127.0.0.1"
port: 7165

window_sec: 5.0
top_n: 10
shards: 8
queue_size: 5000

tick_write:
  url: "http://localhost:8000/write"
  server_ip: "SERVER01"
  workers: 4
  queue_max: 5000
  timeout_sec: 2.0
  max_retries: 3
  drop_on_full: true

  ### Mapeamento: PPA origem (STTP) → PPA destino (salvamento)
  ppa_map:
    2397: 1002397
    2401: 1002401
Campos principais
Campo	Descrição
hostname	Endereço do servidor STTP
port	Porta STTP (1–65535)
window_sec	Tamanho da janela de agregação
top_n	Quantidade de PPAs no relatório
shards	Número de shards (concorrência)
queue_size	Fila por shard

tick_write
Controla a publicação tick-a-tick via HTTP.

Campo	Função
url	Endpoint HTTP
workers	Threads de envio
queue_max	Tamanho da fila
drop_on_full	Descarta se fila cheia
ppa_map	Mapeamento src → dst

Se um PPA não estiver no ppa_map, não será publicado via HTTP.

## Saídas do Sistema
1. Relatório por janela (stdout)
Exemplo:

yaml
Copiar código
[2026-01-10 18:00:05.123456] window=5.000s
total_enqueued=12000 total_processed=11980 backlog=20 dropped=5
batch=240 shards=8

TOP window max latency (ms):
 key   | count | mean   | max    | last   | dropped
 2397  |  1200 |  3.214 | 12.873 |  2.912 | 0
2. Publicação HTTP (tick-a-tick)
Payload enviado:

json
Copiar código
{
  "server_ip": "SERVER01",
  "tempo": "2026-01-10 18:00:01.123",
  "ppa": 1002397,
  "indicator": 3.214
}
tempo sempre em UTC

indicator em milissegundos

## Decisões de Projeto Importantes
Fail-fast: configuração inválida encerra a aplicação.

Sem defaults perigosos: tudo deve ser explicitado.

Concorrência por shards: evita lock global.

Backpressure explícito: descarte controlado.

Domínio isolado: fácil de testar e evoluir.

## Testes e Validação
Recomendado testar:

Latência com relógio desalinhado

Saturação de fila (queue_size pequeno)

STTP com bursts grandes

Endpoint HTTP indisponível (retry)

## Troubleshooting
Programa não encontra config.yaml
Verifique diretório atual

Ou defina:

bash
Copiar código
set CONFIG_PATH=C:\caminho\config.yaml
Latência negativa
Verifique sincronismo de relógio (NTP)

STTP e host devem estar no mesmo referencial UTC

HTTP não recebe dados
Confirme ppa_map

Verifique drop_on_full

Cheque métricas internas (total_failed, total_dropped)

## Licença
LabPlan 2026
Projeto interno / técnico.
