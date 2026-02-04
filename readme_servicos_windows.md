# Execução do STTP como Serviço no Windows (NSSM)

## Objetivo

Este documento descreve como executar os componentes do sistema STTP como **serviços do Windows**, garantindo que:

- a aplicação **continue rodando após logoff do usuário**
- os processos **iniciem automaticamente com o Windows**
- haja **reinício automático em caso de falha**
- o status possa ser verificado por O&M de forma simples

Os serviços tratados aqui são:

- `OH2Writer` → responsável pela escrita no openHistorian 2  
- `SttpStats` → responsável pelo processamento/estatísticas STTP  
  - **dependente do `OH2Writer`**

---

## Por que executar como serviço?

Executar aplicações via console (PowerShell, CMD ou RDP) **não é adequado para ambiente de servidor**, pois:

- ao deslogar do Windows, a sessão do usuário é encerrada
- processos associados à sessão podem ser finalizados automaticamente
- não há política nativa de restart em falha

Ao executar como **Windows Service**:

- o processo roda fora da sessão do usuário
- o Windows passa a gerenciar o ciclo de vida da aplicação
- é possível configurar dependências, restart automático e monitoramento

Isso é o padrão esperado para **ambientes de produção e operação contínua (O&M)**.

---

## Ferramenta utilizada: NSSM

O **NSSM (Non-Sucking Service Manager)** é uma ferramenta leve que permite registrar **qualquer executável** como serviço do Windows, sem necessidade de alterar o código-fonte.

### Por que NSSM?

- Não exige refatoração do Python
- Funciona com executáveis já empacotados (PyInstaller)
- Permite captura de logs (stdout / stderr)
- Muito usado em ambientes industriais e corporativos

---

## Estrutura de diretórios esperada

Exemplo real do projeto:

```
D:\Scripts\sttp_stat\
├── nssm-2.24\
│   └── win64\
│       └── nssm.exe
├── oh2_writer\
│   ├── oh2_writer.exe
│   └── logs\
└── sttp_listener\
    ├── sttp_stats.exe
    ├── config.yaml
    └── logs\
```

> ⚠️ Importante  
> O `config.yaml` deve estar no **mesmo diretório de execução** do `sttp_stats.exe`.

---

## Pré-requisitos

1. Windows com permissões de administrador
2. Executáveis **já testados manualmente** (rodando via duplo clique ou PowerShell)
3. Download do NSSM

### Download do NSSM

- Site oficial: https://nssm.cc/download
- Baixar a versão **Win64**
- Extrair o `nssm.exe` (não é necessário instalar)

---

## Scripts PowerShell

Foram fornecidos dois scripts PowerShell:

- `install_oh2_writer.ps1`
- `install_sttp_stats.ps1`

Esses scripts:

- criam os serviços
- configuram diretório de execução
- habilitam logs
- configuram restart automático
- definem dependência entre serviços

---

## Como executar os scripts

### 1. Abrir PowerShell como Administrador

Isso é obrigatório para criação de serviços.

### 2. Executar os scripts

```powershell
cd D:\Scripts\sttp_stat\_install
.\install_oh2_writer.ps1
.\install_sttp_stats.ps1
```

> Ordem importante:  
> O serviço `OH2Writer` **deve ser instalado primeiro**, pois o `SttpStats` depende dele.

---

## Dependência entre serviços

O serviço `SttpStats` é configurado com dependência do `OH2Writer`.

Isso significa que:

- o Windows **não iniciará** o `SttpStats` se o `OH2Writer` não estiver em execução
- durante o boot, o `OH2Writer` sobe primeiro

> Observação:  
> A dependência garante que o serviço esteja “Started”,  
> **não garante** que a aplicação já esteja pronta (porta aberta).  
> Em caso de falha de conexão inicial, o restart automático resolve.

---

## Logs

Cada serviço possui logs próprios:

### OH2Writer

```
D:\Scripts\sttp_stat\oh2_writer\logs\
├── stdout.log
└── stderr.log
```

### SttpStats

```
D:\Scripts\sttp_stat\sttp_listener\logs\
├── stdout.log
└── stderr.log
```

Esses arquivos devem ser o **primeiro ponto de verificação em caso de falha**.

---

## Verificação de status (O&M)

### Ver status via linha de comando

```powershell
sc query OH2Writer
sc query SttpStats
```

Estados comuns:

- `RUNNING` → serviço ativo
- `STOPPED` → serviço parado
- `START_PENDING` → inicializando

---

### Ver status via interface gráfica

1. Pressione `Win + R`
2. Digite `services.msc`
3. Localize:
   - **OH2Writer**
   - **SttpStats**

A partir daí é possível:

- iniciar
- parar
- reiniciar
- verificar dependências

---

## Reinício automático em falhas

Ambos os serviços são configurados para:

- reiniciar automaticamente em qualquer falha
- sem limite de tentativas

Isso reduz intervenção manual em caso de:

- queda de rede
- falha temporária de dependências
- exceções inesperadas

---

## Como parar ou remover os serviços

### Parar

```powershell
net stop OH2Writer
net stop SttpStats
```

### Remover completamente

```powershell
D:\Scripts\sttp_stat\nssm-2.24\win64\nssm.exe remove OH2Writer confirm
D:\Scripts\sttp_stat\nssm-2.24\win64\nssm.exe remove SttpStats confirm
```

---

## Responsabilidade de O&M

Em operação normal, O&M deve:

- verificar se os serviços estão `RUNNING`
- monitorar crescimento dos logs
- validar comunicação com o openHistorian 2
- reiniciar serviços em caso de manutenção planejada

---

## Conclusão

A execução via serviços Windows usando NSSM:

- elimina dependência de sessões de usuário
- aumenta confiabilidade operacional
- segue boas práticas de ambientes produtivos

Este procedimento é **recomendado para todos os módulos STTP** que necessitam operação contínua.

