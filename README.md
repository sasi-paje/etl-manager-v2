# ETL Manager

Gerenciador de processos ETL estilo Docker para sincronização de dados entre bancos de dados `argus_*` → `webapp_*`.

## Visão Geral

O ETL Manager é uma ferramenta que automatiza a sincronização de tabelas entre bancos de dados MySQL/MariaDB. Ele funciona como um daemon que executa sincronizações em intervalos configuráveis.

### Tabelas Sincronizadas

O processo sincroniza as seguintes tabelas:
- `alerts`
- `channels`
- `channel_status`
- `flow`
- `flow_states`
- `sites`
- `clients`
- `groups`
- `maps`

---

## Pré-requisitos

- Python 3.11 ou superior
- MySQL ou MariaDB
- Acesso a dois bancos de dados MySQL:
  - **Source (origem)**: Banco com prefixo `argus_*`
  - **Target (destino)**: Banco com prefixo `webapp_*`

---

## Instalação

### 1. Clone ou extraia o projeto

```bash
cd /caminho/para/etl-manager
```

### 2. Crie um ambiente virtual (recomendado)

**Linux/macOS:**
```bash
python -m venv .venv
source .venv/bin/activate
```

**Windows:**
```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Instale as dependências

```bash
pip install -e .
```

---

## Configuração

### 1. Copie o arquivo de exemplo

```bash
cp .env.example .env
```

### 2. Edite o arquivo `.env` com suas credenciais

Edite o arquivo `.env` e preencha com os dados dos seus bancos:

```env
# Banco de dados de ORIGEM (argus)
SOURCE_DB_HOST=seu-host-argus.amazonaws.com
SOURCE_DB_USER=seu_usuario
SOURCE_DB_PASSWORD=sua_senha

# Banco de dados de DESTINO (webapp)
TARGET_DB_HOST=seu-host-webapp.amazonaws.com
TARGET_DB_USER=seu_usuario
TARGET_DB_PASSWORD=sua_senha
```

### Alternativa: Variáveis de Ambiente

Você também pode definir as variáveis diretamente no terminal:

**Linux/macOS:**
```bash
export SOURCE_DB_HOST="seu-host-argus.amazonaws.com"
export SOURCE_DB_USER="seu_usuario"
export SOURCE_DB_PASSWORD="sua_senha"
export TARGET_DB_HOST="seu-host-webapp.amazonaws.com"
export TARGET_DB_USER="seu_usuario"
export TARGET_DB_PASSWORD="sua_senha"
```

**Windows (PowerShell):**
```powershell
$env:SOURCE_DB_HOST="seu-host-argus.amazonaws.com"
$env:SOURCE_DB_USER="seu_usuario"
$env:SOURCE_DB_PASSWORD="sua_senha"
$env:TARGET_DB_HOST="seu-host-webapp.amazonaws.com"
$env:TARGET_DB_USER="seu_usuario"
$env:TARGET_DB_PASSWORD="sua_senha"
```

---

## Uso

### Verificar instalação

```bash
etl-manager --help
```

---

### Comandos Principais

#### 1. Adicionar um novo ETL

Registra um novo processo de sincronização:

```bash
# Sintaxe básica
etl-manager add <argus_id> --interval <minutos>

# Exemplos
etl-manager add 110760000549 --interval 60
etl-manager add 110760000088 --interval 30
etl-manager add argus_110760000088 --interval 60
```

Parâmetros:
- `argus_id`: ID do processo Argus (pode usar com ou sem prefixo `argus_`)
- `--interval`: Intervalo de execução em minutos (padrão: 60)
- `--source-host`: Sobrescreve SOURCE_DB_HOST para este ETL específico
- `--target-host`: Sobrescreve TARGET_DB_HOST para este ETL específico
- `--force`: Sobrescreve se já existir

---

#### 2. Listar todos os ETLs

```bash
etl-manager ps
```

Saída exemplo:
```
ARGUS ID             STATUS               INTERVAL   LAST RUN       LAST STATUS 
--------------------------------------------------------------------------------
110760000088         * running               60min   5m ago         success
110760000549         - stopped               30min   2h ago         success
```

---

#### 3. Iniciar o daemon

O daemon executa em background e gerencia todos os ETLs registrados:

```bash
etl-manager daemon start
```

Verificar status:
```bash
etl-manager daemon status
```

Parar o daemon:
```bash
etl-manager daemon stop
```

---

#### 4. Executar ETL imediatamente

Executa um ETL imediatamente (sem esperar o próximo ciclo):

```bash
etl-manager run <argus_id>

# Exemplo
etl-manager run 110760000549
```

---

#### 5. Ver logs

```bash
# Últimas 30 linhas de um ETL específico
etl-manager logs 110760000549

# Últimas 100 linhas
etl-manager logs 110760000549 --tail 100

# Todos os ETLs
etl-manager logs --all
```

---

#### 6. Controlar ETLs individualmente

```bash
# Pausar um ETL (não remove)
etl-manager stop 110760000549

# Reativar um ETL pausado
etl-manager enable 110760000549

# Forçar execução no próximo ciclo
etl-manager restart 110760000549

# Alterar intervalo
etl-manager interval 110760000549 30
```

---

#### 7. Inspecionar detalhes

Mostra todos os detalhes de um ETL específico:

```bash
etl-manager inspect 110760000549
```

---

#### 8. Remover um ETL

Remove um ETL do registro:

```bash
etl-manager rm 110760000549
```

---

## Fluxo Típico de Uso

```bash
# 1. Instalar (uma vez)
pip install -e .

# 2. Configurar credenciais (edite o arquivo .env)
nano .env

# 3. Registrar ETLs
etl-manager add 110760000088 --interval 60
etl-manager add 110760000549 --interval 30

# 4. Iniciar o daemon
etl-manager daemon start

# 5. Monitorar
etl-manager ps
etl-manager logs 110760000088

# Verificar se o daemon está rodando
etl-manager daemon status
```

---

## Estrutura de Arquivos

```
etl-manager/
├── etl_manager/           # Pacote principal
│   ├── __init__.py
│   ├── cli.py              # Interface de linha de comando
│   ├── daemon.py          # Processo em background
│   ├── runner.py          # Motor de sincronização
│   └── state.py           # Gerenciamento de estado
├── logs/                   # Logs do daemon
│   └── daemon.log
├── state.json              # Estado dos ETLs registrados
├── daemon.pid             # PID do daemon em execução
├── .env                    # Variáveis de ambiente (não versionado)
├── .env.example            # Exemplo de variáveis de ambiente
└── pyproject.toml         # Configuração do projeto
```

---

## Solução de Problemas

### "Can't connect to MySQL server on 'localhost'"

Este erro indica que as variáveis de ambiente não foram configuradas corretamente. Verifique:

1. O arquivo `.env` existe e está preenchido
2. As variáveis estão exportadas no terminal atual
3. As credenciais estão corretas

### Daemon não inicia

```bash
# Verifique os logs
etl-manager logs --all

# Verifique se há outro processo
etl-manager daemon status
```

### Verificar banco de dados

Para testar a conexão manualmente:

```python
import pymysql

conn = pymysql.connect(
    host="seu-host",
    user="seu-usuario",
    password="sua-senha",
    database="argus_110760000088"
)
print("Conexão OK!")
conn.close()
```

---

## Licença

MIT License
