# ETL Manager V2

Gerenciador de processos ETL estilo Docker para sincronização de dados de bancos MySQL/MariaDB `argus_*` para Supabase PostgreSQL.

## Visão Geral

O ETL Manager V2 é uma ferramenta que automatiza a sincronização de tabelas de uma origem MySQL/MariaDB para um destino Supabase PostgreSQL. Ele funciona como um daemon que executa sincronizações em intervalos configuráveis.

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
- MySQL ou MariaDB para a origem
- Banco PostgreSQL no Supabase para o destino
- Acesso aos dois bancos:
  - **Source (origem)**: Banco com prefixo `argus_*`
  - **Target (destino)**: Banco Supabase PostgreSQL existente, com schema configurável

---

## Instalação

### 1. Clone ou extraia o projeto

```bash
cd /caminho/para/etl-manager-v2
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

Edite o arquivo `.env` e preencha com os dados da origem MySQL/MariaDB e do destino Supabase PostgreSQL:

```env
# Banco de dados de ORIGEM (argus)
SOURCE_DB_HOST=seu-host-argus.amazonaws.com
SOURCE_DB_USER=seu_usuario
SOURCE_DB_PASSWORD=sua_senha

# Banco de dados de DESTINO (Supabase PostgreSQL)
TARGET_DB_HOST=db.seu-projeto.supabase.co
TARGET_DB_PORT=5432
TARGET_DB_NAME=postgres
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=sua_senha_supabase
TARGET_DB_SCHEMA=public
TARGET_DB_SSLMODE=require
```

### Alternativa: Variáveis de Ambiente

Você também pode definir as variáveis diretamente no terminal:

**Linux/macOS:**
```bash
export SOURCE_DB_HOST="seu-host-argus.amazonaws.com"
export SOURCE_DB_USER="seu_usuario"
export SOURCE_DB_PASSWORD="sua_senha"
export TARGET_DB_HOST="db.seu-projeto.supabase.co"
export TARGET_DB_PORT="5432"
export TARGET_DB_NAME="postgres"
export TARGET_DB_USER="postgres"
export TARGET_DB_PASSWORD="sua_senha_supabase"
export TARGET_DB_SCHEMA="public"
export TARGET_DB_SSLMODE="require"
```

**Windows (PowerShell):**
```powershell
$env:SOURCE_DB_HOST="seu-host-argus.amazonaws.com"
$env:SOURCE_DB_USER="seu_usuario"
$env:SOURCE_DB_PASSWORD="sua_senha"
$env:TARGET_DB_HOST="db.seu-projeto.supabase.co"
$env:TARGET_DB_PORT="5432"
$env:TARGET_DB_NAME="postgres"
$env:TARGET_DB_USER="postgres"
$env:TARGET_DB_PASSWORD="sua_senha_supabase"
$env:TARGET_DB_SCHEMA="public"
$env:TARGET_DB_SSLMODE="require"
```

---

## Uso

### Verificar instalação

```bash
etl-manager-v2 --help
```

---

### Comandos Principais

#### 1. Adicionar um novo ETL

Registra um novo processo de sincronização:

```bash
# Sintaxe básica
etl-manager-v2 add <argus_id> --interval <minutos>

# Exemplos
etl-manager-v2 add 110760000549 --interval 60
etl-manager-v2 add 110760000088 --interval 30
etl-manager-v2 add argus_110760000088 --interval 60
```

Parâmetros:
- `argus_id`: ID do processo Argus (pode usar com ou sem prefixo `argus_`)
- `--interval`: Intervalo de execução em minutos (padrão: 60)
- `--source-host`: Sobrescreve SOURCE_DB_HOST para este ETL específico
- `--target-host`: Sobrescreve TARGET_DB_HOST do Supabase PostgreSQL para este ETL específico
- `--force`: Sobrescreve se já existir

---

#### 2. Listar todos os ETLs

```bash
etl-manager-v2 ps
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
etl-manager-v2 daemon start
```

Verificar status:
```bash
etl-manager-v2 daemon status
```

Parar o daemon:
```bash
etl-manager-v2 daemon stop
```

---

#### 4. Executar ETL imediatamente

Executa um ETL imediatamente (sem esperar o próximo ciclo):

```bash
etl-manager-v2 run <argus_id>

# Exemplo
etl-manager-v2 run 110760000549
```

---

#### 5. Ver logs

```bash
# Últimas 30 linhas de um ETL específico
etl-manager-v2 logs 110760000549

# Últimas 100 linhas
etl-manager-v2 logs 110760000549 --tail 100

# Todos os ETLs
etl-manager-v2 logs --all
```

---

#### 6. Controlar ETLs individualmente

```bash
# Pausar um ETL (não remove)
etl-manager-v2 stop 110760000549

# Reativar um ETL pausado
etl-manager-v2 enable 110760000549

# Forçar execução no próximo ciclo
etl-manager-v2 restart 110760000549

# Alterar intervalo
etl-manager-v2 interval 110760000549 30
```

---

#### 7. Inspecionar detalhes

Mostra todos os detalhes de um ETL específico:

```bash
etl-manager-v2 inspect 110760000549
```

---

#### 8. Remover um ETL

Remove um ETL do registro:

```bash
etl-manager-v2 rm 110760000549
```

---

## Fluxo Típico de Uso

```bash
# 1. Instalar (uma vez)
pip install -e .

# 2. Configurar credenciais (edite o arquivo .env)
nano .env

# 3. Registrar ETLs
etl-manager-v2 add 110760000088 --interval 60
etl-manager-v2 add 110760000549 --interval 30

# 4. Iniciar o daemon
etl-manager-v2 daemon start

# 5. Monitorar
etl-manager-v2 ps
etl-manager-v2 logs 110760000088

# Verificar se o daemon está rodando
etl-manager-v2 daemon status
```

---

## Validação com Supabase de Teste

Antes de iniciar o daemon em produção, valide um ETL em foreground contra um banco Supabase não produtivo:

```bash
# 1. Configure .env com SOURCE_DB_* e TARGET_DB_* de teste
nano .env

# 2. Registre ou atualize um ETL
etl-manager-v2 add 110760000088 --interval 60 --force

# 3. Execute uma sincronização manual
etl-manager-v2 run 110760000088

# 4. Verifique logs e tabelas no schema configurado
etl-manager-v2 logs 110760000088 --tail 100
```

Confirme no Supabase que as tabelas foram criadas no schema `TARGET_DB_SCHEMA` e que os registros esperados foram inseridos antes de iniciar `etl-manager-v2 daemon start`.

---

## Estrutura de Arquivos

```
etl-manager-v2/
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

### Erro de conexão com banco de dados

Erros como "Can't connect to MySQL server" ou falhas de conexão PostgreSQL indicam que as variáveis de ambiente não foram configuradas corretamente. Verifique:

1. O arquivo `.env` existe e está preenchido
2. As variáveis estão exportadas no terminal atual
3. As credenciais estão corretas

### Daemon não inicia

```bash
# Verifique os logs
etl-manager-v2 logs --all

# Verifique se há outro processo
etl-manager-v2 daemon status
```

### Verificar banco de origem MySQL

Para testar a conexão da origem manualmente:

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

### Verificar banco de destino Supabase PostgreSQL

Para testar a conexão do destino manualmente:

```python
import psycopg

conn = psycopg.connect(
    host="db.seu-projeto.supabase.co",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="sua_senha_supabase",
    sslmode="require",
)
print("Conexão OK!")
conn.close()
```

---

## Licença

MIT License
