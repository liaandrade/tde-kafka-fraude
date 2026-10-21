# 🚨 Detecção de Fraude em Tempo Real com Apache Kafka (TDE 03)

Simulação de um sistema de detecção de fraude em transações financeiras usando **Apache Kafka**, **Python** e **SQLite**.

## 📐 Arquitetura

```
┌────────────┐   JSON    ┌─────────────────────┐   JSON    ┌───────────────────────────┐
│  PRODUCER  │ ───────▶  │  KAFKA (tópico      │ ───────▶  │         CONSUMER          │
│ producer.py│  a cada   │  "transacoes")      │           │ consumer.py               │
│            │  3 seg    │  rodando no Docker  │           │  1. salva no SQLite       │
└────────────┘           └─────────────────────┘           │  2. aplica regras de fraude│
                                                           │  3. alerta colorido       │
                                                           └───────────────────────────┘
```

**Conceitos do Kafka usados (resumo para iniciantes):**

| Conceito | O que é | No nosso projeto |
|---|---|---|
| **Broker** | Servidor do Kafka que guarda as mensagens | Container Docker `kafka` |
| **Tópico** | "Canal" onde as mensagens são publicadas | `transacoes` |
| **Producer** | Quem publica mensagens no tópico | `producer.py` |
| **Consumer** | Quem lê mensagens do tópico | `consumer.py` |
| **Consumer Group** | Grupo que divide/lembra a leitura (offset) | `detector-fraude` |
| **Offset** | Posição da mensagem no tópico | Impresso pelo producer |

## 🔎 Regras de fraude

Todas olham o **histórico do mesmo cliente** (consultado no SQLite):

| Alerta | Regra | Cor |
|---|---|---|
| `ALTO_VALOR` | `valor >= 10.000` | 🔴 Vermelho |
| `TEMPO_60s` | 4 (ou mais) transações do mesmo cliente em menos de 60 segundos | 🟡 Amarelo |
| `GEO_10m` | Transações do mesmo cliente em **cidades diferentes** dentro de 10 minutos | 🟣 Magenta |

Uma mesma transação pode disparar mais de um alerta. **Todas** as transações são salvas no banco, com ou sem fraude.

## 📄 Formato da transação

```json
{
  "id_transacao": "3f2b8c1e-....",
  "id_cliente": "C001",
  "valor": 1523.90,
  "timestamp": "2026-10-21T14:30:05+00:00",
  "cidade": "São Paulo"
}
```

## 📁 Estrutura do projeto

```
fraude-kafka/
├── docker-compose.yml   # sobe o Kafka
├── config.py            # configurações (servidor, tópico, limites das regras)
├── producer.py          # gera e envia transações
├── consumer.py          # lê, salva no banco e aplica as regras
├── consultar_banco.py   # (opcional) mostra o conteúdo do SQLite
├── requirements.txt     # dependências Python
└── README.md
```

---

## ✅ Pré-requisitos

- **Docker Desktop** (Windows/Mac) ou Docker Engine + Compose (Linux) — <https://docs.docker.com/get-docker/>
- **Python 3.10 ou superior** — <https://www.python.org/downloads/>
- **Git**

Confira no terminal:

```bash
docker --version
docker compose version
python --version      # no Linux/Mac pode ser: python3 --version
```

## 🚀 Passo a passo

> Você vai usar **3 terminais** abertos dentro da pasta do projeto: um para o Docker (pode ser fechado depois de subir), um para o Consumer e um para o Producer.

### 1️⃣ Criar o ambiente virtual e instalar dependências

```bash
# Criar o ambiente virtual (isola as bibliotecas deste projeto)
python -m venv venv

# Ativar:
#   Windows (PowerShell):   .\venv\Scripts\Activate.ps1
#   Windows (CMD):          venv\Scripts\activate.bat
#   Linux / Mac:            source venv/bin/activate

# Instalar as dependências
pip install -r requirements.txt
```

> 💡 No PowerShell, se aparecer erro de permissão ao ativar, rode uma vez:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

Se ativou certo, aparece `(venv)` no início da linha do terminal.

### 2️⃣ Subir o Kafka com Docker

Abra o **Docker Desktop** (precisa estar aberto/rodando) e execute:

```bash
docker compose up -d
```

Na primeira vez ele baixa a imagem (alguns minutos). Verifique:

```bash
docker compose ps                  # STATUS deve estar "running"
docker compose logs kafka | tail   # deve conter "Kafka Server started"
```

Aguarde uns **15–20 segundos** após subir antes de continuar.

### 3️⃣ Rodar o Consumer (Terminal 1)

```bash
python consumer.py
```

Saída esperada: `✅ Consumer conectado. Aguardando mensagens...` — ele fica parado esperando. Deixe aberto.

### 4️⃣ Rodar o Producer (Terminal 2)

Abra **outro** terminal na pasta do projeto, **ative o venv de novo** (passo 1) e execute:

```bash
python producer.py
```

Você verá uma transação enviada a cada 3 segundos, e no terminal do Consumer aparecerão as transações com os alertas coloridos:

```
✔ OK      | cliente=C003 valor=R$    312,40 cidade=Fortaleza       2026-10-21T14:30:05+00:00
🚨 SUSPEITA | cliente=C001 valor=R$ 14.220,10 cidade=São Paulo     2026-10-21T14:30:08+00:00
   ⚠ ALERTA ALTO_VALOR Valor R$ 14,220.10 >= R$ 10,000.00
```

### 5️⃣ Conferir o banco de dados

Com o consumer rodando (ou depois de parar com `Ctrl+C`):

```bash
python consultar_banco.py
```

Ou abra o arquivo `transacoes.db` no [DB Browser for SQLite](https://sqlitebrowser.org/) ou use:

```bash
sqlite3 transacoes.db "SELECT * FROM transacoes ORDER BY timestamp DESC LIMIT 10;"
```

### 6️⃣ Parar tudo

```bash
# Ctrl+C no Producer e no Consumer, depois:
docker compose down
```

Para também apagar as mensagens guardadas no Kafka, o `docker compose down` já remove o container (e as mensagens, pois não usamos volume). Para zerar o banco, apague o arquivo `transacoes.db`.

---

## 🧪 Como testar / demonstrar as regras

As regras disparam por acaso, então para a apresentação ajuda acelerar:

```bash
python producer.py --intervalo 1     # 1 transação por segundo
```

- **ALTO_VALOR** aparece com frequência (cerca de 8% das transações são ≥ R$ 10.000).
- **TEMPO_60s** aparece quando um mesmo cliente recebe 4 transações em 60s (com só 5 clientes isso acontece naturalmente, mais rápido com `--intervalo 1`).
- **GEO_10m** aparece quando um cliente (que compra quase sempre na sua cidade de origem) faz uma compra em outra cidade (15% de chance) logo após outra transação.

> O requisito do trabalho é 3 segundos — rode sem `--intervalo` na versão "oficial" e use `--intervalo 1` só para acelerar a demonstração.

## 🛠️ Problemas comuns

| Problema | Solução |
|---|---|
| `❌ Não consegui conectar no Kafka` | Docker Desktop fechado ou Kafka ainda iniciando. Rode `docker compose ps` e espere ~20s. |
| `port is already allocated` (porta 9092) | Outro Kafka rodando. Pare-o ou execute `docker compose down`. |
| `ModuleNotFoundError: kafka` | Venv não ativado, ou esqueceu o `pip install -r requirements.txt`. |
| `TypeError` em `str \| None` | Python antigo. Use Python 3.10+. |
| Consumer não mostra nada | Producer não está rodando, ou o consumer foi iniciado em outro `group_id`. Veja `docker compose logs kafka`. |
| Cores estranhas (`←[31m`) no terminal | Use PowerShell/Windows Terminal; o `colorama` já está no `requirements.txt`. |
| Quero recomeçar do zero | `docker compose down`, apague `transacoes.db`, `docker compose up -d`. |

## 🧠 Como as regras foram implementadas (resumo)

1. O Consumer recebe a transação e **a salva primeiro** no SQLite.
2. Em seguida roda as regras, que fazem consultas SQL no histórico do cliente (`WHERE id_cliente = ?`) em janelas de tempo:
   - **TEMPO_60s**: `COUNT(*)` das transações do cliente nos 60s anteriores (inclui a atual) `>= 4`.
   - **GEO_10m**: busca transação do cliente nos 10 min anteriores com `cidade <> cidade_atual`.
   - **ALTO_VALOR**: comparação simples com o valor da própria transação.
3. Cada alerta é impresso colorido e salvo na tabela `alertas`.

O Producer usa o `id_cliente` como **chave** da mensagem, o que garante que as transações do mesmo cliente sejam processadas na ordem em que foram enviadas.
