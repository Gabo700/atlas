# Atlas DataFlow

**Atlas DataFlow** é uma solução completa para integração, consolidação e visualização dos dados provenientes da API do sistema **Jueri**.  
O projeto permite centralizar informações de clientes, produtos, pedidos, vendas e financeiro em um único ambiente seguro, com suporte a Business Intelligence (BI) e análises avançadas.

---

## Sumário

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Funcionalidades](#-funcionalidades)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Instalação e Execução](#-instalação-e-execução)
- [Banco de Dados](#-banco-de-dados)
- [Segurança e Autenticação](#-segurança-e-autenticação)
- [Portal Web e BI](#-portal-web-e-bi)
- [Ambiente de Produção](#-ambiente-de-produção)
- [Licença](#-licença)
- [Autor](#-autor)

---

## Visão Geral

O objetivo do projeto é **integrar o consumo de dados da API Jueri** em um **portal centralizado**, permitindo:
- Armazenamento estruturado em banco de dados PostgreSQL;  
- Atualização automática dos dados (via ETL e filas de processamento);  
- Disponibilização para análise em ferramentas de BI;  
- Acesso via portal seguro com autenticação.

---

## Arquitetura

[API Jueri]
↓ (paginação e coleta incremental)
[ETL Python Backend - FastAPI + Celery]
↓
[PostgreSQL Database]
↓
[Portal Web / BI Dashboard]


- **Backend (ETL):** Responsável por extrair, transformar e carregar os dados.  
- **Banco de Dados:** Armazena e indexa os dados consolidados.  
- **Portal:** Interface web para visualização e relatórios.  
- **BI:** Integração com Metabase / Power BI para análises.

---

## Funcionalidades

-  Conector automático com API Jueri (`/cliente`, `/produto`, `/pedido`, `/venda`, `/financeiro/...`)
-  Coleta de dados paginada e incremental
-  Armazenamento relacional (PostgreSQL)
-  ETL agendado com filas assíncronas
-  Autenticação e controle de acesso ao portal
-  Integração com Metabase / Power BI
-  Logs e monitoramento de tarefas

---

## Tecnologias Utilizadas

| Camada | Ferramenta |
|--------|-------------|
| Linguagem principal | **Python 3.11+** |
| Backend API | **FastAPI** |
| Banco de Dados | **PostgreSQL** |
| ORM | **SQLAlchemy** |
| Filas / Agendamentos | **Celery + Redis** |
| BI | **Metabase (self-hosted)** |
| Infraestrutura | **Docker / VM Linux (Ubuntu)** |

---

## Instalação e Execução

### 1. Clonar o repositório
```bash
git clone https://github.com/seuusuario/jueri-data-hub.git
cd jueri-data-hub

API_BASE_URL=https://jueri.com.br/sis/api/v1/1201
API_TOKEN=seu_token_aqui
DB_URL=postgresql+psycopg2://usuario:senha@localhost:5432/jueri
REDIS_URL=redis://localhost:6379/0


Autor

Gabriel Ribeiro
Desenvolvedor
contato: gabrielribeirosilvasr@protonmail.com


