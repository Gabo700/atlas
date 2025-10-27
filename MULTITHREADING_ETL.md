# Sistema Multithreading ETL - Atlas Data Flow

## 🧵 Arquitetura Multithreading Implementada

### Visão Geral
O sistema agora utiliza **multithreading** para separar a **coleta de dados** da **escrita no banco**, permitindo processamento simultâneo e muito mais eficiente.

## 🔧 Componentes Principais

### 1. ETLWorker (Thread Principal)
- **Responsabilidade**: Coleta de dados da API
- **Funcionalidades**:
  - Faz requisições HTTP para a API
  - Processa respostas paginadas
  - Aplica filtros de data
  - Envia dados para fila thread-safe

### 2. Database Writer (Thread Secundária)
- **Responsabilidade**: Escrita no banco de dados
- **Funcionalidades**:
  - Recebe dados da fila thread-safe
  - Insere em lotes no PostgreSQL
  - Gerencia transações
  - Atualiza contadores thread-safe

### 3. Thread-Safe Queue
- **Tipo**: `queue.Queue(maxsize=1000)`
- **Função**: Comunicação entre threads
- **Características**:
  - Thread-safe por design
  - Buffer de 1000 itens
  - Timeout para evitar travamentos

## 🚀 Fluxo de Execução

```
1. Inicia ETLWorker (Thread Principal)
   ↓
2. Cria tabela raw específica (cliente + rota)
   ↓
3. Inicia Database Writer (Thread Secundária)
   ↓
4. Loop de coleta de dados:
   - Faz requisição HTTP
   - Processa resposta
   - Adiciona dados à fila
   ↓
5. Database Writer (paralelo):
   - Remove dados da fila
   - Insere em lotes no banco
   - Atualiza contadores
   ↓
6. Finalização:
   - Para thread de escrita
   - Aguarda conclusão
   - Atualiza status final
```

## 📊 Benefícios do Multithreading

### Performance
- **Coleta e escrita simultâneas**: Não há espera entre requisições
- **Menor latência**: Dados são escritos imediatamente após coleta
- **Melhor throughput**: Processamento paralelo otimiza recursos

### Robustez
- **Thread-safe**: Uso de locks e queues seguras
- **Tolerância a falhas**: Erro em uma thread não afeta a outra
- **Controle de recursos**: Fila com limite evita sobrecarga de memória

### Monitoramento
- **Contadores thread-safe**: Progresso preciso em tempo real
- **Estatísticas detalhadas**: Taxa de processamento, tempo total
- **Feedback visual**: Interface atualizada em tempo real

## 🔒 Segurança Thread-Safe

### Locks Utilizados
```python
self.lock = threading.Lock()  # Para contadores compartilhados
```

### Queue Thread-Safe
```python
self.data_queue = queue.Queue(maxsize=1000)  # Comunicação entre threads
```

### Padrões de Sincronização
- **Contadores**: Protegidos com locks
- **Comunicação**: Via queue thread-safe
- **Finalização**: Aguarda threads terminarem

## 📈 Estatísticas Melhoradas

O sistema agora exibe:
- **Registros coletados**: Em tempo real
- **Taxa de processamento**: Registros por segundo
- **Tempo estimado**: Baseado na performance atual
- **Status multithreading**: Indicação de processamento paralelo

## 🛠️ Configurações Técnicas

### Tamanhos de Lote
- **Fila**: 1000 itens máximo
- **Batch de inserção**: 50 registros
- **Timeout de queue**: 1 segundo

### Retry Logic
- **Máximo de tentativas**: 3 por página
- **Backoff exponencial**: 2^tentativa + 0.5s
- **Tolerância a falhas**: Continua processamento

### Nomenclatura de Tabelas
```
dados_raw_cliente_{cliente_id}_{nome_rota}
Exemplo: dados_raw_cliente_2151_pedidos
```

## 🎯 Resultados Esperados

### Antes (Sequencial)
- Coleta → Processamento → Escrita
- Tempo total = Tempo coleta + Tempo escrita
- Uso de memória alto (todos os dados em RAM)

### Depois (Multithreading)
- Coleta || Escrita (paralelo)
- Tempo total ≈ Tempo coleta (escrita paralela)
- Uso de memória baixo (fila limitada)

## 🔍 Monitoramento e Debug

### Logs Detalhados
- Erros de cada thread separadamente
- Performance de cada componente
- Status da fila e locks

### Interface Visual
- Progresso em tempo real
- Estatísticas de performance
- Indicação de multithreading ativo

## ⚡ Performance Esperada

Com multithreading, o sistema deve apresentar:
- **2-3x mais rápido** na coleta de grandes volumes
- **Menor uso de memória** (buffer limitado)
- **Melhor responsividade** da interface
- **Maior estabilidade** em longas execuções
