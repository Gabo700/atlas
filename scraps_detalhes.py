import os
from dotenv import load_dotenv
import sys
import time
import psycopg2
import traceback
import json
import requests
import hashlib
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QMessageBox,
    QAbstractItemView,
    QComboBox,
    QDateEdit,
    QProgressDialog,
    QTextEdit,
    QGroupBox,
    QCheckBox
)
from PySide6.QtCore import Qt, QDate, QThread, Signal


# ===============================
#  Funções auxiliares de log
# ===============================
LOG_FILE = "erros_detalhes.log"


def registrar_erro(contexto, erro):
    """Grava o erro no arquivo erros_detalhes.log com data/hora e stacktrace."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n" + "=" * 80 + "\n")
        f.write(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERRO EM: {contexto}\n"
        )
        f.write(str(erro) + "\n")
        f.write(traceback.format_exc())
        f.write("\n" + "=" * 80 + "\n")
    print(f"ERRO ({contexto}): {erro}")


# ===============================
#  Configuração do banco e ETL
# ===============================
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

# ===============================
#  CONFIGURAÇÕES OTIMIZADAS
# ===============================
MAX_WORKERS = 3
BATCH_SIZE = 10
DB_BATCH_SIZE = 100
REQUEST_TIMEOUT = 30
RETRY_MAX = 2
RATE_LIMIT_DELAY = 0.25
DAILY_LIMIT = 10000

# Rate Limiter para detalhes
class RateLimiter:
    def __init__(self, max_per_second=4):
        self.max_per_second = max_per_second
        self.last_request_time = 0
        self.min_interval = 1.0 / max_per_second
        self.lock = threading.Lock()
        self.daily_count = 0
        self.daily_reset_time = self._get_next_reset_time()
    
    def _get_next_reset_time(self):
        """Próximo reset às 00:00"""
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        return datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0)
    
    def check_daily_limit(self):
        """Verifica se atingiu o limite diário"""
        now = datetime.now()
        if now >= self.daily_reset_time:
            self.daily_count = 0
            self.daily_reset_time = self._get_next_reset_time()
        
        return self.daily_count >= DAILY_LIMIT
    
    def wait_if_needed(self):
        """Aguarda se necessário para respeitar o rate limit"""
        with self.lock:
            # Verifica limite diário
            if self.check_daily_limit():
                wait_seconds = (self.daily_reset_time - datetime.now()).total_seconds()
                if wait_seconds > 0:
                    print(f"Limite diário atingido! Aguardando {wait_seconds/3600:.1f}h até reset")
                    time.sleep(min(wait_seconds, 300))
                    return True
                else:
                    self.daily_count = 0
                    self.daily_reset_time = self._get_next_reset_time()
            
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
            self.daily_count += 1
            
            return False

# Instância global do rate limiter
rate_limiter = RateLimiter(max_per_second=4)


# ===============================
#  Thread de Execução do ETL de Detalhes
# ===============================
class DetalhesWorker(QThread):
    progress = Signal(str, int, int, int, int)
    finished = Signal(int, str)
    error = Signal(str)
    
    def __init__(self, tabela_bronze, cliente_id, url_template, headers_template, parent=None):
        super().__init__(parent)
        self.tabela_bronze = tabela_bronze
        self.cliente_id = cliente_id
        self.url_template = url_template
        self.headers_template = headers_template
        self.db_config = DB_CONFIG
        self._stop_flag = False
        self.data_queue = queue.Queue(maxsize=2000)
        self.total_registros = 0
        self.lock = threading.Lock()
        self.writer_thread = None
        self.processed_count = 0
        self.start_time = None
        self.success_count = 0
        self.error_count = 0
        self.rate_limit_count = 0
        self.daily_limit_hit = False
    
    def stop(self):
        self._stop_flag = True
        while not self.data_queue.empty():
            try:
                self.data_queue.get_nowait()
                self.data_queue.task_done()
            except queue.Empty:
                break
    
    def _verificar_tabela_detalhes_raw(self, cur):
        """Cria tabela para armazenar detalhes dos pedidos"""
        nome_tabela = f"dados_detalhes_raw_cliente_{self.cliente_id}"
        
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{nome_tabela}'
            );
        """)
        
        if not cur.fetchone()[0]:
            cur.execute(f"""
                CREATE TABLE {nome_tabela} (
                    id BIGSERIAL PRIMARY KEY,
                    pedido_id INTEGER NOT NULL,
                    data_coleta TIMESTAMP DEFAULT NOW(),
                    payload JSONB NOT NULL,
                    hash_conteudo TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX idx_{nome_tabela}_pedido ON {nome_tabela} (pedido_id);
                CREATE INDEX idx_{nome_tabela}_data ON {nome_tabela} (data_coleta DESC);
                CREATE UNIQUE INDEX idx_{nome_tabela}_hash ON {nome_tabela} (hash_conteudo);
                CREATE INDEX idx_{nome_tabela}_created ON {nome_tabela} (created_at);
            """)
            print(f" Tabela {nome_tabela} criada com sucesso!")
        
        return nome_tabela
    
    def _fetch_pedido_data(self, pedido_id, url_final, headers_dict):
        """Função para buscar dados de um pedido específico"""
        if self._stop_flag:
            return None, "canceled"
        
        if rate_limiter.check_daily_limit():
            self.daily_limit_hit = True
            return None, "daily_limit"
        
        if rate_limiter.wait_if_needed():
            return None, "daily_limit_wait"
            
        retry_count = 0
        while retry_count < RETRY_MAX and not self._stop_flag:
            try:
                response = requests.get(url_final, headers=headers_dict, timeout=REQUEST_TIMEOUT)
                
                if response.status_code == 200:
                    payload_dict = response.json()
                    hash_content = hashlib.md5(
                        json.dumps(payload_dict, sort_keys=True).encode()
                    ).hexdigest()
                    return (pedido_id, json.dumps(payload_dict), hash_content), "success"
                
                elif response.status_code == 429:
                    print(f"Rate limit detectado no pedido {pedido_id}, aumentando delay...")
                    time.sleep(2)
                    retry_count += 1
                    continue
                    
                elif response.status_code == 404:
                    return None, "not_found"
                    
                else:
                    retry_count += 1
                    if retry_count < RETRY_MAX:
                        wait_time = min((2 ** retry_count), 10)
                        time.sleep(wait_time)
                        continue
                    else:
                        return None, f"HTTP {response.status_code}"
                        
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count < RETRY_MAX:
                    wait_time = min((2 ** retry_count), 10)
                    time.sleep(wait_time)
                    continue
                else:
                    return None, str(e)
            except Exception as e:
                return None, str(e)
        
        return None, "max_retries_exceeded"
    
    def _process_batch(self, batch, url_template, headers_dict):
        """Processa um lote de pedidos usando ThreadPoolExecutor"""
        results = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_pedido = {}
            for pedido_id in batch:
                if self._stop_flag or self.daily_limit_hit:
                    break
                    
                url_final = url_template.replace('{pedido_id}', str(pedido_id))
                future = executor.submit(
                    self._fetch_pedido_data, 
                    pedido_id, 
                    url_final, 
                    headers_dict
                )
                future_to_pedido[future] = pedido_id
            
            for future in as_completed(future_to_pedido):
                if self._stop_flag or self.daily_limit_hit:
                    break
                    
                pedido_id = future_to_pedido[future]
                try:
                    result, status = future.result(timeout=REQUEST_TIMEOUT + 15)
                    
                    if status == "daily_limit" or status == "daily_limit_wait":
                        self.daily_limit_hit = True
                        break
                        
                    elif result and status == "success":
                        results.append(result)
                        with self.lock:
                            self.success_count += 1
                    else:
                        with self.lock:
                            self.error_count += 1
                        if status not in ["canceled", "daily_limit", "not_found"]:
                            registrar_erro(f"process_batch (pedido {pedido_id})", f"Status: {status}")
                            
                except Exception as e:
                    with self.lock:
                        self.error_count += 1
                    registrar_erro(f"process_batch_future (pedido {pedido_id})", e)
        
        return results
    
    def _database_writer_optimized(self):
        """Thread de escrita otimizada com batch processing"""
        conn = None
        cur = None
        batch_data = []
        batch_count = 0
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            while not self._stop_flag:
                try:
                    items_to_process = []
                    for _ in range(min(DB_BATCH_SIZE, self.data_queue.qsize() + 1)):
                        try:
                            item = self.data_queue.get_nowait()
                            if item[0] == "STOP":
                                if batch_data:
                                    self._insert_batch(cur, batch_data)
                                    conn.commit()
                                    batch_data = []
                                return
                            items_to_process.append(item)
                        except queue.Empty:
                            break
                    
                    if items_to_process:
                        batch_data.extend(items_to_process)
                        
                        if len(batch_data) >= DB_BATCH_SIZE:
                            self._insert_batch(cur, batch_data)
                            conn.commit()
                            batch_data = []
                            batch_count += 1
                    
                    if self.data_queue.empty():
                        time.sleep(0.01)
                        
                except Exception as e:
                    registrar_erro("database_writer_optimized", e)
            
            if batch_data and not self._stop_flag:
                self._insert_batch(cur, batch_data)
                conn.commit()
                
        except Exception as e:
            registrar_erro("database_writer_optimized (erro crítico)", e)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def _insert_batch(self, cur, batch_data):
        """Insere um lote de dados no banco"""
        if not batch_data:
            return
            
        values_placeholders = []
        values_params = []
        
        for pedido_id, payload, hash_content in batch_data:
            values_placeholders.append("(%s, %s, %s)")
            values_params.extend([pedido_id, payload, hash_content])
        
        query = f"""
            INSERT INTO {self.tabela_destino} (pedido_id, payload, hash_conteudo)
            VALUES {','.join(values_placeholders)}
            ON CONFLICT (hash_conteudo) DO NOTHING;
        """
        
        try:
            cur.execute(query, values_params)
            inserted_count = cur.rowcount
            
            with self.lock:
                self.total_registros += inserted_count
            
            for _ in batch_data:
                self.data_queue.task_done()
                
        except Exception as e:
            registrar_erro("_insert_batch", e)
    
    def run(self):
        """Executa o ETL de detalhes dos pedidos"""
        try:
            self.start_time = time.time()
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            self.tabela_destino = self._verificar_tabela_detalhes_raw(cur)
            conn.commit()
            
            cur.execute("SELECT token FROM clientes_tokens WHERE cliente_id = %s;", (int(self.cliente_id),))
            token_row = cur.fetchone()
            if not token_row:
                self.error.emit(f"Token não encontrado para o cliente {self.cliente_id}")
                return
            
            token = token_row[0]
            
            headers_dict = {}
            if isinstance(self.headers_template, dict):
                headers_dict = self.headers_template.copy()
            elif isinstance(self.headers_template, str):
                try:
                    headers_dict = json.loads(self.headers_template) if self.headers_template else {}
                except json.JSONDecodeError:
                    headers_dict = {}
            
            for key, value in headers_dict.items():
                if isinstance(value, str):
                    headers_dict[key] = value.replace('{token}', token).replace('{cliente_id}', str(self.cliente_id))
            
            url_template_ready = self.url_template.replace('{cliente_id}', str(self.cliente_id))
            
            cur.execute(f"""
                SELECT DISTINCT pedido_id 
                FROM "{self.tabela_bronze}"
                WHERE pedido_id IS NOT NULL
                ORDER BY pedido_id;
            """)
            
            pedidos = [row[0] for row in cur.fetchall()]
            total_pedidos = len(pedidos)
            
            if total_pedidos == 0:
                self.error.emit("Nenhum pedido encontrado na tabela bronze.")
                return
            
            print(f"\nIniciando extração de detalhes")
            print(f"Total de pedidos: {total_pedidos}")
            print(f"Cliente ID: {self.cliente_id}")
            
            self.writer_thread = threading.Thread(target=self._database_writer_optimized, daemon=True)
            self.writer_thread.start()
            
            for i in range(0, total_pedidos, BATCH_SIZE):
                if self._stop_flag or self.daily_limit_hit:
                    break
                
                batch = pedidos[i:i + BATCH_SIZE]
                batch_results = self._process_batch(batch, url_template_ready, headers_dict)
                
                for result in batch_results:
                    if self._stop_flag or self.daily_limit_hit:
                        break
                    try:
                        self.data_queue.put_nowait(result)
                    except queue.Full:
                        time.sleep(0.05)
                        try:
                            self.data_queue.put_nowait(result)
                        except queue.Full:
                            registrar_erro("executar_detalhes", "Fila cheia, perdendo dados")
                
                self.processed_count = min(i + len(batch), total_pedidos)
                elapsed_time = time.time() - self.start_time
                
                with self.lock:
                    total_registros_atual = self.total_registros
                    success_count = self.success_count
                    error_count = self.error_count
                
                if self.processed_count > 0:
                    tempo_medio_por_pedido = elapsed_time / self.processed_count
                    tempo_estimado = int(tempo_medio_por_pedido * (total_pedidos - self.processed_count))
                    taxa_registros = total_registros_atual / elapsed_time if elapsed_time > 0 else 0
                    requests_remaining = DAILY_LIMIT - rate_limiter.daily_count
                else:
                    tempo_estimado = 0
                    taxa_registros = 0
                    requests_remaining = DAILY_LIMIT
                
                msg = f"📄 Lote {i//BATCH_SIZE + 1}/{(total_pedidos-1)//BATCH_SIZE + 1} | ⏱️ {int(elapsed_time)}s | ✅ {success_count} | ❌ {error_count} | 📊 {total_registros_atual:,} | 🚀 {taxa_registros:.1f} reg/s | 📟 {requests_remaining} reqs restantes"
                self.progress.emit(
                    msg,
                    self.processed_count,
                    total_pedidos,
                    total_registros_atual,
                    tempo_estimado
                )
            
            if not self._stop_flag:
                try:
                    self.data_queue.put_nowait(("STOP", None, None))
                except queue.Full:
                    pass
                
                if self.writer_thread and self.writer_thread.is_alive():
                    self.writer_thread.join(timeout=30)
                
                conn.commit()
                cur.close()
                conn.close()
                
                tempo_total = time.time() - self.start_time
                taxa_final = self.total_registros / tempo_total if tempo_total > 0 else 0
                
                with self.lock:
                    success_count = self.success_count
                    error_count = self.error_count
                
                eficiencia = (success_count / self.processed_count * 100) if self.processed_count > 0 else 0
                
                if self.daily_limit_hit:
                    status_final = "⏸️ PAUSADO - LIMITE DIÁRIO ATINGIDO"
                    requests_used = rate_limiter.daily_count
                    next_reset = rate_limiter.daily_reset_time.strftime("%H:%M")
                else:
                    status_final = " CONCLUÍDO"
                    requests_used = rate_limiter.daily_count
                    next_reset = rate_limiter.daily_reset_time.strftime("%H:%M")
                
                mensagem_final = f"""Extração de Detalhes {status_final}

Total de registros coletados: {self.total_registros:,}
Tabela destino: {self.tabela_destino}
Cliente ID: {self.cliente_id}
Tempo total: {int(tempo_total)}s
Taxa média: {taxa_final:.1f} registros/segundo
Pedidos processados: {self.processed_count}/{total_pedidos}
Eficiência: {eficiencia:.1f}%
Sucessos: {success_count} | Erros: {error_count}
Requests usados: {requests_used}/{DAILY_LIMIT}
Próximo reset: {next_reset}
Configuração: {MAX_WORKERS} workers, batch {BATCH_SIZE}"""
                
                self.finished.emit(self.total_registros, mensagem_final)
            else:
                self.error.emit("Execução cancelada pelo usuário.")
                
        except Exception as e:
            registrar_erro("executar_detalhes", e)
            self.error.emit(f"Erro ao executar extração de detalhes:\n{e}")


# ===============================
#  Interface de Scraps de Detalhes
# ===============================
class ScrapsDetalhesManager(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gerenciador de Scraps de Detalhes de Pedidos")
        self.resize(1200, 700)

        layout = QVBoxLayout()

        # Configurações de Performance
        perf_layout = QHBoxLayout()
        perf_layout.addWidget(QLabel("Configurações de Performance:"))
        
        self.workers_combo = QComboBox()
        self.workers_combo.addItems(["2", "3", "4", "5"])
        self.workers_combo.setCurrentText("3")
        perf_layout.addWidget(QLabel("Workers:"))
        perf_layout.addWidget(self.workers_combo)
        
        self.batch_combo = QComboBox()
        self.batch_combo.addItems(["5", "10", "15", "20"])
        self.batch_combo.setCurrentText("10")
        perf_layout.addWidget(QLabel("Batch Size:"))
        perf_layout.addWidget(self.batch_combo)

        self.delay_combo = QComboBox()
        self.delay_combo.addItems(["0.2", "0.25", "0.3", "0.4"])
        self.delay_combo.setCurrentText("0.25")
        perf_layout.addWidget(QLabel("Delay (s):"))
        perf_layout.addWidget(self.delay_combo)
        
        self.status_label = QLabel(f"Requests hoje: {rate_limiter.daily_count}/{DAILY_LIMIT}")
        perf_layout.addWidget(self.status_label)
        
        perf_layout.addStretch()
        layout.addLayout(perf_layout)

        # Formulário
        form_layout = QVBoxLayout()

        # Linha 1: Tabela Bronze
        row1 = QHBoxLayout()
        self.tabela_bronze_combo = QComboBox()
        self.tabela_bronze_combo.setEditable(False)
        self.tabela_bronze_combo.setMinimumWidth(300)
        
        row1.addWidget(QLabel("Tabela Bronze:"))
        row1.addWidget(self.tabela_bronze_combo)
        btn_carregar = QPushButton("Carregar Tabelas Bronze")
        btn_carregar.clicked.connect(self.carregar_tabelas_bronze)
        row1.addWidget(btn_carregar)
        row1.addStretch()
        form_layout.addLayout(row1)
        
        # Linha 2: URL
        row2 = QHBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("https://jueri.com.br/sis/api/v1/{cliente_id}/pedido/{pedido_id}")
        self.url_input.setText("https://jueri.com.br/sis/api/v1/{cliente_id}/pedido/{pedido_id}")
        row2.addWidget(QLabel("URL Template:"))
        row2.addWidget(self.url_input, stretch=3)
        form_layout.addLayout(row2)
        
        # Linha 3: Headers
        row3 = QVBoxLayout()
        row3.addWidget(QLabel("Headers (JSON):"))
        self.headers_input = QTextEdit()
        self.headers_input.setPlaceholderText('{\n  "Authorization": "Bearer {token}",\n  "Accept": "application/json"\n}')
        self.headers_input.setMaximumHeight(80)
        self.headers_input.setPlainText('''{
  "Authorization": "Bearer {token}",
  "Accept": "application/json",
  "Content-Type": "application/json"
}''')
        row3.addWidget(self.headers_input)
        form_layout.addLayout(row3)

        # Botões
        btn_layout = QHBoxLayout()
        btn_executar = QPushButton("Executar Extração de Detalhes")
        btn_recarregar = QPushButton("Recarregar Lista")
        btn_status = QPushButton("Status Rate Limit")

        btn_executar.clicked.connect(self.executar_detalhes)
        btn_recarregar.clicked.connect(self.carregar_tabelas_bronze)
        btn_status.clicked.connect(self.mostrar_status_rate_limit)

        btn_layout.addWidget(btn_executar)
        btn_layout.addWidget(btn_recarregar)
        btn_layout.addWidget(btn_status)
        btn_layout.addStretch()

        form_layout.addLayout(btn_layout)
        layout.addLayout(form_layout)

        # Tabela de informações
        info_label = QLabel("Informações dos Pedidos na Tabela Bronze:")
        layout.addWidget(info_label)
        
        self.tabela_info = QTableWidget()
        self.tabela_info.setColumnCount(5)
        self.tabela_info.setHorizontalHeaderLabels(
            ["Pedido ID", "Status", "Comprador", "Valor Total", "Data Pedido"]
        )
        self.tabela_info.horizontalHeader().setStretchLastSection(True)

        layout.addWidget(self.tabela_info)
        self.setLayout(layout)

        # Inicializa
        self.inicializar_banco()
        self.carregar_tabelas_bronze()
        
        self.tabela_bronze_combo.currentTextChanged.connect(self.carregar_info_pedidos)
        
        self.timer = threading.Timer(60.0, self.atualizar_status_rate_limit)
        self.timer.daemon = True
        self.timer.start()

    def atualizar_status_rate_limit(self):
        """Atualiza o status do rate limit periodicamente"""
        try:
            self.status_label.setText(f" Requests hoje: {rate_limiter.daily_count}/{DAILY_LIMIT}")
        except:
            pass
        self.timer = threading.Timer(60.0, self.atualizar_status_rate_limit)
        self.timer.daemon = True
        self.timer.start()

    def mostrar_status_rate_limit(self):
        """Mostra status detalhado do rate limit"""
        remaining = DAILY_LIMIT - rate_limiter.daily_count
        next_reset = rate_limiter.daily_reset_time
        now = datetime.now()
        time_to_reset = next_reset - now
        
        horas = int(time_to_reset.total_seconds() // 3600)
        minutos = int((time_to_reset.total_seconds() % 3600) // 60)
        
        QMessageBox.information(self, "Status Rate Limit", 
            f"""📊 Status dos Limites da API:

Requests usados hoje: {rate_limiter.daily_count:,}/{DAILY_LIMIT:,}
Requests disponíveis: {remaining:,}
Próximo reset: {next_reset.strftime('%d/%m/%Y %H:%M')}
Tempo até reset: {horas:02d}:{minutos:02d}

 Configuração Atual:
   • Workers: {MAX_WORKERS}
   • Batch Size: {BATCH_SIZE} 
   • Delay entre requests: {RATE_LIMIT_DELAY}s
   • Limite: 5 req/segundo, {DAILY_LIMIT:,}/dia""")

    def conectar(self):
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding("UTF8")
        return conn

    def inicializar_banco(self):
        """Verifica se o banco está configurado corretamente"""
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'clientes_tokens'
                );
            """)
            
            existe = cur.fetchone()[0]
            
            if not existe:
                QMessageBox.warning(self, "Atenção", "Tabela clientes_tokens não encontrada. Execute a inicialização do banco primeiro.")
            
            cur.close()
            conn.close()
            
        except Exception as e:
            registrar_erro("inicializar_banco", e)

    def carregar_tabelas_bronze(self):
        """Carrega tabelas bronze disponíveis"""
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE '%_bronze' 
                AND table_schema = 'public'
                ORDER BY table_name;
            """)
            
            tabelas = cur.fetchall()
            
            self.tabela_bronze_combo.clear()
            for tabela in tabelas:
                self.tabela_bronze_combo.addItem(tabela[0])
            
            cur.close()
            conn.close()
            
            if tabelas:
                self.status_label.setText(f"Requests hoje: {rate_limiter.daily_count}/{DAILY_LIMIT}")
            else:
                QMessageBox.warning(self, "Aviso", "Nenhuma tabela bronze encontrada")
                
        except Exception as e:
            registrar_erro("carregar_tabelas_bronze", e)
            QMessageBox.critical(self, "Erro", f"Erro ao carregar tabelas:\n{e}")

    def carregar_info_pedidos(self, tabela_nome):
        """Carrega informações dos pedidos da tabela bronze selecionada"""
        if not tabela_nome:
            return
            
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            cur.execute(f"""
                SELECT 
                    pedido_id,
                    pedido_status,
                    comprador_nome,
                    valor_total,
                    data_pedido
                FROM "{tabela_nome}"
                WHERE pedido_id IS NOT NULL
                ORDER BY pedido_id DESC
                LIMIT 100;
            """)
            
            dados = cur.fetchall()
            
            self.tabela_info.setRowCount(len(dados))
            for i, row in enumerate(dados):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    self.tabela_info.setItem(i, j, item)
            
            cur.close()
            conn.close()
            
        except Exception as e:
            registrar_erro("carregar_info_pedidos", e)

    def executar_detalhes(self):
        """Executa a extração de detalhes dos pedidos"""
        tabela_bronze = self.tabela_bronze_combo.currentText()
        url_template = self.url_input.text().strip()
        headers_text = self.headers_input.toPlainText().strip()
        
        if not tabela_bronze:
            QMessageBox.warning(self, "Atenção", "Selecione uma tabela bronze.")
            return
        
        if not url_template:
            QMessageBox.warning(self, "Atenção", "Informe a URL template.")
            return
        
        try:
            partes = tabela_bronze.replace('tbl_', '').split('_')
            cliente_id = int(partes[0])
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Não foi possível identificar o Cliente ID:\n{e}")
            return
        
        try:
            if headers_text:
                headers_template = json.loads(headers_text)
            else:
                headers_template = {
                    "Authorization": "Bearer {token}",
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }
        except json.JSONDecodeError as e:
            QMessageBox.warning(self, "Erro no JSON", f"Headers inválido:\n{e}")
            return
        
        global MAX_WORKERS, BATCH_SIZE, RATE_LIMIT_DELAY
        MAX_WORKERS = int(self.workers_combo.currentText())
        BATCH_SIZE = int(self.batch_combo.currentText())
        RATE_LIMIT_DELAY = float(self.delay_combo.currentText())
        
        rate_limiter.max_per_second = min(4, int(1/RATE_LIMIT_DELAY))
        rate_limiter.min_interval = 1.0 / rate_limiter.max_per_second
        
        resposta = QMessageBox.question(
            self,
            "Confirmação",
            f"""Extrair detalhes dos pedidos?

Cliente ID: {cliente_id}
Tabela Bronze: {tabela_bronze}

 Configurações:
   • Workers: {MAX_WORKERS}
   • Batch Size: {BATCH_SIZE}
   • Delay entre requests: {RATE_LIMIT_DELAY}s
   • Rate Limit: {rate_limiter.max_per_second} req/segundo
   • Daily Limit: {DAILY_LIMIT:,} requests

Status:
   • Requests usados hoje: {rate_limiter.daily_count:,}
   • Requests disponíveis: {DAILY_LIMIT - rate_limiter.daily_count:,}

Deseja continuar?""",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            progress_dialog = QProgressDialog("Iniciando extração de detalhes...", "Cancelar", 0, 100, self)
            progress_dialog.setWindowTitle("Extraindo Detalhes")
            progress_dialog.setWindowModality(Qt.WindowModal)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.setValue(0)
            
            self.detalhes_worker = DetalhesWorker(tabela_bronze, cliente_id, url_template, headers_template)
            
            def on_progress(msg, current, total, registros, tempo_estimado):
                minutos = tempo_estimado // 60
                segundos = tempo_estimado % 60
                tempo_str = f"{minutos:02d}:{segundos:02d}" if tempo_estimado > 0 else "--:--"
                
                mensagem_completa = f"""
{msg}

 Estatísticas:
   • Progresso: {current}/{total} pedidos ({current*100//total if total > 0 else 0}%)
   • Registros coletados: {registros:,}
   • Tempo estimado: {tempo_str}
   • Workers: {MAX_WORKERS}, Batch: {BATCH_SIZE}, Delay: {RATE_LIMIT_DELAY}s
                """.strip()
                
                progress_dialog.setLabelText(mensagem_completa)
                progress_dialog.setMaximum(total)
                progress_dialog.setValue(current)
                QApplication.processEvents()
            
            def on_finished(registros, mensagem):
                progress_dialog.close()
                QMessageBox.information(self, "Sucesso", mensagem)
                self.status_label.setText(f"Requests hoje: {rate_limiter.daily_count}/{DAILY_LIMIT}")
            
            def on_error(mensagem):
                progress_dialog.close()
                QMessageBox.critical(self, "Erro", mensagem)
                self.status_label.setText(f"Requests hoje: {rate_limiter.daily_count}/{DAILY_LIMIT}")
            
            self.detalhes_worker.progress.connect(on_progress)
            self.detalhes_worker.finished.connect(on_finished)
            self.detalhes_worker.error.connect(on_error)
            
            progress_dialog.canceled.connect(self.detalhes_worker.stop)
            
            self.detalhes_worker.start()


# ===============================
#  Função principal para teste
# ===============================
def main():
    app = QApplication(sys.argv)
    janela = ScrapsDetalhesManager()
    janela.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()