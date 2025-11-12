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
)
from PySide6.QtCore import Qt, QDate, QThread, Signal


# ===============================
#  Funções auxiliares de log
# ===============================
LOG_FILE = "erros_scraps.log"


def registrar_erro(contexto, erro):
    """Grava o erro no arquivo erros_scraps.log com data/hora e stacktrace."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n" + "=" * 80 + "\n")
        f.write(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERRO EM: {contexto}\n"
        )
        f.write(str(erro) + "\n")
        f.write(traceback.format_exc())
        f.write("\n" + "=" * 80 + "\n")
    print(f"⚠️ ERRO ({contexto}): {erro}")


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

# Configurações do ETL
RETRY_MAX = 5          # Máximo de tentativas de retry
BATCH_EXPORT = 1       # Processa 1 registro por vez conforme solicitado


# ===============================
#  Thread de Execução do ETL com Multithreading
# ===============================
class ETLWorker(QThread):
    progress = Signal(str, int, int, int, int)  # mensagem, progresso_atual, progresso_total, registros_coletados, estimativa_segundos
    finished = Signal(int, str)  # registros_coletados, mensagem_final
    error = Signal(str)  # mensagem_erro
    
    def __init__(self, scrap_id, parent=None):
        super().__init__(parent)
        self.scrap_id = scrap_id
        self.db_config = DB_CONFIG
        self._stop_flag = False
        self.data_queue = queue.Queue(maxsize=1000)  # Fila thread-safe para dados
        self.total_registros = 0
        self.lock = threading.Lock()  # Lock para contadores thread-safe
        self.writer_thread = None
        
        # Controles de processamento
        self.retry_max = RETRY_MAX
        self.batch_export = BATCH_EXPORT
        self.start_time = None
        self.nome_tabela_destino = None  # Armazenará o nome da tabela específica
    
    def stop(self):
        self._stop_flag = True
        # Adiciona item especial para parar o writer
        try:
            self.data_queue.put_nowait(("STOP", None, None))
        except queue.Full:
            pass
    
    def _database_writer(self):
        """Thread separada para escrita no banco de dados - busca 1 escreve 1"""
        conn = None
        cur = None
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            while not self._stop_flag:
                try:
                    # Tenta pegar dados da fila com timeout
                    data_item = self.data_queue.get(timeout=1.0)
                    
                    if data_item[0] == "STOP":
                        break
                    
                    payload, hash_content, _ = data_item
                    
                    # Insere 1 registro por vez na tabela específica da rota
                    cur.execute(f"""
                        INSERT INTO {self.nome_tabela_destino} (payload, hash_conteudo)
                        VALUES (%s, %s)
                        ON CONFLICT (hash_conteudo) DO NOTHING;
                    """, (payload, hash_content))
                    
                    if cur.rowcount > 0:
                        with self.lock:
                            self.total_registros += 1
                        print(f" Registro inserido em {self.nome_tabela_destino}: {self.total_registros}")
                    
                    conn.commit()
                    self.data_queue.task_done()
                    
                except queue.Empty:
                    # Timeout normal, continua o loop
                    continue
                except Exception as e:
                    registrar_erro("database_writer", e)
                    break
                
        except Exception as e:
            registrar_erro("database_writer (erro crítico)", e)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def run(self):
        """Executa o ETL completo com multithreading e processamento em chunks"""
        try:
            self.start_time = time.time()
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Busca informações do scrap incluindo a tabela raw
            cur.execute("""
                SELECT cs.id, cs.cliente_id, cs.rota_id, cs.data_inicio, cs.data_fim,
                       car.url, car.headers, car.metodo_http, car.nome_rota, car.tabela_raw
                FROM clientes_scraps cs
                JOIN clientes_api_rotas car ON cs.rota_id = car.id
                WHERE cs.id = %s;
            """, (int(self.scrap_id),))
            
            scrap_data = cur.fetchone()
            if not scrap_data:
                self.error.emit("Scrap não encontrado.")
                return
            
            scrap_id_db, cliente_id, rota_id, data_inicio, data_fim, url, headers, metodo_http, nome_rota, tabela_raw = scrap_data
            
            # Define a tabela de destino
            self.nome_tabela_destino = tabela_raw
            
            if not self.nome_tabela_destino:
                self.error.emit(f"Tabela raw não configurada para a rota '{nome_rota}'.\n\nPor favor, recadastre a rota na aba 'Rotas de API'.")
                return
            
            # Busca token do cliente
            cur.execute("SELECT token FROM clientes_tokens WHERE cliente_id = %s;", (int(cliente_id),))
            token_row = cur.fetchone()
            if not token_row:
                self.error.emit(f"Token não encontrado para o cliente {cliente_id}")
                return
            
            token = token_row[0]
            
            # Atualiza status para executando
            cur.execute("""
                UPDATE clientes_scraps 
                SET status = 'executando', atualizado_em = NOW()
                WHERE id = %s;
            """, (int(self.scrap_id),))
            conn.commit()
            
            # Verifica se a tabela existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (self.nome_tabela_destino,))
            
            if not cur.fetchone()[0]:
                self.error.emit(f"Tabela '{self.nome_tabela_destino}' não existe no banco de dados.\n\nPor favor, recadastre a rota na aba 'Rotas de API'.")
                return
            
            conn.commit()
            
            # Inicia a thread de escrita no banco
            self.writer_thread = threading.Thread(target=self._database_writer, daemon=True)
            self.writer_thread.start()
            
            # Processa headers
            if isinstance(headers, dict):
                headers_dict = headers.copy() if headers else {}
            elif isinstance(headers, str):
                try:
                    headers_dict = json.loads(headers) if headers else {}
                except json.JSONDecodeError:
                    headers_dict = {}
            else:
                headers_dict = {}
            
            # Substitui placeholders nos headers
            for key, value in headers_dict.items():
                if isinstance(value, str):
                    headers_dict[key] = value.replace('{token}', token).replace('{cliente_id}', str(cliente_id))
            
            # Substitui placeholders na URL
            url_processada = url.replace('{cliente_id}', str(cliente_id))
            url_processada = url_processada.replace('{cliente}', str(cliente_id))
            
            # Converte datas para string no formato da API
            data_inicio_str = data_inicio.strftime('%Y-%m-%d')
            data_fim_str = data_fim.strftime('%Y-%m-%d')
            
            # Calcula progresso total estimado
            total_dias = (data_fim - data_inicio).days + 1
            estimativa_paginas = total_dias * 10  # Estimativa conservadora
            
            current_page = 1
            has_more_pages = True
            
            print(f"\n🚀 Iniciando ETL do cliente {cliente_id} - Rota: {nome_rota}")
            print(f"📅 Período: {data_inicio_str} até {data_fim_str}")
            print(f"⚙️ Configurações: Retry={self.retry_max}, Processamento=1 por vez")
            print(f"📊 Tabela destino: {self.nome_tabela_destino}")
            
            # Loop principal de coleta de dados
            while has_more_pages and not self._stop_flag:
                # Parâmetros da requisição com filtro de data
                params = {
                    "data_inicial": data_inicio_str,
                    "data_final": data_fim_str,
                    "page": current_page,
                    "per_page": 1000
                }
                
                # Retry logic melhorado
                retry_count = 0
                response = None
                
                while retry_count < self.retry_max and not self._stop_flag:
                    try:
                        if metodo_http.upper() == 'GET':
                            response = requests.get(url_processada, headers=headers_dict, params=params, timeout=30)
                        elif metodo_http.upper() == 'POST':
                            response = requests.post(url_processada, headers=headers_dict, params=params, timeout=30)
                        else:
                            response = requests.get(url_processada, headers=headers_dict, params=params, timeout=30)
                        
                        response.raise_for_status()
                        break  # Sucesso, sai do loop de retry
                        
                    except requests.exceptions.RequestException as e:
                        retry_count += 1
                        if retry_count < self.retry_max:
                            # Backoff exponencial
                            wait_time = (2 ** retry_count) + 0.5
                            print(f"⚠️ Tentativa {retry_count}/{self.retry_max} falhou, aguardando {wait_time:.1f}s...")
                            time.sleep(wait_time)
                            continue
                        else:
                            # Falha definitiva
                            registrar_erro(f"executar_scrap (página {current_page}) - Falha após {self.retry_max} tentativas", e)
                            has_more_pages = False
                            break
                
                if response is None or response.status_code != 200:
                    continue  # Pula para próxima página
                
                if response.status_code == 200:
                    response_data = response.json()
                    
                    # Extrai dados da resposta
                    if isinstance(response_data, dict):
                        if "data" in response_data and isinstance(response_data["data"], list):
                            items_to_save = response_data["data"]
                            # Verifica se há mais páginas usando múltiplos campos
                            has_more_pages = (
                                response_data.get("next_page_url") is not None or
                                (response_data.get("current_page", 0) < response_data.get("last_page", 1)) or
                                (response_data.get("current_page", 0) < response_data.get("total_pages", 1))
                            )
                        else:
                            items_to_save = [response_data]
                            has_more_pages = False
                    elif isinstance(response_data, list):
                        items_to_save = response_data
                        has_more_pages = False
                    else:
                        items_to_save = []
                        has_more_pages = False
                    
                    # Se não há itens nesta página, provavelmente chegou ao fim
                    if not items_to_save:
                        has_more_pages = False
                    
                    # Processa cada item e adiciona à fila thread-safe (busca 1 escreve 1)
                    for item in items_to_save:
                        if item:  # Verifica se o item não está vazio
                            # Gera hash único para evitar duplicatas
                            hash_content = hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()
                            
                            # Adiciona à fila thread-safe para escrita no banco
                            try:
                                self.data_queue.put_nowait((json.dumps(item), hash_content, None))
                                print(f"📤 Item enviado para fila: {hash_content[:8]}...")
                            except queue.Full:
                                # Se a fila estiver cheia, aguarda um pouco
                                time.sleep(0.1)
                                try:
                                    self.data_queue.put_nowait((json.dumps(item), hash_content, None))
                                except queue.Full:
                                    registrar_erro("executar_scrap", "Fila de dados cheia, perdendo dados")
                
                # Atualiza progresso usando contador thread-safe
                elapsed_time = time.time() - self.start_time
                tempo_estimado = int(elapsed_time / current_page * estimativa_paginas) if current_page > 1 else 0
                
                with self.lock:
                    total_registros_atual = self.total_registros
                
                # Calcula taxa de processamento
                taxa_registros = total_registros_atual / elapsed_time if elapsed_time > 0 else 0
                
                msg = f"📄 Página {current_page} | ⏱️ {int(elapsed_time)}s | 📊 {total_registros_atual:,} registros | 🚀 {taxa_registros:.1f} reg/s"
                self.progress.emit(
                    msg,
                    current_page,
                    estimativa_paginas,
                    total_registros_atual,
                    tempo_estimado
                )
                
                # Avança para próxima página
                current_page += 1
                
                # Se não tem mais páginas, para o loop
                if not has_more_pages:
                    break
                
                # Pequena pausa para não sobrecarregar a API
                time.sleep(0.1)
            
            # Aguarda a thread de escrita terminar
            if self.writer_thread and self.writer_thread.is_alive():
                # Adiciona item especial para parar o writer
                try:
                    self.data_queue.put_nowait(("STOP", None, None))
                except queue.Full:
                    pass
                
                # Aguarda a thread terminar (máximo 30 segundos)
                self.writer_thread.join(timeout=30)
            
            if not self._stop_flag:
                # Conecta novamente para atualizar status
                conn = psycopg2.connect(**self.db_config)
                cur = conn.cursor()
                
                with self.lock:
                    total_registros_final = self.total_registros
                
                # Atualiza status para concluido
                cur.execute("""
                    UPDATE clientes_scraps 
                    SET status = 'concluido', registros_coletados = %s, atualizado_em = NOW()
                    WHERE id = %s;
                """, (total_registros_final, int(self.scrap_id)))
                
                conn.commit()
                cur.close()
                conn.close()
                
                # Calcula estatísticas finais
                tempo_total = time.time() - self.start_time
                taxa_final = total_registros_final / tempo_total if tempo_total > 0 else 0
                
                mensagem_final = f"""Scrap ID {self.scrap_id} concluído com sucesso!

✅ Total de registros coletados: {total_registros_final:,}
📊 Tabela: {self.nome_tabela_destino}
📅 Período: {data_inicio_str} até {data_fim_str}
⏱️ Tempo total: {int(tempo_total)}s
🚀 Taxa média: {taxa_final:.1f} registros/segundo
📄 Páginas processadas: {current_page - 1}
🧵 Multithreading: Ativo (Coleta + Escrita simultânea)
⚙️ Processamento: Busca 1 escreve 1 (chunks)"""
                
                self.finished.emit(total_registros_final, mensagem_final)
            else:
                # Marca como cancelado
                conn = psycopg2.connect(**self.db_config)
                cur = conn.cursor()
                cur.execute("""
                    UPDATE clientes_scraps 
                    SET status = 'cancelado', atualizado_em = NOW()
                    WHERE id = %s;
                """, (int(self.scrap_id),))
                conn.commit()
                cur.close()
                conn.close()
                
                self.error.emit("Execução cancelada pelo usuário.")
                
        except Exception as e:
            # Em caso de erro, marca como erro
            try:
                if 'conn' not in locals() or conn is None:
                    conn = psycopg2.connect(**self.db_config)
                cur = conn.cursor()
                cur.execute("""
                    UPDATE clientes_scraps 
                    SET status = 'erro', atualizado_em = NOW()
                    WHERE id = %s;
                """, (int(self.scrap_id),))
                conn.commit()
                cur.close()
                conn.close()
            except:
                pass
            
            registrar_erro("executar_scrap", e)
            self.error.emit(f"Erro ao executar scrap:\n{e}")


# ===============================
#  Interface principal
# ===============================
class ScrapsManager(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gerenciador de Scraps de ETL")
        self.resize(1200, 700)

        layout = QVBoxLayout()

        # === SEÇÃO DE FORMULÁRIO ===
        form_layout = QVBoxLayout()

        # Linha 1: Cliente e Rota
        row1 = QHBoxLayout()
        self.cliente_combo = QComboBox()
        self.cliente_combo.setEditable(False)
        self.cliente_combo.setMinimumWidth(200)
        self.cliente_combo.currentIndexChanged.connect(self.carregar_rotas_combo)
        
        self.rota_combo = QComboBox()
        self.rota_combo.setEditable(False)
        self.rota_combo.setMinimumWidth(250)
        
        row1.addWidget(QLabel("Cliente:"))
        row1.addWidget(self.cliente_combo)
        row1.addWidget(QLabel("Rota:"))
        row1.addWidget(self.rota_combo)
        row1.addStretch()
        form_layout.addLayout(row1)
        
        # Linha 2: Datas
        row2 = QHBoxLayout()
        self.data_inicio = QDateEdit()
        self.data_inicio.setCalendarPopup(True)
        self.data_inicio.setDate(QDate.currentDate())
        self.data_inicio.setDisplayFormat("dd/MM/yyyy")
        
        self.data_fim = QDateEdit()
        self.data_fim.setCalendarPopup(True)
        self.data_fim.setDate(QDate.currentDate())
        self.data_fim.setDisplayFormat("dd/MM/yyyy")
        
        row2.addWidget(QLabel("Data Início:"))
        row2.addWidget(self.data_inicio)
        row2.addWidget(QLabel("Data Fim:"))
        row2.addWidget(self.data_fim)
        row2.addStretch()
        form_layout.addLayout(row2)

        # Botões de ação
        btn_layout = QHBoxLayout()
        btn_salvar = QPushButton("Criar Scrap")
        btn_excluir = QPushButton("Excluir")
        btn_executar = QPushButton(" Executar Scrap")
        btn_recarregar = QPushButton(" Recarregar Lista")

        btn_salvar.clicked.connect(self.salvar_scrap)
        btn_excluir.clicked.connect(self.excluir_scrap)
        btn_executar.clicked.connect(self.executar_scrap)
        btn_recarregar.clicked.connect(self.recarregar_tudo)

        btn_layout.addWidget(btn_salvar)
        btn_layout.addWidget(btn_excluir)
        btn_layout.addWidget(btn_executar)
        btn_layout.addWidget(btn_recarregar)
        btn_layout.addStretch()

        form_layout.addLayout(btn_layout)
        layout.addLayout(form_layout)

        # === TABELA ===
        self.tabela = QTableWidget()
        self.tabela.setColumnCount(9)
        self.tabela.setHorizontalHeaderLabels(
            [
                "ID",
                "Cliente ID",
                "Rota",
                "Data Início",
                "Data Fim",
                "Status",
                "Registros",
                "Criado em",
                "Atualizado em",
            ]
        )
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.cellClicked.connect(self.preencher_campos)

        layout.addWidget(self.tabela)
        self.setLayout(layout)

        # Inicializa o banco e carrega dados
        self.inicializar_banco()
        self.carregar_clientes()
        self.carregar_dados()

    def conectar(self):
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding("UTF8")
        return conn

    def inicializar_banco(self):
        """Cria a tabela de scraps de ETL se não existir"""
        try:
            conn = self.conectar()
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes_scraps (
                    id SERIAL PRIMARY KEY,
                    cliente_id INTEGER NOT NULL REFERENCES clientes_tokens(cliente_id) ON DELETE CASCADE,
                    rota_id INTEGER NOT NULL REFERENCES clientes_api_rotas(id) ON DELETE CASCADE,
                    data_inicio DATE NOT NULL,
                    data_fim DATE NOT NULL,
                    status TEXT DEFAULT 'pendente',
                    registros_coletados INTEGER DEFAULT 0,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    atualizado_em TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT check_datas CHECK (data_fim >= data_inicio)
                );
                
                CREATE INDEX IF NOT EXISTS idx_scraps_cliente 
                    ON clientes_scraps(cliente_id);
                
                CREATE INDEX IF NOT EXISTS idx_scraps_status 
                    ON clientes_scraps(status) WHERE status = 'pendente';
                
                CREATE INDEX IF NOT EXISTS idx_scraps_datas 
                    ON clientes_scraps(data_inicio, data_fim);
                
                -- Trigger para atualizar timestamp
                CREATE OR REPLACE FUNCTION atualiza_timestamp_scraps() 
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.atualizado_em = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                
                DROP TRIGGER IF EXISTS trg_atualiza_scraps ON clientes_scraps;
                
                CREATE TRIGGER trg_atualiza_scraps 
                    BEFORE UPDATE ON clientes_scraps
                    FOR EACH ROW 
                    EXECUTE FUNCTION atualiza_timestamp_scraps();
            """)

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("inicializar_banco", e)

    def carregar_clientes(self):
        """Carrega os clientes cadastrados no dropdown"""
        try:
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute("""
                SELECT cliente_id, ativo 
                FROM clientes_tokens 
                ORDER BY cliente_id;
            """)
            clientes = cur.fetchall()
            
            self.cliente_combo.clear()
            for cliente_id, ativo in clientes:
                if ativo:
                    self.cliente_combo.addItem(str(cliente_id), cliente_id)
            
            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_clientes", e)

    def carregar_rotas_combo(self):
        """Carrega as rotas do cliente selecionado"""
        try:
            self.rota_combo.clear()
            cliente_id = self.cliente_combo.currentData()
            
            if cliente_id is None:
                return
            
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, nome_rota, url, tabela_raw 
                FROM clientes_api_rotas 
                WHERE cliente_id = %s AND ativo = TRUE
                ORDER BY nome_rota;
            """, (cliente_id,))
            rotas = cur.fetchall()
            
            for rota_id, nome_rota, url, tabela_raw in rotas:
                # Mostra a tabela raw no combo para o usuário saber onde os dados serão salvos
                tabela_info = f" → {tabela_raw}" if tabela_raw else " [SEM TABELA]"
                display_text = f"{nome_rota}{tabela_info}"
                self.rota_combo.addItem(display_text, rota_id)
            
            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_rotas_combo", e)

    def recarregar_tudo(self):
        """Recarrega clientes, rotas e dados da tabela"""
        self.carregar_clientes()
        self.carregar_dados()

    def carregar_dados(self):
        """Carrega os scraps cadastrados"""
        try:
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute("""
                SELECT cs.id, cs.cliente_id, car.nome_rota, cs.data_inicio, cs.data_fim, 
                       cs.status, cs.registros_coletados, cs.criado_em, cs.atualizado_em
                FROM clientes_scraps cs
                JOIN clientes_api_rotas car ON cs.rota_id = car.id
                ORDER BY cs.id DESC;
            """)
            dados = cur.fetchall()

            self.tabela.setRowCount(len(dados))
            for i, row in enumerate(dados):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    self.tabela.setItem(i, j, item)

            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_dados", e)
            QMessageBox.critical(self, "Erro", f"Erro ao carregar dados:\n{e}")

    def salvar_scrap(self):
        """Salva um novo scrap"""
        cliente_id = self.cliente_combo.currentData()
        rota_id = self.rota_combo.currentData()
        
        if cliente_id is None or rota_id is None:
            QMessageBox.warning(
                self, "Atenção", "Selecione um cliente e uma rota."
            )
            return
        
        data_inicio = self.data_inicio.date().toPython()
        data_fim = self.data_fim.date().toPython()
        
        if data_fim < data_inicio:
            QMessageBox.warning(
                self, "Atenção", "A data fim deve ser maior ou igual à data início."
            )
            return

        try:
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO clientes_scraps (cliente_id, rota_id, data_inicio, data_fim)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
            """, (int(cliente_id), int(rota_id), data_inicio, data_fim))
            
            scrap_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            
            QMessageBox.information(self, "Sucesso", f"Scrap criado com sucesso! ID: {scrap_id}")
            self.limpar_campos()
            self.recarregar_tudo()
        except Exception as e:
            registrar_erro("salvar_scrap", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar scrap:\n{e}")

    def excluir_scrap(self):
        """Exclui um scrap"""
        item = self.tabela.currentItem()
        if item is None:
            QMessageBox.warning(self, "Atenção", "Selecione um scrap na tabela para excluir.")
            return
        
        row = item.row()
        scrap_id = self.tabela.item(row, 0).text()

        resposta = QMessageBox.question(
            self,
            "Confirmação",
            f"Deseja realmente excluir o scrap ID {scrap_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            try:
                conn = self.conectar()
                cur = conn.cursor()
                cur.execute("DELETE FROM clientes_scraps WHERE id = %s;", (int(scrap_id),))
                conn.commit()
                cur.close()
                conn.close()
                
                QMessageBox.information(self, "Sucesso", "Scrap removido com sucesso.")
                self.recarregar_tudo()
            except Exception as e:
                registrar_erro("excluir_scrap", e)
                QMessageBox.critical(self, "Erro", f"Erro ao excluir scrap:\n{e}")

    def executar_scrap(self):
        """Executa o ETL completo: busca dados da API e salva na tabela específica da rota"""
        item = self.tabela.currentItem()
        if item is None:
            QMessageBox.warning(self, "Atenção", "Selecione um scrap na tabela para executar.")
            return
        
        row = item.row()
        scrap_id = self.tabela.item(row, 0).text()
        status = self.tabela.item(row, 5).text()

        if status in ['executando', 'concluido']:
            QMessageBox.warning(self, "Atenção", f"Este scrap já está {status}.")
            return

        resposta = QMessageBox.question(
            self,
            "Confirmação",
            f"Executar o scrap ID {scrap_id}?\n\nIsso iniciará o processo de coleta de dados.",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            # Cria diálogo de progresso
            progress_dialog = QProgressDialog("Iniciando execução do ETL...", "Cancelar", 0, 100, self)
            progress_dialog.setWindowTitle("Executando Scrap")
            progress_dialog.setWindowModality(Qt.WindowModal)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.setValue(0)
            
            # Cria a thread de execução
            self.etl_worker = ETLWorker(int(scrap_id))
            
            # Conecta sinais
            def on_progress(msg, current, total, registros, tempo_estimado):
                # Converte tempo estimado para formato legível
                minutos = tempo_estimado // 60
                segundos = tempo_estimado % 60
                tempo_str = f"{minutos:02d}:{segundos:02d}" if tempo_estimado > 0 else "--:--"
                
                # Formata mensagem completa
                mensagem_completa = f"""
{msg}

Estatísticas:
   • Progresso: {current}/{total} páginas ({current*100//total if total > 0 else 0}%)
   • Registros coletados: {registros:,}
   • Tempo estimado restante: {tempo_str}
                """.strip()
                
                progress_dialog.setLabelText(mensagem_completa)
                progress_dialog.setMaximum(total)
                progress_dialog.setValue(current)
                
                # Processa eventos para atualizar a UI
                QApplication.processEvents()
            
            def on_finished(registros, mensagem):
                progress_dialog.close()
                QMessageBox.information(self, "Sucesso", mensagem)
                self.recarregar_tudo()
            
            def on_error(mensagem):
                progress_dialog.close()
                QMessageBox.critical(self, "Erro", mensagem)
                self.recarregar_tudo()
            
            self.etl_worker.progress.connect(on_progress)
            self.etl_worker.finished.connect(on_finished)
            self.etl_worker.error.connect(on_error)
            
            # Conecta cancelamento
            progress_dialog.canceled.connect(self.etl_worker.stop)
            
            # Inicia a thread
            self.etl_worker.start()

    def preencher_campos(self, row, _):
        """Preenche os campos com os dados da linha clicada"""
        cliente_id = self.tabela.item(row, 1).text()
        
        # Define o cliente selecionado
        index = self.cliente_combo.findData(int(cliente_id))
        if index >= 0:
            self.cliente_combo.setCurrentIndex(index)
        
        # Define as datas
        data_inicio_str = self.tabela.item(row, 3).text()
        data_fim_str = self.tabela.item(row, 4).text()
        
        try:
            data_inicio = datetime.strptime(data_inicio_str, "%Y-%m-%d")
            data_fim = datetime.strptime(data_fim_str, "%Y-%m-%d")
            self.data_inicio.setDate(QDate(data_inicio.year, data_inicio.month, data_inicio.day))
            self.data_fim.setDate(QDate(data_fim.year, data_fim.month, data_fim.day))
        except:
            pass

    def limpar_campos(self):
        """Limpa os campos do formulário"""
        self.cliente_combo.setCurrentIndex(0)
        self.rota_combo.clear()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    janela = ScrapsManager()
    janela.show()
    sys.exit(app.exec())