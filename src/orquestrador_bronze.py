# orquestrador_bronze.py
import os
import psycopg2
import traceback
import json
from datetime import datetime
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QMessageBox,
    QComboBox,
    QProgressDialog,
    QTextEdit,
)
from PySide6.QtCore import Qt, QThread, Signal

from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

# ===============================
#  Thread de Processamento Bronze
# ===============================
class BronzeProcessor(QThread):
    progress = Signal(str, int, int)  # mensagem, progresso_atual, progresso_total
    finished = Signal(str)  # mensagem_final
    error = Signal(str)  # mensagem_erro
    
    def __init__(self, tabela_raw, cliente_id, parent=None):
        super().__init__(parent)
        self.tabela_raw = tabela_raw
        self.cliente_id = cliente_id
        self.db_config = DB_CONFIG
    
    def _extrair_cliente_id_da_tabela(self, nome_tabela):
        """Extrai o cliente_id do nome da tabela"""
        try:
            # Remove 'tbl_' do início se existir
            nome_sem_prefixo = nome_tabela.replace('tbl_', '')
            
            # Pega a primeira parte antes do primeiro underscore
            partes = nome_sem_prefixo.split('_')
            if partes:
                return int(partes[0])
            else:
                raise ValueError("Não foi possível extrair cliente_id")
        except Exception as e:
            raise ValueError(f"Erro ao extrair cliente_id de {nome_tabela}: {e}")
    
    def _criar_tabela_bronze(self, cur, nome_tabela_bronze):
        """Cria tabela bronze com estrutura normalizada"""
        try:
            print(f"🛠️ Criando tabela: {nome_tabela_bronze}")
            
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{nome_tabela_bronze}" (
                    id BIGSERIAL PRIMARY KEY,
                    raw_id BIGINT NOT NULL,
                    cliente_id INTEGER NOT NULL,
                    data_processamento TIMESTAMP DEFAULT NOW(),
                    
                    -- Campos comuns extraídos
                    pedido_id INTEGER,
                    pedido_status TEXT,
                    vendedor_nome TEXT,
                    comprador_id INTEGER,
                    comprador_nome TEXT,
                    comprador_email TEXT,
                    comprador_documento TEXT,
                    data_baixa TIMESTAMP,
                    data_pedido TIMESTAMP,
                    observacao TEXT,
                    integracao TEXT,
                    
                    -- Dados financeiros
                    valor_total DECIMAL(15,2),
                    valor_desconto DECIMAL(15,2),
                    valor_liquido DECIMAL(15,2),
                    
                    -- Dados de endereço
                    endereco_entrega JSONB,
                    
                    -- Itens do pedido
                    itens_pedido JSONB,
                    
                    -- Metadados
                    metadata JSONB,
                    
                    -- Links para outras tabelas
                    usuario_id INTEGER,
                    divisao_id INTEGER,
                    
                    CONSTRAINT fk_bronze_cliente FOREIGN KEY (cliente_id) 
                        REFERENCES clientes_tokens(cliente_id) ON DELETE CASCADE
                );
            """)
            print("CREATE TABLE executado")
            
            # Cria índices
            nome_idx = nome_tabela_bronze.replace('tbl_', '').replace('"', '')
            print(f"Criando índices para: {nome_idx}")
            
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_idx}_cliente ON "{nome_tabela_bronze}" (cliente_id);')
                print("Índice cliente criado")
            except Exception as e:
                print(f"Erro ao criar índice cliente: {e}")
            
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_idx}_pedido ON "{nome_tabela_bronze}" (pedido_id);')
                print("Índice pedido criado")
            except Exception as e:
                print(f"Erro ao criar índice pedido: {e}")
            
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_idx}_comprador ON "{nome_tabela_bronze}" (comprador_id);')
                print("Índice comprador criado")
            except Exception as e:
                print(f"Erro ao criar índice comprador: {e}")
            
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{nome_idx}_data ON "{nome_tabela_bronze}" (data_processamento);')
                print("Índice data criado")
            except Exception as e:
                print(f"Erro ao criar índice data: {e}")
            
        except Exception as e:
            print(f"Erro ao criar tabela bronze: {e}")
            raise
    
    def _extrair_dados_pedido(self, payload):
        """Extrai e normaliza dados do payload do pedido"""
        try:
            dados = {}
            
            # Campos básicos com tratamento para None
            dados['pedido_id'] = payload.get('id')
            dados['pedido_status'] = payload.get('status')
            
            # Trata vendedor que pode ser None
            vendedor = payload.get('vendedor')
            if vendedor is not None:
                dados['vendedor_nome'] = str(vendedor).strip()
            else:
                dados['vendedor_nome'] = None
                
            dados['observacao'] = payload.get('observacao')
            dados['integracao'] = payload.get('integracao')
            
            # Datas
            if payload.get('data_baixa'):
                try:
                    dados['data_baixa'] = datetime.fromisoformat(payload['data_baixa'].replace('Z', '+00:00'))
                except:
                    dados['data_baixa'] = None
            
            if payload.get('data_pedido'):
                try:
                    dados['data_pedido'] = datetime.fromisoformat(payload['data_pedido'].replace('Z', '+00:00'))
                except:
                    dados['data_pedido'] = None
            
            # Comprador
            comprador = payload.get('comprador', {})
            if comprador and isinstance(comprador, dict):
                dados['comprador_id'] = comprador.get('id')
                
                # Trata nome do comprador que pode ser None
                comprador_nome = comprador.get('nome')
                if comprador_nome is not None:
                    dados['comprador_nome'] = str(comprador_nome).strip()
                else:
                    dados['comprador_nome'] = None
                    
                dados['comprador_email'] = comprador.get('email')
                dados['comprador_documento'] = comprador.get('documento')
            else:
                dados['comprador_id'] = None
                dados['comprador_nome'] = None
                dados['comprador_email'] = None
                dados['comprador_documento'] = None
            
            # Dados financeiros
            financeiro = payload.get('financeiro', {})
            if financeiro and isinstance(financeiro, dict):
                dados['valor_total'] = financeiro.get('total')
                dados['valor_desconto'] = financeiro.get('desconto')
                dados['valor_liquido'] = financeiro.get('liquido')
            else:
                # Tenta encontrar campos financeiros no root
                dados['valor_total'] = payload.get('valor_total') or payload.get('total')
                dados['valor_desconto'] = payload.get('desconto')
                dados['valor_liquido'] = payload.get('valor_liquido') or payload.get('liquido')
            
            # Endereço de entrega
            dados['endereco_entrega'] = payload.get('endereco_entrega') or payload.get('endereco')
            
            # Itens do pedido
            dados['itens_pedido'] = payload.get('itens') or payload.get('items') or payload.get('produtos')
            
            # Metadados adicionais
            metadata = {}
            for key, value in payload.items():
                if key not in ['id', 'status', 'vendedor', 'comprador', 'data_baixa', 
                              'data_pedido', 'observacao', 'integracao', 'financeiro',
                              'endereco_entrega', 'endereco', 'itens', 'items', 'produtos',
                              'valor_total', 'total', 'desconto', 'valor_liquido', 'liquido']:
                    metadata[key] = value
            
            dados['metadata'] = metadata if metadata else None
            
            return dados
            
        except Exception as e:
            print(f"Erro ao extrair dados do pedido: {e}")
            return {}
    
    def _mapear_usuario_divisao(self, cur, comprador_documento, comprador_nome):
        """Mapeia comprador para usuário e divisão existentes"""
        try:
            # Primeiro tenta pelo documento (se não for None)
            if comprador_documento and comprador_documento.strip():
                cur.execute("""
                    SELECT u.id, u.divisao_id 
                    FROM usuarios u 
                    WHERE u.documento = %s AND u.ativo = TRUE
                    LIMIT 1;
                """, (comprador_documento.strip(),))
                resultado = cur.fetchone()
                if resultado:
                    return resultado
            
            # Se não encontrou pelo documento, tenta pelo nome (se não for None)
            if comprador_nome and comprador_nome.strip():
                cur.execute("""
                    SELECT u.id, u.divisao_id 
                    FROM usuarios u 
                    WHERE LOWER(TRIM(u.nome)) = LOWER(TRIM(%s)) AND u.ativo = TRUE
                    LIMIT 1;
                """, (comprador_nome.strip(),))
                resultado = cur.fetchone()
                if resultado:
                    return resultado
            
            return (None, None)
            
        except Exception as e:
            print(f"Erro no mapeamento de usuário: {e}")
            return (None, None)
    
    def run(self):
        """Executa o processamento dos dados raw para bronze"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Extrai cliente_id do nome da tabela
            cliente_id = self._extrair_cliente_id_da_tabela(self.tabela_raw)
            print(f"Cliente ID extraído: {cliente_id}")
            print(f"Tabela RAW: {self.tabela_raw}")
            
            # Nome da tabela bronze
            nome_base = self.tabela_raw.replace('_raw', '_bronze')
            nome_tabela_bronze = nome_base
            print(f"Tabela Bronze a ser criada: {nome_tabela_bronze}")
            
            # Cria tabela bronze
            self.progress.emit("Criando tabela bronze...", 0, 100)
            print("Tentando criar tabela bronze...")
            self._criar_tabela_bronze(cur, nome_tabela_bronze)
            conn.commit()
            print("Tabela bronze criada/comitada")
            
            # Verifica se a tabela foi criada
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (nome_tabela_bronze,))
            
            tabela_existe = cur.fetchone()[0]
            print(f"Tabela {nome_tabela_bronze} existe? {tabela_existe}")
            
            if not tabela_existe:
                self.error.emit(f"Falha ao criar tabela bronze: {nome_tabela_bronze}")
                return
            
            # Conta registros para processar
            cur.execute(f'SELECT COUNT(*) FROM "{self.tabela_raw}";')
            total_registros = cur.fetchone()[0]
            print(f"Total de registros na RAW: {total_registros}")
            
            if total_registros == 0:
                self.finished.emit(f"Nenhum dado para processar na tabela {self.tabela_raw}")
                return
            
            self.progress.emit(f"Iniciando processamento de {total_registros} registros...", 0, total_registros)
            
            # Busca todos os registros raw
            cur.execute(f'''
                SELECT id, payload, data_processamento 
                FROM "{self.tabela_raw}" 
                ORDER BY id;
            ''')
            
            registros_processados = 0
            registros_com_erro = 0
            registros_raw = cur.fetchall()
            print(f"Total de registros buscados: {len(registros_raw)}")
            
            # Processa cada registro
            for i, (raw_id, payload, data_processamento) in enumerate(registros_raw):
                try:
                    if i % 100 == 0:
                        print(f"Processando registro {i}/{len(registros_raw)} - ID: {raw_id}")
                    
                    # Converte payload para dict
                    if isinstance(payload, str):
                        try:
                            payload_dict = json.loads(payload)
                        except json.JSONDecodeError:
                            print(f"Erro ao decodificar JSON do registro {raw_id}")
                            registros_com_erro += 1
                            continue
                    else:
                        payload_dict = payload
                    
                    # Verifica se payload não é None
                    if payload_dict is None:
                        print(f"Payload vazio no registro {raw_id}")
                        registros_com_erro += 1
                        continue
                    
                    # Extrai dados normalizados
                    dados_normalizados = self._extrair_dados_pedido(payload_dict)
                    
                    # Se não conseguiu extrair dados, pula para o próximo
                    if not dados_normalizados:
                        print(f"Não foi possível extrair dados do registro {raw_id}")
                        registros_com_erro += 1
                        continue
                    
                    # Mapeia usuário e divisão
                    usuario_id, divisao_id = self._mapear_usuario_divisao(
                        cur, 
                        dados_normalizados.get('comprador_documento'),
                        dados_normalizados.get('comprador_nome')
                    )
                    
                    # Prepara dados para inserção (trata valores None)
                    valor_total = dados_normalizados.get('valor_total')
                    valor_desconto = dados_normalizados.get('valor_desconto')
                    valor_liquido = dados_normalizados.get('valor_liquido')
                    
                    # Converte para decimal ou mantém como None
                    try:
                        valor_total = float(valor_total) if valor_total is not None else None
                    except (TypeError, ValueError):
                        valor_total = None
                        
                    try:
                        valor_desconto = float(valor_desconto) if valor_desconto is not None else None
                    except (TypeError, ValueError):
                        valor_desconto = None
                        
                    try:
                        valor_liquido = float(valor_liquido) if valor_liquido is not None else None
                    except (TypeError, ValueError):
                        valor_liquido = None
                    
                    # Insere na tabela bronze
                    cur.execute(f'''
                        INSERT INTO "{nome_tabela_bronze}" (
                            raw_id, cliente_id, pedido_id, pedido_status, vendedor_nome,
                            comprador_id, comprador_nome, comprador_email, comprador_documento,
                            data_baixa, data_pedido, observacao, integracao,
                            valor_total, valor_desconto, valor_liquido,
                            endereco_entrega, itens_pedido, metadata,
                            usuario_id, divisao_id
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
                        );
                    ''', (
                        raw_id, cliente_id,
                        dados_normalizados.get('pedido_id'),
                        dados_normalizados.get('pedido_status'),
                        dados_normalizados.get('vendedor_nome'),
                        dados_normalizados.get('comprador_id'),
                        dados_normalizados.get('comprador_nome'),
                        dados_normalizados.get('comprador_email'),
                        dados_normalizados.get('comprador_documento'),
                        dados_normalizados.get('data_baixa'),
                        dados_normalizados.get('data_pedido'),
                        dados_normalizados.get('observacao'),
                        dados_normalizados.get('integracao'),
                        valor_total,
                        valor_desconto,
                        valor_liquido,
                        json.dumps(dados_normalizados.get('endereco_entrega')) if dados_normalizados.get('endereco_entrega') else None,
                        json.dumps(dados_normalizados.get('itens_pedido')) if dados_normalizados.get('itens_pedido') else None,
                        json.dumps(dados_normalizados.get('metadata')) if dados_normalizados.get('metadata') else None,
                        usuario_id,
                        divisao_id
                    ))
                    
                    registros_processados += 1
                    
                    # Atualiza progresso a cada 10 registros
                    if registros_processados % 10 == 0:
                        self.progress.emit(
                            f"Processados {registros_processados}/{total_registros} registros...",
                            registros_processados,
                            total_registros
                        )
                        
                except Exception as e:
                    registros_com_erro += 1
                    print(f"Erro ao processar registro {raw_id}: {e}")
                    continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Verifica quantos registros foram inseridos na tabela bronze
            conn_final = psycopg2.connect(**self.db_config)
            cur_final = conn_final.cursor()
            cur_final.execute(f'SELECT COUNT(*) FROM "{nome_tabela_bronze}";')
            total_bronze = cur_final.fetchone()[0]
            cur_final.close()
            conn_final.close()
            
            mensagem_final = f"""
 Processamento Bronze Concluído!

 Estatísticas:
   • Tabela origem: {self.tabela_raw}
   • Tabela destino: {nome_tabela_bronze}
   • Registros na RAW: {total_registros}
   • Registros processados: {registros_processados}
   • Registros na BRONZE: {total_bronze}
   • Registros com erro: {registros_com_erro}
   • Cliente ID: {cliente_id}

 Dados extraídos:
   • Pedidos normalizados
   • Compradores mapeados
   • Dados financeiros
   • Itens e endereços
   • Metadados organizados
            """.strip()
            
            print(f"Processamento finalizado: {registros_processados} registros processados")
            print(f"Registros na tabela bronze: {total_bronze}")
            self.finished.emit(mensagem_final)
            
        except Exception as e:
            error_msg = f"Erro no processamento bronze:\n{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            self.error.emit(error_msg)

# ===============================
#  Interface do Orquestrador Bronze
# ===============================
class OrquestradorBronze(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Orquestrador Bronze - Transformação de Dados")
        self.resize(1000, 700)
        
        layout = QVBoxLayout()
        
        # Título
        titulo = QLabel("Orquestrador Bronze - Transformação RAW para BRONZE")
        titulo.setStyleSheet("font-size: 16px; font-weight: bold; margin: 10px;")
        layout.addWidget(titulo)
        
        # Controles
        controles_layout = QHBoxLayout()
        
        # Combo para selecionar tabelas raw
        controles_layout.addWidget(QLabel("Tabela RAW:"))
        self.tabela_raw_combo = QComboBox()
        self.tabela_raw_combo.setMinimumWidth(300)
        controles_layout.addWidget(self.tabela_raw_combo)
        
        # Botão para carregar tabelas
        btn_carregar_tabelas = QPushButton("Carregar Tabelas")
        btn_carregar_tabelas.clicked.connect(self.carregar_tabelas_raw)
        controles_layout.addWidget(btn_carregar_tabelas)
        
        # Botão processar
        btn_processar = QPushButton("Processar para Bronze")
        btn_processar.clicked.connect(self.processar_bronze)
        btn_processar.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
        controles_layout.addWidget(btn_processar)
        
        controles_layout.addStretch()
        layout.addLayout(controles_layout)
        
        # Área de informações
        info_layout = QVBoxLayout()
        info_layout.addWidget(QLabel("Informações da Tabela Selecionada:"))
        self.info_text = QTextEdit()
        self.info_text.setMaximumHeight(150)
        self.info_text.setReadOnly(True)
        info_layout.addWidget(self.info_text)
        layout.addLayout(info_layout)
        
        # Tabela de preview
        layout.addWidget(QLabel("Preview dos Dados RAW:"))
        self.tabela_preview = QTableWidget()
        self.tabela_preview.setColumnCount(4)
        self.tabela_preview.setHorizontalHeaderLabels(["ID", "Data Processamento", "Status", "Comprador"])
        layout.addWidget(self.tabela_preview)
        
        # Conecta o combo para atualizar preview
        self.tabela_raw_combo.currentTextChanged.connect(self.atualizar_preview)
        
        self.setLayout(layout)
        
        # Carrega tabelas ao iniciar
        self.carregar_tabelas_raw()
    
    def conectar(self):
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding("UTF8")
        return conn
    
    def _extrair_cliente_id_da_tabela(self, nome_tabela):
        """Extrai o cliente_id do nome da tabela"""
        try:
            # Remove 'tbl_' do início se existir
            nome_sem_prefixo = nome_tabela.replace('tbl_', '')
            
            # Pega a primeira parte antes do primeiro underscore
            partes = nome_sem_prefixo.split('_')
            if partes:
                return int(partes[0])
            else:
                raise ValueError("Não foi possível extrair cliente_id")
        except Exception as e:
            raise ValueError(f"Erro ao extrair cliente_id de {nome_tabela}: {e}")
    
    def carregar_tabelas_raw(self):
        """Carrega lista de tabelas raw disponíveis"""
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            # Busca tabelas que terminam com _raw
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE '%_raw' 
                AND table_schema = 'public'
                ORDER BY table_name;
            """)
            
            tabelas = cur.fetchall()
            
            self.tabela_raw_combo.clear()
            for tabela in tabelas:
                self.tabela_raw_combo.addItem(tabela[0])
            
            cur.close()
            conn.close()
            
            if tabelas:
                QMessageBox.information(self, "Sucesso", f"Carregadas {len(tabelas)} tabelas RAW")
            else:
                QMessageBox.warning(self, "Aviso", "Nenhuma tabela RAW encontrada")
                
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao carregar tabelas:\n{e}")
    
    def atualizar_preview(self, tabela_nome):
        """Atualiza o preview dos dados quando seleciona uma tabela"""
        if not tabela_nome:
            return
            
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            # Busca informações básicas da tabela
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total_registros,
                    MIN(data_processamento) as data_minima,
                    MAX(data_processamento) as data_maxima
                FROM "{tabela_nome}";
            """)
            info = cur.fetchone()
            
            # Busca alguns registros para preview
            cur.execute(f"""
                SELECT 
                    id,
                    data_processamento,
                    payload->>'status' as status,
                    payload->'comprador'->>'nome' as comprador
                FROM "{tabela_nome}"
                ORDER BY id DESC
                LIMIT 50;
            """)
            preview_data = cur.fetchall()
            
            cur.close()
            conn.close()
            
            # Extrai cliente_id para exibir
            try:
                cliente_id = self._extrair_cliente_id_da_tabela(tabela_nome)
                cliente_info = f"Cliente ID: {cliente_id}"
            except:
                cliente_info = "Cliente ID: Não identificado"
            
            # Atualiza informações
            info_text = f"""
Tabela: {tabela_nome}
Total de registros: {info[0]:,}
Período: {info[1]} até {info[2]}
{cliente_info}
            """.strip()
            
            self.info_text.setPlainText(info_text)
            
            # Atualiza preview
            self.tabela_preview.setRowCount(len(preview_data))
            for i, row in enumerate(preview_data):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    self.tabela_preview.setItem(i, j, item)
                    
        except Exception as e:
            self.info_text.setPlainText(f"Erro ao carregar preview: {str(e)}")
    
    def processar_bronze(self):
        """Inicia o processamento para bronze"""
        tabela_raw = self.tabela_raw_combo.currentText()
        if not tabela_raw:
            QMessageBox.warning(self, "Atenção", "Selecione uma tabela RAW para processar")
            return
        
        # Extrai cliente_id do nome da tabela
        try:
            cliente_id = self._extrair_cliente_id_da_tabela(tabela_raw)
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Não foi possível identificar o Cliente ID da tabela:\n{e}")
            return
        
        resposta = QMessageBox.question(
            self,
            "Confirmar Processamento",
            f"""
Deseja processar a tabela {tabela_raw} para Bronze?

Cliente ID: {cliente_id}
Transformação: RAW → BRONZE

Isso criará uma nova tabela bronze com dados normalizados.
            """.strip(),
            QMessageBox.Yes | QMessageBox.No
        )
        
        if resposta == QMessageBox.Yes:
            # Cria diálogo de progresso
            progress_dialog = QProgressDialog("Iniciando processamento Bronze...", "Cancelar", 0, 100, self)
            progress_dialog.setWindowTitle("Processando Bronze")
            progress_dialog.setWindowModality(Qt.WindowModal)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.setValue(0)
            
            # Cria thread de processamento
            self.bronze_worker = BronzeProcessor(tabela_raw, cliente_id)
            
            def on_progress(mensagem, atual, total):
                progress_dialog.setLabelText(mensagem)
                progress_dialog.setMaximum(total)
                progress_dialog.setValue(atual)
                from PySide6.QtWidgets import QApplication
                QApplication.processEvents()
            
            def on_finished(mensagem):
                progress_dialog.close()
                QMessageBox.information(self, "Sucesso", mensagem)
                self.atualizar_preview(tabela_raw)  # Atualiza preview
            
            def on_error(mensagem):
                progress_dialog.close()
                QMessageBox.critical(self, "Erro", mensagem)
            
            self.bronze_worker.progress.connect(on_progress)
            self.bronze_worker.finished.connect(on_finished)
            self.bronze_worker.error.connect(on_error)
            
            # Conecta cancelamento
            progress_dialog.canceled.connect(self.bronze_worker.terminate)
            
            # Inicia processamento
            self.bronze_worker.start()