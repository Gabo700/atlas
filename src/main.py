import os
from dotenv import load_dotenv
import sys
import psycopg2
import traceback
import json
from datetime import datetime
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
    QTextEdit,
    QComboBox,
    QTabWidget,
)
from PySide6.QtCore import Qt

# Imports dos módulos de lógica
from scraps import ScrapsManager

# ===============================
#  Funções auxiliares de log
# ===============================
LOG_FILE = "erros.log"

def registrar_erro(contexto, erro):
    """Grava o erro no arquivo erros.log com data/hora e stacktrace."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n" + "=" * 80 + "\n")
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERRO EM: {contexto}\n")
        f.write(str(erro) + "\n")
        f.write(traceback.format_exc())
        f.write("\n" + "=" * 80 + "\n")
    print(f" ERRO ({contexto}): {erro}")

# ===============================
#  Configuração do banco
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
#  Interface Principal com Abas
# ===============================
class AtlasDataFlowManager(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Atlas Data Flow - Gerenciador Completo")
        self.resize(1200, 700)

        # Layout principal
        layout = QVBoxLayout()
        
        # Cria as abas
        tabs = QTabWidget()
        
        # Aba 1: Gerenciamento de Clientes e Tokens
        self.tab_clientes = QWidget()
        self.setup_tab_clientes()
        tabs.addTab(self.tab_clientes, "Clientes & Tokens")
        
        # Aba 2: Gerenciamento de Rotas de API
        self.tab_rotas = QWidget()
        self.setup_tab_rotas()
        tabs.addTab(self.tab_rotas, "Rotas de API")
        
        # Aba 3: Scraps de ETL (usa classe externa)
        self.scraps_manager = ScrapsManager()
        tabs.addTab(self.scraps_manager, "Scraps de ETL")
        
        layout.addWidget(tabs)
        self.setLayout(layout)
        
        # Inicializa as funções das abas
        self.inicializar_banco()
        self.carregar_dados_clientes()
        self.carregar_clientes_combo()

    def conectar(self):
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding("UTF8")
        return conn

    def inicializar_banco(self):
        """Inicializa as tabelas do banco de dados"""
        try:
            conn = self.conectar()
            cur = conn.cursor()

            # Cria tabela de rotas de API se não existir
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes_api_rotas (
                    id SERIAL PRIMARY KEY,
                    cliente_id INTEGER NOT NULL REFERENCES clientes_tokens(cliente_id) ON DELETE CASCADE,
                    nome_rota TEXT NOT NULL,
                    url TEXT NOT NULL,
                    metodo_http TEXT DEFAULT 'GET',
                    headers JSONB NOT NULL DEFAULT '{}',
                    ativo BOOLEAN DEFAULT TRUE,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    atualizado_em TIMESTAMP DEFAULT NOW(),
                    UNIQUE(cliente_id, nome_rota)
                );
                
                CREATE INDEX IF NOT EXISTS idx_api_rotas_cliente 
                    ON clientes_api_rotas(cliente_id);
                
                CREATE INDEX IF NOT EXISTS idx_api_rotas_ativo 
                    ON clientes_api_rotas(ativo) WHERE ativo = TRUE;
                
                CREATE OR REPLACE FUNCTION atualiza_timestamp_api_rotas() 
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.atualizado_em = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                
                DROP TRIGGER IF EXISTS trg_atualiza_api_rotas ON clientes_api_rotas;
                
                CREATE TRIGGER trg_atualiza_api_rotas 
                    BEFORE UPDATE ON clientes_api_rotas
                    FOR EACH ROW 
                    EXECUTE FUNCTION atualiza_timestamp_api_rotas();
            """)

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("inicializar_banco", e)

    # ===============================
    #  TAB 1: Clientes e Tokens
    # ===============================
    def setup_tab_clientes(self):
        layout = QVBoxLayout()
        
        # Formulário
        form_layout = QHBoxLayout()
        self.cliente_id_input = QLineEdit()
        self.token_input = QLineEdit()
        self.cliente_id_input.setPlaceholderText("ID do Cliente (ex: 2151)")
        self.token_input.setPlaceholderText("Token do Cliente")
        
        btn_salvar_cli = QPushButton("Salvar Novo")
        btn_excluir_cli = QPushButton("Excluir")
        btn_salvar_cli.clicked.connect(self.salvar_token)
        btn_excluir_cli.clicked.connect(self.excluir_cliente)
        
        form_layout.addWidget(QLabel("Cliente ID:"))
        form_layout.addWidget(self.cliente_id_input)
        form_layout.addWidget(QLabel("Token:"))
        form_layout.addWidget(self.token_input)
        form_layout.addWidget(btn_salvar_cli)
        form_layout.addWidget(btn_excluir_cli)
        layout.addLayout(form_layout)
        
        # Botões da tabela
        btn_layout = QHBoxLayout()
        btn_recarregar_cli = QPushButton("Recarregar Lista")
        btn_salvar_edicao_cli = QPushButton("Salvar Edições da Tabela")
        btn_recarregar_cli.clicked.connect(self.carregar_dados_clientes)
        btn_salvar_edicao_cli.clicked.connect(self.salvar_edicoes_tabela_clientes)
        btn_layout.addWidget(btn_salvar_edicao_cli)
        btn_layout.addWidget(btn_recarregar_cli)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # Tabela
        self.tabela_clientes = QTableWidget()
        self.tabela_clientes.setColumnCount(6)
        self.tabela_clientes.setHorizontalHeaderLabels(
            ["ID", "Cliente ID", "Token", "Ativo", "Criado em", "Atualizado em"]
        )
        self.tabela_clientes.horizontalHeader().setStretchLastSection(True)
        self.tabela_clientes.cellClicked.connect(self.preencher_campos_clientes)
        self.tabela_clientes.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.SelectedClicked)
        layout.addWidget(self.tabela_clientes)
        
        self.tab_clientes.setLayout(layout)

    def carregar_dados_clientes(self):
        try:
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute(
                "SELECT id, cliente_id, token, ativo, criado_em, atualizado_em FROM clientes_tokens ORDER BY id DESC;"
            )
            dados = cur.fetchall()

            self.tabela_clientes.setRowCount(len(dados))
            for i, row in enumerate(dados):
                try:
                    for j, value in enumerate(row):
                        if j == 3:
                            item = QTableWidgetItem()
                            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
                            item.setCheckState(Qt.Checked if value else Qt.Unchecked)
                        else:
                            item = QTableWidgetItem(str(value))
                            if j not in [1, 2]:
                                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                        self.tabela_clientes.setItem(i, j, item)
                except Exception as e:
                    registrar_erro("carregar_dados_clientes (linha corrompida)", f"{e}\nLinha: {row}")
                    raise

            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_dados_clientes", e)
            QMessageBox.critical(self, "Erro", f"Erro ao carregar dados:\n{e}")

    def salvar_token(self):
        cliente_id = self.cliente_id_input.text().strip()
        token = self.token_input.text().strip()

        if not cliente_id.isdigit() or not token:
            QMessageBox.warning(self, "Atenção", "Informe um ID numérico e um token válido.")
            return

        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            cur.execute(
                """
                INSERT INTO clientes_tokens (cliente_id, token)
                VALUES (%s, %s)
                ON CONFLICT (cliente_id)
                DO UPDATE SET token = EXCLUDED.token, atualizado_em = NOW();
                """,
                (int(cliente_id), token),
            )
            
            conn.commit()
            cur.close()
            conn.close()
            
            QMessageBox.information(self, "Sucesso", "Token salvo com sucesso!")
            self.cliente_id_input.clear()
            self.token_input.clear()
            self.carregar_dados_clientes()
            self.carregar_clientes_combo()  # Atualiza o combo da aba de rotas
        except Exception as e:
            registrar_erro("salvar_token", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar token:\n{e}")

    def excluir_cliente(self):
        cliente_id = self.cliente_id_input.text().strip()
        if not cliente_id.isdigit():
            QMessageBox.warning(self, "Atenção", "Informe um ID numérico válido.")
            return

        resposta = QMessageBox.question(
            self, "Confirmação",
            f"Deseja realmente excluir o cliente {cliente_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            try:
                conn = self.conectar()
                cur = conn.cursor()
                cur.execute("DELETE FROM clientes_tokens WHERE cliente_id = %s;", (int(cliente_id),))
                conn.commit()
                cur.close()
                conn.close()
                QMessageBox.information(self, "Sucesso", "Cliente removido com sucesso.")
                self.cliente_id_input.clear()
                self.token_input.clear()
                self.carregar_dados_clientes()
                self.carregar_clientes_combo()  # Atualiza o combo da aba de rotas
            except Exception as e:
                registrar_erro("excluir_cliente", e)
                QMessageBox.critical(self, "Erro", f"Erro ao excluir cliente:\n{e}")

    def preencher_campos_clientes(self, row, _):
        cliente_id = self.tabela_clientes.item(row, 1).text()
        token = self.tabela_clientes.item(row, 2).text()
        self.cliente_id_input.setText(cliente_id)
        self.token_input.setText(token)

    def salvar_edicoes_tabela_clientes(self):
        try:
            conn = self.conectar()
            cur = conn.cursor()
            alteracoes = 0
            erros = []
            
            for row in range(self.tabela_clientes.rowCount()):
                try:
                    id_registro = int(self.tabela_clientes.item(row, 0).text())
                    cliente_id = self.tabela_clientes.item(row, 1).text().strip()
                    token = self.tabela_clientes.item(row, 2).text().strip()
                    ativo = self.tabela_clientes.item(row, 3).checkState() == Qt.Checked
                    
                    if not cliente_id.isdigit() or not token:
                        erros.append(f"Linha {row + 1}: ID ou token inválido")
                        continue
                    
                    cur.execute(
                        """UPDATE clientes_tokens 
                        SET cliente_id = %s, token = %s, ativo = %s, atualizado_em = NOW()
                        WHERE id = %s;""",
                        (int(cliente_id), token, ativo, id_registro),
                    )
                    alteracoes += 1
                except Exception as e:
                    erros.append(f"Linha {row + 1}: {str(e)}")
            
            conn.commit()
            cur.close()
            conn.close()
            
            mensagem = f"{alteracoes} registro(s) atualizado(s) com sucesso!"
            if erros:
                mensagem += "\n\n Erros encontrados:\n" + "\n".join(erros)
            QMessageBox.information(self, "Resultado", mensagem)
            self.carregar_dados_clientes()
            self.carregar_clientes_combo()
        except Exception as e:
            registrar_erro("salvar_edicoes_tabela_clientes", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar edições:\n{e}")

    # ===============================
    #  TAB 2: Rotas de API
    # ===============================
    def setup_tab_rotas(self):
        layout = QVBoxLayout()
        
        # Formulário
        form_layout = QVBoxLayout()
        
        # Linha 1
        row1 = QHBoxLayout()
        self.cliente_combo_rotas = QComboBox()
        self.cliente_combo_rotas.setEditable(False)
        self.cliente_combo_rotas.setMinimumWidth(200)
        self.cliente_combo_rotas.currentIndexChanged.connect(self.filtrar_rotas_por_cliente)
        self.nome_rota_input = QLineEdit()
        self.nome_rota_input.setPlaceholderText("Nome da Rota (ex: pedidos, produtos)")
        row1.addWidget(QLabel("Cliente:"))
        row1.addWidget(self.cliente_combo_rotas)
        row1.addWidget(QLabel("Nome da Rota:"))
        row1.addWidget(self.nome_rota_input)
        form_layout.addLayout(row1)
        
        # Linha 2
        row2 = QHBoxLayout()
        self.url_input = QLineEdit()
        self.metodo_combo = QComboBox()
        self.url_input.setPlaceholderText("URL (ex: https://api.com/v1/{cliente_id}/pedidos)")
        self.metodo_combo.addItems(["GET", "POST", "PUT", "DELETE", "PATCH"])
        row2.addWidget(QLabel("URL:"))
        row2.addWidget(self.url_input, stretch=3)
        row2.addWidget(QLabel("Método:"))
        row2.addWidget(self.metodo_combo, stretch=1)
        form_layout.addLayout(row2)
        
        # Linha 3
        row3 = QVBoxLayout()
        row3.addWidget(QLabel("Headers (JSON):"))
        self.headers_input = QTextEdit()
        self.headers_input.setPlaceholderText(
            '{\n  "Authorization": "Bearer {token}",\n  "Accept": "application/json",\n  "Content-Type": "application/json"\n}'
        )
        self.headers_input.setMaximumHeight(100)
        row3.addWidget(self.headers_input)
        form_layout.addLayout(row3)
        
        # Botões
        btn_layout = QHBoxLayout()
        btn_salvar_rota = QPushButton("Salvar Nova Rota")
        btn_excluir_rota = QPushButton("Excluir Rota")
        btn_recarregar_rotas = QPushButton("Recarregar Lista")
        btn_salvar_edicao_rota = QPushButton("Salvar Edições da Tabela")
        
        btn_salvar_rota.clicked.connect(self.salvar_rota)
        btn_excluir_rota.clicked.connect(self.excluir_rota)
        btn_recarregar_rotas.clicked.connect(self.recarregar_rotas)
        btn_salvar_edicao_rota.clicked.connect(self.salvar_edicoes_tabela_rotas)
        
        btn_layout.addWidget(btn_salvar_rota)
        btn_layout.addWidget(btn_excluir_rota)
        btn_layout.addWidget(btn_salvar_edicao_rota)
        btn_layout.addWidget(btn_recarregar_rotas)
        btn_layout.addStretch()
        
        form_layout.addLayout(btn_layout)
        layout.addLayout(form_layout)
        
        # Tabela
        self.tabela_rotas = QTableWidget()
        self.tabela_rotas.setColumnCount(8)
        self.tabela_rotas.setHorizontalHeaderLabels(
            ["ID", "Cliente ID", "Nome Rota", "URL", "Método", "Headers", "Ativo", "Criado em"]
        )
        self.tabela_rotas.horizontalHeader().setStretchLastSection(True)
        self.tabela_rotas.cellClicked.connect(self.preencher_campos_rotas)
        self.tabela_rotas.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.SelectedClicked)
        layout.addWidget(self.tabela_rotas)
        
        self.tab_rotas.setLayout(layout)
        self._atualizando_campos_rotas = False

    def carregar_clientes_combo(self):
        try:
            self._atualizando_campos_rotas = True
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute("SELECT cliente_id, ativo FROM clientes_tokens ORDER BY cliente_id;")
            clientes = cur.fetchall()
            
            self.cliente_combo_rotas.clear()
            
            for cliente_id, ativo in clientes:
                if ativo:
                    self.cliente_combo_rotas.addItem(str(cliente_id), cliente_id)
            
            cur.close()
            conn.close()
            self._atualizando_campos_rotas = False
        except Exception as e:
            registrar_erro("carregar_clientes_combo", e)
            self._atualizando_campos_rotas = False

    def filtrar_rotas_por_cliente(self):
        if self._atualizando_campos_rotas:
            return
        cliente_id = self.cliente_combo_rotas.currentData()
        if cliente_id is None:
            self.carregar_dados_rotas()
        else:
            self.carregar_dados_rotas(cliente_id=cliente_id)

    def carregar_dados_rotas(self, cliente_id=None):
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            if cliente_id is not None:
                cur.execute("""
                    SELECT id, cliente_id, nome_rota, url, metodo_http, headers, ativo, criado_em 
                    FROM clientes_api_rotas WHERE cliente_id = %s ORDER BY nome_rota;
                """, (cliente_id,))
            else:
                cur.execute("""
                    SELECT id, cliente_id, nome_rota, url, metodo_http, headers, ativo, criado_em 
                    FROM clientes_api_rotas ORDER BY cliente_id, nome_rota;
                """)
            
            dados = cur.fetchall()
            self.tabela_rotas.setRowCount(len(dados))
            
            for i, row in enumerate(dados):
                try:
                    for j, value in enumerate(row):
                        if j == 5:
                            headers_str = json.dumps(value, indent=2) if value else "{}"
                            item = QTableWidgetItem(headers_str)
                        elif j == 6:
                            item = QTableWidgetItem()
                            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
                            item.setCheckState(Qt.Checked if value else Qt.Unchecked)
                        else:
                            item = QTableWidgetItem(str(value))
                        if j not in [2, 3, 4, 5]:
                            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                        self.tabela_rotas.setItem(i, j, item)
                except Exception as e:
                    registrar_erro("carregar_dados_rotas (linha corrompida)", f"{e}\nLinha: {row}")
                    raise

            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_dados_rotas", e)
            QMessageBox.critical(self, "Erro", f"Erro ao carregar dados:\n{e}")

    def salvar_rota(self):
        cliente_id = self.cliente_combo_rotas.currentData()
        if cliente_id is None:
            QMessageBox.warning(self, "Atenção", "Selecione um cliente.")
            return
        
        nome_rota = self.nome_rota_input.text().strip()
        url = self.url_input.text().strip()
        metodo = self.metodo_combo.currentText()
        headers_text = self.headers_input.toPlainText().strip()

        if not nome_rota or not url:
            QMessageBox.warning(self, "Atenção", "Nome da rota e URL são obrigatórios.")
            return

        try:
            if headers_text:
                headers_json = json.loads(headers_text)
            else:
                headers_json = {
                    "Authorization": "Bearer {token}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
        except json.JSONDecodeError as e:
            QMessageBox.warning(self, "Erro no JSON", f"Headers inválido:\n{e}")
            return

        try:
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO clientes_api_rotas (cliente_id, nome_rota, url, metodo_http, headers)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cliente_id, nome_rota)
                DO UPDATE SET url = EXCLUDED.url, metodo_http = EXCLUDED.metodo_http,
                    headers = EXCLUDED.headers, atualizado_em = NOW();
            """, (int(cliente_id), nome_rota, url, metodo, json.dumps(headers_json)))
            conn.commit()
            cur.close()
            conn.close()
            QMessageBox.information(self, "Sucesso", "Rota de API salva com sucesso!")
            self.limpar_campos_rotas()
            self.recarregar_rotas()
        except Exception as e:
            registrar_erro("salvar_rota", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar rota:\n{e}")

    def excluir_rota(self):
        cliente_id = self.cliente_combo_rotas.currentData()
        nome_rota = self.nome_rota_input.text().strip()
        if cliente_id is None or not nome_rota:
            QMessageBox.warning(self, "Atenção", "Selecione um cliente e informe o nome da rota.")
            return

        resposta = QMessageBox.question(
            self, "Confirmação",
            f"Deseja realmente excluir a rota '{nome_rota}' do cliente {cliente_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            try:
                conn = self.conectar()
                cur = conn.cursor()
                cur.execute("DELETE FROM clientes_api_rotas WHERE cliente_id = %s AND nome_rota = %s;",
                          (int(cliente_id), nome_rota))
                conn.commit()
                cur.close()
                conn.close()
                QMessageBox.information(self, "Sucesso", "Rota removida com sucesso.")
                self.limpar_campos_rotas()
                self.recarregar_rotas()
            except Exception as e:
                registrar_erro("excluir_rota", e)
                QMessageBox.critical(self, "Erro", f"Erro ao excluir rota:\n{e}")

    def recarregar_rotas(self):
        self.carregar_clientes_combo()
        self.carregar_dados_rotas()

    def preencher_campos_rotas(self, row, _):
        self._atualizando_campos_rotas = True
        cliente_id = self.tabela_rotas.item(row, 1).text()
        nome_rota = self.tabela_rotas.item(row, 2).text()
        url = self.tabela_rotas.item(row, 3).text()
        metodo = self.tabela_rotas.item(row, 4).text()
        headers = self.tabela_rotas.item(row, 5).text()

        index = self.cliente_combo_rotas.findData(int(cliente_id))
        if index >= 0:
            self.cliente_combo_rotas.setCurrentIndex(index)
        
        self.nome_rota_input.setText(nome_rota)
        self.url_input.setText(url)
        self.metodo_combo.setCurrentText(metodo)
        self.headers_input.setPlainText(headers)
        self._atualizando_campos_rotas = False

    def limpar_campos_rotas(self):
        self.cliente_combo_rotas.setCurrentIndex(0)
        self.nome_rota_input.clear()
        self.url_input.clear()
        self.headers_input.clear()

    def salvar_edicoes_tabela_rotas(self):
        try:
            conn = self.conectar()
            cur = conn.cursor()
            alteracoes = 0
            erros = []
            
            for row in range(self.tabela_rotas.rowCount()):
                try:
                    id_registro = int(self.tabela_rotas.item(row, 0).text())
                    nome_rota = self.tabela_rotas.item(row, 2).text().strip()
                    url = self.tabela_rotas.item(row, 3).text().strip()
                    metodo = self.tabela_rotas.item(row, 4).text().strip()
                    headers_text = self.tabela_rotas.item(row, 5).text().strip()
                    ativo = self.tabela_rotas.item(row, 6).checkState() == Qt.Checked
                    
                    if not nome_rota or not url:
                        erros.append(f"Linha {row + 1}: Nome da rota e URL são obrigatórios")
                        continue
                    
                    try:
                        headers_json = json.loads(headers_text) if headers_text else {}
                    except json.JSONDecodeError:
                        erros.append(f"Linha {row + 1}: Headers JSON inválido")
                        continue
                    
                    cur.execute("""
                        UPDATE clientes_api_rotas 
                        SET nome_rota = %s, url = %s, metodo_http = %s, 
                            headers = %s, ativo = %s, atualizado_em = NOW()
                        WHERE id = %s;
                    """, (nome_rota, url, metodo, json.dumps(headers_json), ativo, id_registro))
                    alteracoes += 1
                except Exception as e:
                    erros.append(f"Linha {row + 1}: {str(e)}")
            
            conn.commit()
            cur.close()
            conn.close()
            
            mensagem = f"{alteracoes} registro(s) atualizado(s) com sucesso!"
            if erros:
                mensagem += "\n\n Erros encontrados:\n" + "\n".join(erros)
            QMessageBox.information(self, "Resultado", mensagem)
            self.carregar_dados_rotas()
        except Exception as e:
            registrar_erro("salvar_edicoes_tabela_rotas", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar edições:\n{e}")



if __name__ == "__main__":
    app = QApplication(sys.argv)
    janela = AtlasDataFlowManager()
    janela.show()
    sys.exit(app.exec())
