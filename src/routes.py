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
)
from PySide6.QtCore import Qt


# ===============================
#  Funções auxiliares de log
# ===============================
LOG_FILE = "erros_rotas.log"


def registrar_erro(contexto, erro):
    """Grava o erro no arquivo erros_rotas.log com data/hora e stacktrace."""
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
#  Interface principal
# ===============================
class APIRoutesManager(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gerenciador de Rotas de API")
        self.resize(1100, 600)
        self._atualizando_campos = False  # Flag para evitar loop ao preencher campos

        layout = QVBoxLayout()

        # === SEÇÃO DE FORMULÁRIO ===
        form_layout = QVBoxLayout()

        # Linha 1: Cliente ID e Nome da Rota
        row1 = QHBoxLayout()
        self.cliente_combo = QComboBox()
        self.cliente_combo.setEditable(False)
        self.cliente_combo.setMinimumWidth(200)
        self.cliente_combo.currentIndexChanged.connect(self.filtrar_por_cliente)
        self.nome_rota_input = QLineEdit()
        self.nome_rota_input.setPlaceholderText("Nome da Rota (ex: pedidos, produtos)")

        row1.addWidget(QLabel("Cliente:"))
        row1.addWidget(self.cliente_combo)
        row1.addWidget(QLabel("Nome da Rota:"))
        row1.addWidget(self.nome_rota_input)
        form_layout.addLayout(row1)

        # Linha 2: URL e Método HTTP
        row2 = QHBoxLayout()
        self.url_input = QLineEdit()
        self.metodo_combo = QComboBox()
        self.url_input.setPlaceholderText(
            "URL (ex: https://api.com/v1/{cliente_id}/pedidos)"
        )
        self.metodo_combo.addItems(["GET", "POST", "PUT", "DELETE", "PATCH"])

        row2.addWidget(QLabel("URL:"))
        row2.addWidget(self.url_input, stretch=3)
        row2.addWidget(QLabel("Método:"))
        row2.addWidget(self.metodo_combo, stretch=1)
        form_layout.addLayout(row2)

        # Linha 3: Headers (área de texto)
        row3 = QVBoxLayout()
        row3.addWidget(QLabel("Headers (JSON):"))
        self.headers_input = QTextEdit()
        self.headers_input.setPlaceholderText(
            '{\n  "Authorization": "Bearer {token}",\n  "Accept": "application/json",\n  "Content-Type": "application/json"\n}'
        )
        self.headers_input.setMaximumHeight(100)
        row3.addWidget(self.headers_input)
        form_layout.addLayout(row3)

        # Botões de ação
        btn_layout = QHBoxLayout()
        btn_salvar = QPushButton("💾 Salvar Nova Rota")
        btn_excluir = QPushButton("🗑️ Excluir Rota")
        btn_recarregar = QPushButton("🔄 Recarregar Lista")
        btn_salvar_edicao = QPushButton("✅ Salvar Edições da Tabela")

        btn_salvar.clicked.connect(self.salvar_rota)
        btn_excluir.clicked.connect(self.excluir_rota)
        btn_recarregar.clicked.connect(self.recarregar_tudo)
        btn_salvar_edicao.clicked.connect(self.salvar_edicoes_tabela)

        btn_layout.addWidget(btn_salvar)
        btn_layout.addWidget(btn_excluir)
        btn_layout.addWidget(btn_salvar_edicao)
        btn_layout.addWidget(btn_recarregar)
        btn_layout.addStretch()

        form_layout.addLayout(btn_layout)
        layout.addLayout(form_layout)

        # === TABELA ===
        self.tabela = QTableWidget()
        self.tabela.setColumnCount(8)
        self.tabela.setHorizontalHeaderLabels(
            [
                "ID",
                "Cliente ID",
                "Nome Rota",
                "URL",
                "Método",
                "Headers",
                "Ativo",
                "Criado em",
            ]
        )
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.cellClicked.connect(self.preencher_campos)
        self.tabela.setEditTriggers(
            QAbstractItemView.DoubleClicked | QAbstractItemView.SelectedClicked
        )

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
        """Cria a tabela de rotas de API se não existir"""
        try:
            conn = self.conectar()
            cur = conn.cursor()

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
                
                -- Trigger para atualizar timestamp
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

    def carregar_clientes(self):
        """Carrega os clientes cadastrados no dropdown"""
        try:
            self._atualizando_campos = True  # Previne que o filtro seja acionado durante o carregamento
            
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
                # Mostra apenas clientes ativos
                if ativo:
                    self.cliente_combo.addItem(str(cliente_id), cliente_id)
            
            cur.close()
            conn.close()
            
            self._atualizando_campos = False  # Libera para receber novos eventos
        except Exception as e:
            registrar_erro("carregar_clientes", e)
            self._atualizando_campos = False

    def recarregar_tudo(self):
        """Recarrega tanto os clientes quanto os dados da tabela"""
        self.carregar_clientes()
        self.carregar_dados()

    def filtrar_por_cliente(self):
        """Filtra as rotas exibidas pelo cliente selecionado"""
        if self._atualizando_campos:
            return  # Evita execução durante preenchimento de campos
        
        cliente_id = self.cliente_combo.currentData()
        if cliente_id is None:
            self.carregar_dados(todos=True)
        else:
            self.carregar_dados(cliente_id=cliente_id)

    def carregar_dados(self, cliente_id=None, todos=False):
        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            # Se um cliente específico foi selecionado, filtra apenas suas rotas
            if cliente_id is not None:
                cur.execute("""
                    SELECT id, cliente_id, nome_rota, url, metodo_http, headers, ativo, criado_em 
                    FROM clientes_api_rotas 
                    WHERE cliente_id = %s
                    ORDER BY nome_rota;
                """, (cliente_id,))
            else:
                cur.execute("""
                    SELECT id, cliente_id, nome_rota, url, metodo_http, headers, ativo, criado_em 
                    FROM clientes_api_rotas 
                    ORDER BY cliente_id, nome_rota;
                """)
            
            dados = cur.fetchall()

            self.tabela.setRowCount(len(dados))
            for i, row in enumerate(dados):
                try:
                    for j, value in enumerate(row):
                        # Coluna Headers (5) - mostra JSON formatado
                        if j == 5:
                            headers_str = json.dumps(value, indent=2) if value else "{}"
                            item = QTableWidgetItem(headers_str)
                        # Coluna Ativo (6) - checkbox
                        elif j == 6:
                            item = QTableWidgetItem()
                            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
                            item.setCheckState(Qt.Checked if value else Qt.Unchecked)
                        else:
                            item = QTableWidgetItem(str(value))

                        # Apenas algumas colunas são editáveis
                        if j not in [2, 3, 4, 5]:  # nome_rota, url, metodo, headers
                            item.setFlags(item.flags() & ~Qt.ItemIsEditable)

                        self.tabela.setItem(i, j, item)
                except Exception as e:
                    registrar_erro(
                        "carregar_dados (linha corrompida)", f"{e}\nLinha: {row}"
                    )
                    raise

            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_dados", e)
            QMessageBox.critical(self, "Erro", f"Erro ao carregar dados:\n{e}")

    def salvar_rota(self):
        """Salva uma nova rota de API"""
        # Obtém o cliente selecionado
        cliente_id = self.cliente_combo.currentData()
        if cliente_id is None:
            QMessageBox.warning(
                self, "Atenção", "Selecione um cliente."
            )
            return
        
        cliente_id = str(cliente_id)
        nome_rota = self.nome_rota_input.text().strip()
        url = self.url_input.text().strip()
        metodo = self.metodo_combo.currentText()
        headers_text = self.headers_input.toPlainText().strip()

        # Validações
        if not nome_rota or not url:
            QMessageBox.warning(self, "Atenção", "Nome da rota e URL são obrigatórios.")
            return

        # Valida e processa headers
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

            # Salva a rota
            cur.execute(
                """
                INSERT INTO clientes_api_rotas (cliente_id, nome_rota, url, metodo_http, headers)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cliente_id, nome_rota)
                DO UPDATE SET 
                    url = EXCLUDED.url,
                    metodo_http = EXCLUDED.metodo_http,
                    headers = EXCLUDED.headers,
                    atualizado_em = NOW();
            """,
                (int(cliente_id), nome_rota, url, metodo, json.dumps(headers_json)),
            )

            conn.commit()
            cur.close()
            conn.close()

            QMessageBox.information(self, "Sucesso", "Rota de API salva com sucesso!")
            self.limpar_campos()
            self.recarregar_tudo()

        except Exception as e:
            registrar_erro("salvar_rota", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar rota:\n{e}")

    def salvar_edicoes_tabela(self):
        """Salva todas as alterações feitas diretamente na tabela"""
        try:
            conn = self.conectar()
            cur = conn.cursor()

            alteracoes = 0
            erros = []

            for row in range(self.tabela.rowCount()):
                try:
                    id_registro = int(self.tabela.item(row, 0).text())
                    nome_rota = self.tabela.item(row, 2).text().strip()
                    url = self.tabela.item(row, 3).text().strip()
                    metodo = self.tabela.item(row, 4).text().strip()
                    headers_text = self.tabela.item(row, 5).text().strip()
                    ativo = self.tabela.item(row, 6).checkState() == Qt.Checked

                    if not nome_rota or not url:
                        erros.append(
                            f"Linha {row + 1}: Nome da rota e URL são obrigatórios"
                        )
                        continue

                    # Valida headers
                    try:
                        headers_json = json.loads(headers_text) if headers_text else {}
                    except json.JSONDecodeError:
                        erros.append(f"Linha {row + 1}: Headers JSON inválido")
                        continue

                    cur.execute(
                        """
                        UPDATE clientes_api_rotas 
                        SET nome_rota = %s, url = %s, metodo_http = %s, 
                            headers = %s, ativo = %s, atualizado_em = NOW()
                        WHERE id = %s;
                    """,
                        (
                            nome_rota,
                            url,
                            metodo,
                            json.dumps(headers_json),
                            ativo,
                            id_registro,
                        ),
                    )

                    alteracoes += 1

                except Exception as e:
                    erros.append(f"Linha {row + 1}: {str(e)}")

            conn.commit()
            cur.close()
            conn.close()

            mensagem = f"✅ {alteracoes} registro(s) atualizado(s) com sucesso!"
            if erros:
                mensagem += "\n\n⚠️ Erros encontrados:\n" + "\n".join(erros)

            QMessageBox.information(self, "Resultado", mensagem)
            self.carregar_dados()

        except Exception as e:
            registrar_erro("salvar_edicoes_tabela", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar edições:\n{e}")

    def excluir_rota(self):
        cliente_id = self.cliente_combo.currentData()
        nome_rota = self.nome_rota_input.text().strip()

        if cliente_id is None or not nome_rota:
            QMessageBox.warning(
                self, "Atenção", "Selecione um cliente e informe o nome da rota."
            )
            return

        resposta = QMessageBox.question(
            self,
            "Confirmação",
            f"Deseja realmente excluir a rota '{nome_rota}' do cliente {cliente_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            try:
                conn = self.conectar()
                cur = conn.cursor()
                cur.execute(
                    "DELETE FROM clientes_api_rotas WHERE cliente_id = %s AND nome_rota = %s;",
                    (int(cliente_id), nome_rota),
                )
                conn.commit()
                cur.close()
                conn.close()

                QMessageBox.information(self, "Sucesso", "Rota removida com sucesso.")
                self.limpar_campos()
                self.recarregar_tudo()
            except Exception as e:
                registrar_erro("excluir_rota", e)
                QMessageBox.critical(self, "Erro", f"Erro ao excluir rota:\n{e}")

    def preencher_campos(self, row, _):
        """Preenche os campos do formulário com os dados da linha clicada"""
        self._atualizando_campos = True  # Previne que o filtro seja acionado
        
        cliente_id = self.tabela.item(row, 1).text()
        nome_rota = self.tabela.item(row, 2).text()
        url = self.tabela.item(row, 3).text()
        metodo = self.tabela.item(row, 4).text()
        headers = self.tabela.item(row, 5).text()

        # Define o cliente selecionado no combo box
        index = self.cliente_combo.findData(int(cliente_id))
        if index >= 0:
            self.cliente_combo.setCurrentIndex(index)
        
        self.nome_rota_input.setText(nome_rota)
        self.url_input.setText(url)
        self.metodo_combo.setCurrentText(metodo)
        self.headers_input.setPlainText(headers)
        
        self._atualizando_campos = False  # Libera para receber novos eventos

    def limpar_campos(self):
        """Limpa todos os campos do formulário"""
        self.cliente_combo.setCurrentIndex(0)
        self.nome_rota_input.clear()
        self.url_input.clear()
        # self.metodo_combo.setCurrentIndex(0)  # Mantém o método HTTP selecionado
        self.headers_input.clear()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    janela = APIRoutesManager()
    janela.show()
    sys.exit(app.exec())
