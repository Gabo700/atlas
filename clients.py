import os
from dotenv import load_dotenv
import sys
import psycopg2
import traceback
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
)
from PySide6.QtCore import Qt


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
    print(f"ERRO ({contexto}): {erro}")


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
class TokenManager(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gerenciador de Tokens de Clientes")
        self.resize(900, 500)

        layout = QVBoxLayout()
        form_layout = QHBoxLayout()

        # Campos
        self.cliente_id_input = QLineEdit()
        self.token_input = QLineEdit()
        self.cliente_id_input.setPlaceholderText("ID do Cliente (ex: 2151)")
        self.token_input.setPlaceholderText("Token do Cliente")

        # Botões
        btn_salvar = QPushButton("Salvar Novo")
        btn_excluir = QPushButton("Excluir")
        btn_recarregar = QPushButton("Recarregar Lista")
        btn_salvar_edicao = QPushButton("Salvar Edições da Tabela")

        btn_salvar.clicked.connect(self.salvar_token)
        btn_excluir.clicked.connect(self.excluir_cliente)
        btn_recarregar.clicked.connect(self.carregar_dados)
        btn_salvar_edicao.clicked.connect(self.salvar_edicoes_tabela)

        # Layout superior
        form_layout.addWidget(QLabel("Cliente ID:"))
        form_layout.addWidget(self.cliente_id_input)
        form_layout.addWidget(QLabel("Token:"))
        form_layout.addWidget(self.token_input)
        form_layout.addWidget(btn_salvar)
        form_layout.addWidget(btn_excluir)

        layout.addLayout(form_layout)

        # Botões de ação da tabela
        btn_layout = QHBoxLayout()
        btn_layout.addWidget(btn_salvar_edicao)
        btn_layout.addWidget(btn_recarregar)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # Tabela
        self.tabela = QTableWidget()
        self.tabela.setColumnCount(6)
        self.tabela.setHorizontalHeaderLabels(
            ["ID", "Cliente ID", "Token", "Ativo", "Criado em", "Atualizado em"]
        )
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.cellClicked.connect(self.preencher_campos)
        
        # Permite edição apenas nas colunas Cliente ID e Token
        self.tabela.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.SelectedClicked)
        
        layout.addWidget(self.tabela)

        self.setLayout(layout)
        self.carregar_dados()

    def conectar(self):
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding("LATIN1")
        return conn

    def carregar_dados(self):
        try:
            conn = self.conectar()
            cur = conn.cursor()
            cur.execute(
                "SELECT id, cliente_id, token, ativo, criado_em, atualizado_em FROM clientes_tokens ORDER BY id DESC;"
            )
            dados = cur.fetchall()

            self.tabela.setRowCount(len(dados))
            for i, row in enumerate(dados):
                try:
                    for j, value in enumerate(row):
                        # Coluna Ativo (3) usa checkbox
                        if j == 3:
                            item = QTableWidgetItem()
                            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
                            item.setCheckState(Qt.Checked if value else Qt.Unchecked)
                        else:
                            item = QTableWidgetItem(str(value))
                            
                            # Apenas Cliente ID (coluna 1), Token (coluna 2) e Ativo (coluna 3) são editáveis
                            if j not in [1, 2]:
                                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                        
                        self.tabela.setItem(i, j, item)
                except Exception as e:
                    registrar_erro("carregar_dados (linha corrompida)", f"{e}\nLinha: {row}")
                    raise

            cur.close()
            conn.close()
        except Exception as e:
            registrar_erro("carregar_dados", e)
            QMessageBox.critical(self, "Erro", f"Erro ao carregar dados:\n{e}")

    def criar_tabela_cliente(self, cur, cliente_id):
        """Cria tabela otimizada para dados raw do cliente específico"""
        nome_tabela = f"dados_raw_cliente_{cliente_id}"
        
        # Verifica se a tabela já existe
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
            """,
            (nome_tabela,)
        )
        
        if cur.fetchone()[0]:
            return nome_tabela  # Tabela já existe
        
        # Cria a tabela com todas as otimizações
        cur.execute(f"""
            CREATE TABLE {nome_tabela} (
                id BIGSERIAL PRIMARY KEY,
                data_coleta TIMESTAMP DEFAULT NOW() NOT NULL,
                origem TEXT NOT NULL,
                payload JSONB NOT NULL,
                hash_conteudo TEXT,
                CONSTRAINT uq_{nome_tabela}_hash UNIQUE (hash_conteudo)
            );
            
            -- Índice para busca por data (mais recente primeiro)
            CREATE INDEX idx_{nome_tabela}_data 
                ON {nome_tabela} (data_coleta DESC);
            
            -- Índice GIN para buscas JSONB otimizadas
            CREATE INDEX idx_{nome_tabela}_payload_gin 
                ON {nome_tabela} USING GIN (payload jsonb_path_ops);
            
            -- Índice para origem
            CREATE INDEX idx_{nome_tabela}_origem 
                ON {nome_tabela} (origem);
            
            -- Índice composto para consultas frequentes
            CREATE INDEX idx_{nome_tabela}_origem_data 
                ON {nome_tabela} (origem, data_coleta DESC);
            
            -- Índice para hash (já tem UNIQUE mas esse é para performance)
            CREATE INDEX idx_{nome_tabela}_hash 
                ON {nome_tabela} (hash_conteudo) 
                WHERE hash_conteudo IS NOT NULL;
            
            -- Comentário na tabela
            COMMENT ON TABLE {nome_tabela} IS 
                'Dados raw do cliente {cliente_id} - Criado automaticamente';
        """)
        
        return nome_tabela

    def salvar_token(self):
        """Salva um novo token usando os campos de entrada"""
        cliente_id = self.cliente_id_input.text().strip()
        token = self.token_input.text().strip()

        if not cliente_id.isdigit() or not token:
            QMessageBox.warning(
                self, "Atenção", "Informe um ID numérico e um token válido."
            )
            return

        try:
            conn = self.conectar()
            cur = conn.cursor()
            
            # Verifica se é um novo cliente
            cur.execute(
                "SELECT COUNT(*) FROM clientes_tokens WHERE cliente_id = %s;",
                (int(cliente_id),)
            )
            eh_novo = cur.fetchone()[0] == 0
            
            # Salva o token
            cur.execute(
                """
                INSERT INTO clientes_tokens (cliente_id, token)
                VALUES (%s, %s)
                ON CONFLICT (cliente_id)
                DO UPDATE SET token = EXCLUDED.token, atualizado_em = NOW();
                """,
                (int(cliente_id), token),
            )
            
            # Se é novo cliente, cria a tabela
            mensagem = "Token salvo com sucesso!"
            if eh_novo:
                nome_tabela = self.criar_tabela_cliente(cur, cliente_id)
                mensagem += f"\n\n Tabela '{nome_tabela}' criada com sucesso!"
            
            conn.commit()
            cur.close()
            conn.close()
            
            QMessageBox.information(self, "Sucesso", mensagem)
            self.cliente_id_input.clear()
            self.token_input.clear()
            self.carregar_dados()
        except Exception as e:
            registrar_erro("salvar_token", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar token:\n{e}")

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
                    cliente_id = self.tabela.item(row, 1).text().strip()
                    token = self.tabela.item(row, 2).text().strip()
                    ativo = self.tabela.item(row, 3).checkState() == Qt.Checked
                    
                    if not cliente_id.isdigit() or not token:
                        erros.append(f"Linha {row + 1}: ID ou token inválido")
                        continue
                    
                    cur.execute(
                        """
                        UPDATE clientes_tokens 
                        SET cliente_id = %s, token = %s, ativo = %s, atualizado_em = NOW()
                        WHERE id = %s;
                        """,
                        (int(cliente_id), token, ativo, id_registro),
                    )
                    alteracoes += 1
                    
                except Exception as e:
                    erros.append(f"Linha {row + 1}: {str(e)}")
            
            conn.commit()
            cur.close()
            conn.close()
            
            mensagem = f" {alteracoes} registro(s) atualizado(s) com sucesso!"
            if erros:
                mensagem += "\n\n Erros encontrados:\n" + "\n".join(erros)
            
            QMessageBox.information(self, "Resultado", mensagem)
            self.carregar_dados()
            
        except Exception as e:
            registrar_erro("salvar_edicoes_tabela", e)
            QMessageBox.critical(self, "Erro", f"Erro ao salvar edições:\n{e}")

    def excluir_cliente(self):
        cliente_id = self.cliente_id_input.text().strip()
        if not cliente_id.isdigit():
            QMessageBox.warning(self, "Atenção", "Informe um ID numérico válido.")
            return

        resposta = QMessageBox.question(
            self,
            "Confirmação",
            f"Deseja realmente excluir o cliente {cliente_id}?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if resposta == QMessageBox.Yes:
            try:
                conn = self.conectar()
                cur = conn.cursor()
                cur.execute(
                    "DELETE FROM clientes_tokens WHERE cliente_id = %s;",
                    (int(cliente_id),),
                )
                conn.commit()
                cur.close()
                conn.close()
                QMessageBox.information(
                    self, "Sucesso", "Cliente removido com sucesso."
                )
                self.cliente_id_input.clear()
                self.token_input.clear()
                self.carregar_dados()
            except Exception as e:
                registrar_erro("excluir_cliente", e)
                QMessageBox.critical(self, "Erro", f"Erro ao excluir cliente:\n{e}")

    def preencher_campos(self, row, _):
        cliente_id = self.tabela.item(row, 1).text()
        token = self.tabela.item(row, 2).text()
        self.cliente_id_input.setText(cliente_id)
        self.token_input.setText(token)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    janela = TokenManager()
    janela.show()
    sys.exit(app.exec())