"""
Script de Migração: Adiciona coluna tabela_raw e cria tabelas para rotas existentes
Execute este script UMA VEZ para atualizar o banco de dados
"""

import os
from dotenv import load_dotenv
import psycopg2
import re

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
}


def normalizar_nome_tabela(nome):
    """
    Normaliza o nome da rota para criar um nome de tabela válido
    Remove caracteres especiais e espaços, mantém apenas letras, números e underscore
    """
    # Remove acentos e caracteres especiais
    nome = nome.lower()
    nome = re.sub(r'[àáâãäå]', 'a', nome)
    nome = re.sub(r'[èéêë]', 'e', nome)
    nome = re.sub(r'[ìíîï]', 'i', nome)
    nome = re.sub(r'[òóôõö]', 'o', nome)
    nome = re.sub(r'[ùúûü]', 'u', nome)
    nome = re.sub(r'[ç]', 'c', nome)
    
    # Remove espaços e caracteres especiais, mantém apenas letras, números e underscore
    nome = re.sub(r'[^a-z0-9_]', '_', nome)
    
    # Remove underscores duplicados
    nome = re.sub(r'_+', '_', nome)
    
    # Remove underscores no início e fim
    nome = nome.strip('_')
    
    return nome


def criar_tabela_raw_rota(cur, cliente_id, nome_rota):
    """
    Cria uma tabela raw específica para a rota do cliente
    Formato: raw_{cliente_id}_{nome_rota_normalizado}
    """
    nome_normalizado = normalizar_nome_tabela(nome_rota)
    nome_tabela = f"raw_{cliente_id}_{nome_normalizado}"
    
    # Verifica se a tabela já existe
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
    """, (nome_tabela,))
    
    if cur.fetchone()[0]:
        print(f"Tabela '{nome_tabela}' já existe!")
        return nome_tabela
    
    # Cria a tabela com estrutura otimizada
    cur.execute(f"""
        CREATE TABLE {nome_tabela} (
            id BIGSERIAL PRIMARY KEY,
            data_coleta TIMESTAMP DEFAULT NOW() NOT NULL,
            payload JSONB NOT NULL,
            hash_conteudo TEXT,
            criado_em TIMESTAMP DEFAULT NOW(),
            atualizado_em TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_{nome_tabela}_hash UNIQUE (hash_conteudo)
        );
        
        -- Índice para busca por data (mais recente primeiro)
        CREATE INDEX idx_{nome_tabela}_data 
            ON {nome_tabela} (data_coleta DESC);
        
        -- Índice GIN para buscas JSONB otimizadas
        CREATE INDEX idx_{nome_tabela}_payload_gin 
            ON {nome_tabela} USING GIN (payload jsonb_path_ops);
        
        -- Índice para hash (performance em verificação de duplicatas)
        CREATE INDEX idx_{nome_tabela}_hash 
            ON {nome_tabela} (hash_conteudo) 
            WHERE hash_conteudo IS NOT NULL;
        
        -- Trigger para atualizar timestamp
        CREATE OR REPLACE FUNCTION atualiza_timestamp_{nome_tabela}() 
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.atualizado_em = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trg_atualiza_{nome_tabela}
            BEFORE UPDATE ON {nome_tabela}
            FOR EACH ROW 
            EXECUTE FUNCTION atualiza_timestamp_{nome_tabela}();
        
        -- Comentário na tabela
        COMMENT ON TABLE {nome_tabela} IS 
            'Dados raw da rota {nome_rota} do cliente {cliente_id} - Criado por migração';
    """)
    
    print(f" Tabela '{nome_tabela}' criada com sucesso!")
    return nome_tabela


def executar_migracao():
    """Executa a migração completa"""
    print("\n" + "="*80)
    print("MIGRAÇÃO: Adicionar coluna tabela_raw e criar tabelas para rotas existentes")
    print("="*80 + "\n")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding("UTF8")
        cur = conn.cursor()
        
        # Passo 1: Verificar se a coluna já existe
        print("📋 Passo 1: Verificando estrutura da tabela clientes_api_rotas...")
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'clientes_api_rotas' 
            AND column_name = 'tabela_raw';
        """)
        
        coluna_existe = cur.fetchone() is not None
        
        if coluna_existe:
            print("Coluna 'tabela_raw' já existe!")
        else:
            print(" Adicionando coluna 'tabela_raw'...")
            cur.execute("""
                ALTER TABLE clientes_api_rotas 
                ADD COLUMN tabela_raw TEXT;
                
                CREATE INDEX IF NOT EXISTS idx_api_rotas_tabela 
                    ON clientes_api_rotas(tabela_raw);
            """)
            conn.commit()
            print(" Coluna 'tabela_raw' adicionada com sucesso!")
        
        # Passo 2: Buscar todas as rotas existentes
        print("\n Passo 2: Processando rotas existentes...")
        cur.execute("""
            SELECT id, cliente_id, nome_rota, tabela_raw
            FROM clientes_api_rotas
            ORDER BY cliente_id, nome_rota;
        """)
        
        rotas = cur.fetchall()
        total_rotas = len(rotas)
        
        if total_rotas == 0:
            print("  Nenhuma rota encontrada para processar.")
        else:
            print(f"Encontradas {total_rotas} rotas para processar.\n")
            
            rotas_atualizadas = 0
            rotas_ignoradas = 0
            
            for rota_id, cliente_id, nome_rota, tabela_raw_atual in rotas:
                print(f" Processando: Cliente {cliente_id} - {nome_rota}")
                
                # Se já tem tabela configurada, ignora
                if tabela_raw_atual:
                    print(f"Já possui tabela: {tabela_raw_atual}")
                    rotas_ignoradas += 1
                    continue
                
                # Cria a tabela e atualiza o registro
                nome_tabela = criar_tabela_raw_rota(cur, cliente_id, nome_rota)
                
                cur.execute("""
                    UPDATE clientes_api_rotas 
                    SET tabela_raw = %s, atualizado_em = NOW()
                    WHERE id = %s;
                """, (nome_tabela, rota_id))
                
                rotas_atualizadas += 1
                print(f"Rota atualizada: {nome_tabela}\n")
            
            conn.commit()
            
            print("\n" + "="*80)
            print(" RESUMO DA MIGRAÇÃO")
            print("="*80)
            print(f"   • Total de rotas encontradas: {total_rotas}")
            print(f"   • Rotas processadas e atualizadas: {rotas_atualizadas}")
            print(f"   • Rotas já configuradas (ignoradas): {rotas_ignoradas}")
            print("="*80 + "\n")
        
        # Passo 3: Verificar resultado final
        print("Passo 3: Verificando resultado final...")
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(tabela_raw) as com_tabela,
                COUNT(*) - COUNT(tabela_raw) as sem_tabela
            FROM clientes_api_rotas;
        """)
        
        total, com_tabela, sem_tabela = cur.fetchone()
        
        print(f"   • Total de rotas: {total}")
        print(f"   • Rotas com tabela configurada: {com_tabela}")
        print(f"   • Rotas sem tabela configurada: {sem_tabela}")
        
        if sem_tabela > 0:
            print("\n   ATENÇÃO: Ainda existem rotas sem tabela configurada!")
            print("      Execute este script novamente ou recadastre as rotas manualmente.")
        else:
            print("\n SUCESSO! Todas as rotas estão configuradas corretamente!")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*80)
        print("MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\nERRO durante a migração: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        
        print("\nA migração falhou. Reverta as alterações se necessário.")
        return False
    
    return True


if __name__ == "__main__":
    print("\n🚀 Iniciando script de migração...")
    print("Este script irá:")
    print("   1. Adicionar a coluna 'tabela_raw' na tabela clientes_api_rotas")
    print("   2. Criar tabelas raw para todas as rotas existentes")
    print("   3. Atualizar os registros com os nomes das tabelas criadas")
    
    resposta = input("\n Deseja continuar? (s/N): ").strip().lower()
    
    if resposta == 's':
        sucesso = executar_migracao()
        
        if sucesso:
            print("\n Você pode agora executar o sistema normalmente!")
            print("   Execute: python src/main.py")
        else:
            print("\n A migração falhou. Verifique os erros acima.")
    else:
        print("\n Migração cancelada pelo usuário.")