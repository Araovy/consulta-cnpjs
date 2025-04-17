import requests
from datetime import datetime, timedelta
from supabase import create_client
import time

# Configurações
API_KEY = 'b3cecbb4-cc1a-4c4c-9339-798ccd4d22d6-da45bd86-c0f9-4bd7-8039-95088fcccc19'
HEADERS = {'Authorization': API_KEY}
SUPABASE_URL = 'https://ntubqodbzvkhsapedved.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im50dWJxb2RienZraHNhcGVkdmVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NDg5OTE2OSwiZXhwIjoyMDYwNDc1MTY5fQ.cFYXM1N04icvH6-5D01GOmZzGQxrPrwoVVoejoptXb4'
TABELA_CONTRATOS = 'contratos'
TABELA_CONSULTA = 'consulta_simples'

# Conexão Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def consultar_simples_nacional(doc):
    """Consulta aprimorada que verifica todas as possibilidades"""
    # Se for CPF (11 dígitos), retorna False automaticamente
    if len(doc) == 11:
        print(f"CPF {doc} detectado - Definindo como não optante")
        return False
        
    url = f'https://api.cnpja.com/office/{doc}?simples=true'
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Resposta completa para {doc}:", data)  # Debug
            
            # Verifica todas as possíveis estruturas
            caminhos = [
                ['company', 'simples', 'optant'],
                ['company', 'simplesOptant'],
                ['simples', 'optant'],
                ['simplesOptant']
            ]
            
            for caminho in caminhos:
                try:
                    valor = data
                    for chave in caminho:
                        valor = valor[chave]
                    print(f"Encontrado em {caminho}: {valor}")
                    return valor
                except (KeyError, TypeError):
                    continue
            
            print("Simples Nacional não encontrado na resposta")
            return None
            
        print(f"Erro na API: {response.status_code} - {response.text}")
        return None
        
    except Exception as e:
        print(f"Erro na consulta: {str(e)}")
        return None

def processar_cnpjs():
    try:
        print("\n=== INÍCIO DO PROCESSAMENTO ===")
        
        # 1. Buscar documentos da tabela contratos
        print("\n[1/4] Buscando documentos...")
        contratos = supabase.table(TABELA_CONTRATOS).select('cnpj').execute()
        # Corrigindo a sintaxe da compreensão de conjunto
        docs_unicos = list({str(c['cnpj']).strip().zfill(14) for c in contratos.data if c['cnpj']})
        print(f"Encontrados {len(docs_unicos)} documentos únicos (CNPJs/CPFs)")

        # 2. Buscar documentos já validados
        print("\n[2/4] Verificando validações existentes...")
        validados = supabase.table(TABELA_CONSULTA).select('cnpj,data_validacao').execute()
        docs_validados = {v['cnpj']: v['data_validacao'] for v in validados.data}
        print(f"Documentos já validados: {len(docs_validados)}")

        # 3. Identificar documentos para processar
        docs_processar = []
        for doc in docs_unicos:
            if doc not in docs_validados:
                docs_processar.append((doc, True))  # (documento, é_novo)
            else:
                # Verifica se a validação tem mais de 90 dias
                try:
                    data_validade = datetime.strptime(docs_validados[doc], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        data_validade = datetime.strptime(docs_validados[doc], '%Y-%m-%d')
                    except ValueError:
                        print(f"⚠️ Formato de data inválido para {doc}: {docs_validados[doc]}")
                        continue
                
                if (datetime.now() - data_validade).days > 90:
                    docs_processar.append((doc, False))

        print(f"\n[3/4] Documentos para processar: {len(docs_processar)}")

        # 4. Processar cada documento
        print("\n[4/4] Processando documentos...")
        for doc, is_novo in docs_processar:
            print(f"\n--- Processando {doc} ({'CPF' if len(doc) == 11 else 'CNPJ'}) ({'novo' if is_novo else 'atualizar'}) ---")
            
            simples_nacional = consultar_simples_nacional(doc)
            
            # Se for None (erro na consulta), mantém o valor existente para atualizações
            if not is_novo and simples_nacional is None:
                print("⚠️ Mantendo valor existente devido a erro na consulta")
                continue
                
            dados = {
                'cnpj': doc,
                'simples_nacional': simples_nacional if simples_nacional is not None else False,
                'data_validacao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            if is_novo:
                supabase.table(TABELA_CONSULTA).insert(dados).execute()
                print(f"✅ Inserido - Simples: {dados['simples_nacional']}")
            else:
                supabase.table(TABELA_CONSULTA).update(dados).eq('cnpj', doc).execute()
                print(f"🔄 Atualizado - Simples: {dados['simples_nacional']}")

        print("\n=== PROCESSAMENTO CONCLUÍDO COM SUCESSO ===")

    except Exception as e:
        print(f"\n⛔ ERRO: {str(e)}")

if __name__ == "__main__":
    processar_cnpjs()