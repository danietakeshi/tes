import requests
import duckdb
import pandas as pd
from dotenv import dotenv_values
import requests_cache
from datetime import timedelta
from flatten_json import flatten
import calendar
import math


requests_cache.install_cache('granatum_cache', backend='sqlite', expire_after=timedelta(hours=1), allowable_codes=[200])

def read_dotenv():
    secret_dict={}
    config = dotenv_values()
    for i in config:
        secret_dict[i] = config.get(i)
    return secret_dict

class piaba:
    def __init__(self, token, url):
        self.token = token
        self.url = url

    def get_api_info(self, endpoint = 'contas', save_parquet = True):
        result = []

        url = f"{self.url}/{endpoint}"
        params = {
            'access_token': self.token,
        }
        response = requests.get(url, params=params)
        data = response.json()
        result.extend(data)

        print(f"Used Cache : {response.from_cache}")

        df = pd.DataFrame.from_dict(result)
        
        if save_parquet:
            df.to_parquet(f'../parquet/{endpoint}.parquet')

        return df
    
    def get_lancamentos_by_month(self, endpoint = 'lancamentos', conta = '74988', data_inicio = '2024-02-01', data_fim = '2024-02-29', save_parquet = True):
        result = []

        url = f"{self.url}/{endpoint}"
        params = {
            'access_token': self.token,
            'conta_id': conta,
            'data_inicio': data_inicio,
            'data_fim': data_fim,
            'limit': 500,
            'tipo_view': 'count',
        }
        response = requests.get(url, params=params)
        data = response.json()
        
        count_from_api = data['0']
        
        number_of_pages = math.ceil(count_from_api / 500)
        
        page_number = 1
        
        print(f'Count from API: {count_from_api} - pages: {number_of_pages}')
        
        params.pop("tipo_view")
        
        for page in range(number_of_pages):
            print(f'Getting page number {page + 1}')
            params['start'] = 500 * (page)
            response = requests.get(url, params=params)
            data = response.json()
            result.extend(data)
        
        df = pd.DataFrame.from_dict(result)
        
        print(f'Count from DataFrame: {len(df.index)}')

        print(f"Used Cache : {response.from_cache}")
        
        if save_parquet and len(df.index):
            df.to_parquet(f'../parquet/{data_inicio.replace('-', '')}_{conta}_{endpoint}.parquet')

        return df, count_from_api == len(df.index)
    
    def get_categorias(self, endpoint = 'categorias', save_parquet = True):
        result = []

        url = f"{self.url}/{endpoint}"
        params = {
            'access_token': self.token,
        }
        response = requests.get(url, params=params)
        data = response.json()
        result.extend(data)

        print(f"Used Cache : {response.from_cache}")

        df = pd.DataFrame.from_dict(result)
        
        df_1 = unnest_column(df, 'categorias_filhas')
        
        df_2 = unnest_column(df_1, 'categorias_filhas')
        
        df_categorias = duckdb.query("""
        SELECT 
            df.id id_categoria,
            df.descricao descricao_categoria,
            df.tipo_categoria_id,
            df_1.id id_subcategoria_1,
            df_1.descricao descricao_subcategoria_1,
            df_1.tipo_categoria_id tipo_subcategoria_1_id,
            df_2.id id_subcategoria_2,
            df_2.descricao descricao_subcategoria_2,
            df_2.tipo_categoria_id tipo_subcategoria_2_id
        FROM df
        LEFT JOIN df_1 ON df.id = df_1.parent_id
        LEFT JOIN df_2 ON df_1.id = df_2.parent_id; 
        """).df()
        
        if save_parquet:
            df_categorias.to_parquet(f'../parquet/{endpoint}.parquet')

        return df_categorias
    
    def get_centros_custo_lucro(self, endpoint = 'centros_custo_lucro', save_parquet = True):
        result = []

        url = f"{self.url}/{endpoint}"
        params = {
            'access_token': self.token,
        }
        response = requests.get(url, params=params)
        data = response.json()
        result.extend(data)

        print(f"Used Cache : {response.from_cache}")

        df = pd.DataFrame.from_dict(result)
        
        df_1 = unnest_column(df, 'centros_custo_lucro_filhos')
        
        df_centros_custo_lucro = duckdb.query("""
        SELECT 
            df.id id_centro_custo_pai,
            df.descricao descricao_centro_custo_pai,
            df_1.id id_centro_custo_filho,
            df_1.descricao descricao_centro_custo_filho
        FROM df
        LEFT JOIN df_1 ON df.id = df_1.parent_id; 
        """).df()
        
        if save_parquet:
            df_centros_custo_lucro.to_parquet(f'../parquet/{endpoint}.parquet')

        return df_centros_custo_lucro
    
def unnest_column(nested_df, column_source):
    categorias_filhas = []

    for list in nested_df[column_source]:
        if list != []:
            for categoria in list:
                categorias_filhas.append(categoria)

    unnested_df = pd.DataFrame.from_dict(categorias_filhas)

    return unnested_df

def get_start_end_dates(year, month):
    # Get the last day of the month
    last_day = calendar.monthrange(year, month)[1]
    
    # Start date is always the first of the month
    start_date = f"{year}-{month:02d}-01"
    
    # End date is the last day of the month
    end_date = f"{year}-{month:02d}-{last_day}"
    
    return start_date, end_date
    

if __name__ == '__main__':
    secret_dict = read_dotenv()
    pb = piaba(secret_dict["TOKEN_GRANATUM"], secret_dict["URL"])
    print(pb.token)
    print(pb.url)
    
    # pb.get_api_info(endpoint='contas')
    # pb.get_api_info(endpoint='clientes')
    # pb.get_api_info(endpoint='fornecedores')
    # pb.get_api_info(endpoint='formas_pagamento')
    # df_categorias = pb.get_categorias()
    # df = pb.get_centros_custo_lucro()
    
    # print(df)

    year = 2024
    month = 2
    start_date, end_date = get_start_end_dates(year, month)
    print("Start Date:", start_date)
    print("End Date:", end_date)
    
    df_contas = pd.read_parquet('../parquet/contas.parquet')
    
    for conta in df_contas.id.values:
        print(conta)
        df, check_rows = pb.get_lancamentos_by_month(endpoint='lancamentos', conta=conta, data_inicio=start_date, data_fim=end_date, save_parquet=True)
        print(f'Number of rows in df is equal to the API: {check_rows}')
        
    df = duckdb.query("""
    SELECT 
        l.id,
        l.grupo_id,
        l.lancamento_transferencia_id,
        l.categoria_id,
        c.descricao_categoria,
        c.descricao_subcategoria_1,
        c.descricao_subcategoria_2,
        l.centro_custo_lucro_id,
        ccl.descricao_centro_custo_pai,
        ccl.descricao_centro_custo_filho,
        l.tipo_custo_nivel_producao_id,
        l.tipo_custo_apropriacao_produto_id,
        l.conta_id,
        ct.descricao descricao_conta,
        l.forma_pagamento_id,
        fp.descricao descricao_forma_pagamento,
        l.pessoa_id,
        COALESCE(cl.nome, f.nome) nome,
        l.tipo_lancamento_id,
        l.descricao,
        l.tipo_documento_id,
        l.documento,
        l.status,
        l.infinito,
        l.data_vencimento,
        l.data_pagamento,
        l.data_competencia,
        l.observacao,
        l.pagamento_automatico,
        l.numero_repeticao,
        l.total_repeticoes,
        l.periodicidade,
        l.pedido_id,
        l.lancamento_composto_id,
        l.wgi_usuario_id,
        l.itens_adicionais,
        l.tags,
        l.anexos,
        l.modified,
        l.valor,
        l.nfe_id,
        l.filename
    FROM read_parquet('*_lancamentos.parquet', filename = true) l
    LEFT JOIN 'categorias.parquet' c ON l.categoria_id = COALESCE(c.id_subcategoria_2, c.id_subcategoria_1, c.id_categoria)
    LEFT JOIN 'centros_custo_lucro.parquet' ccl ON l.centro_custo_lucro_id = COALESCE(ccl.id_centro_custo_filho, ccl.id_centro_custo_pai)
    LEFT JOIN 'contas.parquet' ct ON l.conta_id = ct.id
    LEFT JOIN 'formas_pagamento.parquet' fp ON l.forma_pagamento_id = fp.id
    LEFT JOIN 'clientes.parquet' cl ON l.pessoa_id = cl.id
    LEFT JOIN 'fornecedores.parquet' f ON l.pessoa_id = f.id
    """).df()
    

    