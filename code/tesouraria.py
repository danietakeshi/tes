import requests
import duckdb
import pandas as pd
from dotenv import dotenv_values
import requests_cache
from datetime import timedelta
from flatten_json import flatten

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

    def get_contas(self, endpoint = 'contas'):
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

        return df
    
    def get_categorias(self, endpoint = 'categorias'):
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

        return df
    
def unnest_column(nested_df, column_source):
    categorias_filhas = []

    for list in nested_df[column_source]:
        if list != []:
            for categoria in list:
                categorias_filhas.append(categoria)

    unnested_df = pd.DataFrame.from_dict(categorias_filhas)

    return unnested_df
    

if __name__ == '__main__':
    secret_dict = read_dotenv()
    pb = piaba(secret_dict["TOKEN_GRANATUM"], secret_dict["URL"])
    print(pb.token)
    print(pb.url)

    df = pb.get_categorias()

    df_1 = unnest_column(df, 'categorias_filhas')

    df_2 = unnest_column(df_1, 'categorias_filhas')

    