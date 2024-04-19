import ast
import tqdm
import joblib
import requests
import itertools
import pandas as pd
from typing import Union
import dask.dataframe as dd

# monitor module
from monitor.folder import folder_exists

# store module


# variables module
from utils.variables import config, request_authorization

# Step1. 定義 API 格式
base_url_dict = config["base_url"]
country_port_dict = config["country"]
goods_category_dict = config["goods_category"]
import_export_transport = config["import_or_export"]
goods_category_list = [None, *list(goods_category_dict.keys())]

def main():

    # Step2. 透過 requests 爬取資料
    delayed_func = tqdm.tqdm(
        [
            joblib.delayed(
                function = post_api
            )(
                base_url = one_base_url,
                one_import_export_transport = one_import_export_transport,  
                one_batch_ports = one_country_port,
                headers = {"Authorization": request_authorization},
                twice_ports = twice_country_port, 
                good_category = good_category, 
            )
            for (base_url_title, one_base_url), one_country_port, one_import_export_transport, twice_country_port, good_category in itertools.product(
                base_url_dict.items(),
                country_port_dict.values(),
                import_export_transport,
                [None, *list(country_port_dict.values())],
                goods_category_list
            ) 
            if (
                (base_url_title == "TEU" and one_import_export_transport in ["i", "e", "t"]) or (base_url_title == "goods" and one_import_export_transport in ["i", "e"])
            ) and (
                (base_url_title == "TEU" and good_category is None) or (base_url_title == "goods" and not(good_category is None))
            ) and (
                (one_import_export_transport == "t" and not(twice_country_port is None)) or (not(one_import_export_transport == "t") and twice_country_port is None)
            )
        ]        
    )
    df = joblib.Parallel(n_jobs = 8)(delayed_func)
    df = pd.concat(df, axis = 0).reset_index(drop = True)
    

    # Step3. 儲存資料
    df.to_parquet("iMarine_dataset.parquet.gz", compression = "gzip")
    return 

def post_api(
    base_url: str, 
    one_import_export_transport: str, 
    one_batch_ports: list,
    headers: dict = None,
    **kwargs
) -> Union[list, pd.DataFrame]:
    
    if "goods" in base_url:
        if not(one_batch_ports, kwargs["twice_ports"]):
            return None
        goods_category_items = "-".join(goods_category_dict[kwargs["good_category"]])
        api_link = "/".join([base_url, one_import_export_transport, "2", goods_category_items])

    elif "container" in base_url:
        if not(one_batch_ports, kwargs["twice_ports"]):
            return None        
        api_link = "/".join([base_url, one_import_export_transport])

    if one_import_export_transport == "i":
        response = requests.post(
            api_link, 
            json = {
                "Start":"201601", 
                "End":"202404", 
                "IPortLevel": 1,  
                "IPort": one_batch_ports,
                "EPortLevel": 1,
                "EPort": config["taiwan_port"]
            },
            headers = headers       
        )

    elif one_import_export_transport == "e":
        response = requests.post(
            api_link, 
            json = {
                "Start": "201601", 
                "End": "202404", 
                "IPortLevel": 1,  
                "IPort": config["taiwan_port"],
                "EPortLevel": 1,
                "EPort": one_batch_ports
            },
            headers = headers       
        )

    elif one_import_export_transport == "t":
        response = requests.post(
            api_link, 
            json = {
                "Start": "201601", 
                "End": "202404", 
                "IPortLevel": 1,  
                "IPort": one_batch_ports,
                "EPortLevel": 1,
                "EPort": one_batch_ports,
                "TWPortLevel": 1,
                "TWPort": kwargs["twice_ports"]
            },
            headers = headers       
        )

    df_records = ast.literal_eval(response.text)
    if df_records.__len__() == 0:
        return None
    else:
        df = pd.DataFrame.from_records(df_records)
        if isinstance(df, pd.Series):
            return df.to_frame()

        else:
            return df

if __name__ == "__main__":
    main()