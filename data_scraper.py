import os
import ast
import tqdm
import time
import json
import joblib
import requests
import itertools
import tqdm.contrib.itertools
import pandas as pd
from typing import Union, List, Dict, Optional
import dask.dataframe as dd

# monitor module
from monitor.folder import folder_exists

# store module


# variables module
from utils.variables import config, request_authorization

n_jobs = 14

# Step1. 定義 API 格式
base_url_dict = config["base_url"]
country_port_dict = config["country"]
goods_category_dict = config["goods_category"]
import_export_transport = config["import_or_export"]

# Step1. 讀取過去已經掃描過的資訊
if "already_queried_api.json" in os.listdir(os.path.dirname(__file__)):
    with open("already_queried_api.json", "r") as f:
        already_queried_api = json.load(f)

else:
    already_queried_api = list()

def identify_containing_or_not(
    base_url_title, 
    one_import_export_transport, 
    one_country, 
    one_country_port, 
    twice_country, 
    twice_country_port, 
    good_category, 
    good_category_items,
    data_list
):
    
    if not(one_country == twice_country):
        if (
            (
                base_url_title == "TEU" and 
                good_category == "None" and (
                    (not(one_import_export_transport == "t") and twice_country_port == "None" and not(f"{base_url_title}-{one_import_export_transport}-{one_country}" in already_queried_api) ) or
                    (one_import_export_transport == "t" and not(twice_country_port == "None") and not(f"{base_url_title}-{one_import_export_transport}-{one_country}-{twice_country}" in already_queried_api) )
                ) and 
                not("{}-{}-{}".format(base_url_title, one_import_export_transport, one_country) in data_list)
            ) or (
                base_url_title == "goods" and (
                    not(good_category == "None") and 
                    not(one_import_export_transport == "t") and 
                    not("{}-{}-{}-{}".format(base_url_title, one_import_export_transport, one_country, good_category) in data_list) and 
                    not(f"{base_url_title}-{one_import_export_transport}-{one_country}-{good_category}" in already_queried_api)
                )
            )                
        ):
            return {
                "base_url_title": base_url_title, 
                "one_import_export_transport": one_import_export_transport,  
                "one_country": one_country, 
                "one_batch_ports": one_country_port,
                "headers": {"Authorization": request_authorization},
                "twice_ports": twice_country_port, 
                "good_category": good_category, 
                "good_category_items": good_category_items, 
                "twice_country": twice_country
            }

def main():
    global already_queried_api

    # Step2. 生成欲放入 post_api 函數的列表
    data_list = os.listdir("./iMarine_dataset/")
    # delayed_func = [
    #     joblib.delayed(identify_containing_or_not)(
    #         base_url_title, 
    #         one_import_export_transport, 
    #         one_country, 
    #         one_country_port, 
    #         twice_country, 
    #         twice_country_port, 
    #         good_category, 
    #         good_category_items,
    #         data_list            
    #     )
    #         for (
    #             base_url_title, 
    #             one_import_export_transport, 
    #             (one_country, one_country_port), 
    #             (twice_country, twice_country_port), 
    #             (good_category, good_category_items)
    #         ) in tqdm.contrib.itertools.product(
    #             base_url_dict.keys(),
    #             import_export_transport,
    #             country_port_dict.items(),
    #             country_port_dict.items(),
    #             goods_category_dict.items()
    #         )         
    # ]
    api_parameters_list = [
        {
            "base_url_title": base_url_title, 
            "one_import_export_transport": one_import_export_transport,  
            "one_country": one_country, 
            "one_batch_ports": one_country_port,
            "headers": {"Authorization": request_authorization},
            "twice_ports": twice_country_port, 
            "good_category": good_category, 
            "good_category_items": good_category_items, 
            "twice_country": twice_country
        }
        for (
            base_url_title, 
            one_import_export_transport, 
            (one_country, one_country_port), 
            (twice_country, twice_country_port), 
            (good_category, good_category_items)
        ) in tqdm.contrib.itertools.product(
            list(base_url_dict.keys()),
            import_export_transport,
            country_port_dict.items(),
            country_port_dict.items(),
            goods_category_dict.items()
        ) 
        if not(one_country == twice_country)
        if (
            (
                base_url_title == "TEU" and 
                good_category == "None" and (
                    (
                        not(one_import_export_transport == "t") and 
                        twice_country == "None" and 
                        not(f"{base_url_title}-{one_import_export_transport}-{one_country}" in already_queried_api)
                    ) or
                    (
                        one_import_export_transport == "t" and 
                        not(twice_country == "None") and 
                        not(f"{base_url_title}-{one_import_export_transport}-{one_country}-{twice_country}" in already_queried_api)
                    )
                )
                and not("{}-{}-{}".format(base_url_title, one_import_export_transport, one_country) in data_list)
            ) or (
                base_url_title == "goods" and (
                    not(good_category == "None") and 
                    not(one_import_export_transport == "t") and 
                    not("{}-{}-{}-{}".format(base_url_title, one_import_export_transport, one_country, good_category) in data_list) and 
                    not(f"{base_url_title}-{one_import_export_transport}-{one_country}-{good_category}" in already_queried_api) and 
                    twice_country == "None"
                )
            )                
        )
    ]
    # api_parameters_list = joblib.Parallel(n_jobs = -1)(tqdm.tqdm(delayed_func))
    # num_of_groups = api_parameters_list.__len__() // n_jobs
    # batch_api_parameters_list = [
    #     api_parameters_list[i*n_jobs: (i+1)*n_jobs] if not(num_of_groups == i) else api_parameters_list[i*n_jobs:]
    #     for i in range(num_of_groups + 1)
    # ]

    # Step2. 透過 requests 爬取資料
    for one_batch in tqdm.tqdm(api_parameters_list):
        print(one_batch)
        post_api(**one_batch)
        with open("./already_queried_api.json", "w") as f:
            json.dump(already_queried_api, f)

        try:
            with open("already_queried_api.json", "r") as f:
                already_queried_api = json.load(f)
        except:
            continue
    # delayed_func = tqdm.tqdm(
    #     [
    #         joblib.delayed(
    #             function = post_api
    #         )(
    #             base_url_title = base_url_title, 
    #             one_import_export_transport = one_import_export_transport,  
    #             one_country = one_country, 
    #             one_batch_ports = one_country_port,
    #             headers = {"Authorization": request_authorization},
    #             twice_ports = twice_country_port, 
    #             good_category = good_category, 
    #             good_category_items = good_category_items, 
    #             twice_country = twice_country
    #         )
    #         for (
    #             base_url_title, 
    #             one_import_export_transport, 
    #             (one_country, one_country_port), 
    #             (twice_country, twice_country_port), 
    #             (good_category, good_category_items)
    #         ) in tqdm.contrib.itertools.product(
    #             base_url_dict.keys(),
    #             import_export_transport,
    #             country_port_dict.items(),
    #             country_port_dict.items(),
    #             goods_category_dict.items()
    #         ) 
    #         if (
    #             (
    #                 base_url_title == "TEU" and 
    #                 good_category == "None" and (
    #                     (not(one_import_export_transport == "t") and twice_country_port == "None") or (one_import_export_transport == "t" and not(twice_country_port == "None"))
    #                 ) and 
    #                 not("{}-{}-{}".format(base_url_title, one_import_export_transport, one_country) in data_list) and 
    #                 not(f"{base_url_title}-{one_import_export_transport}-{one_country}" in already_queried_api)
    #             ) or (
    #                 base_url_title == "goods" and (
    #                     not(good_category == "None") and 
    #                     not(one_import_export_transport == "t") and 
    #                     twice_country_port == "None" and 
    #                     not("{}-{}-{}-{}".format(base_url_title, one_import_export_transport, one_country, good_category) in data_list) and 
    #                     not(f"{base_url_title}-{one_import_export_transport}-{one_country}-{good_category}" in already_queried_api)
    #                 )
    #             )                
    #         ) and 
    #         not(one_country == twice_country)
    #     ]
    # )
    # joblib.Parallel(n_jobs = -1)(delayed_func)
    return 

def post_api(
    base_url_title: str, 
    one_import_export_transport: str, 
    one_batch_ports: List[str],
    one_country: str, 
    headers: Optional[Dict[str, str]],
    **kwargs
) -> Union[list, pd.DataFrame]:
    
    base_url = base_url_dict[base_url_title]
    
    time.sleep(1)
    if "goods" in base_url:
        goods_category_items = "-".join(kwargs["good_category_items"])
        api_link = "/".join([base_url, one_import_export_transport, "2", goods_category_items])

    elif "container" in base_url:
        api_link = "/".join([base_url, one_import_export_transport])

    try:
        if one_import_export_transport == "i":
            if base_url_title == "goods":
                file_name = "iMarine_data-{}-{}-{}-{}-201601-202404.parquet.gz".format(
                    base_url_title,
                    one_import_export_transport,
                    one_country,
                    kwargs["good_category"]
                )

            else:
                file_name = "iMarine_data-{}-{}-{}-201601-202404.parquet.gz".format(
                    base_url_title,
                    one_import_export_transport,
                    one_country
                )

            response = requests.post(
                api_link, 
                json = {
                    "Start":"201601", 
                    "End":"202404", 
                    "IPortLevel": 1,  
                    "IPort": one_batch_ports,
                    "TWPortLevel": 1,
                    "TWPort": config["taiwan_port"]
                },
                headers = headers       
            )

        elif one_import_export_transport == "e":
            if base_url_title == "goods":
                file_name = "iMarine_data-{}-{}-{}-{}-201601-202404.parquet.gz".format(
                    base_url_title,
                    one_import_export_transport,
                    one_country,
                    kwargs["good_category"]
                )

            else:
                file_name = "iMarine_data-{}-{}-{}-201601-202404.parquet.gz".format(
                    base_url_title,
                    one_import_export_transport,
                    one_country
                )

            response = requests.post(
                api_link, 
                json = {
                    "Start": "201601", 
                    "End": "202404", 
                    "TWPortLevel": 1,  
                    "TWPort": config["taiwan_port"],
                    "EPortLevel": 1,
                    "EPort": one_batch_ports
                },
                headers = headers       
            )

        elif one_import_export_transport == "t":
            file_name = "iMarine_data-{}-{}-{}-{}-201601-202404.parquet.gz".format(
                base_url_title,
                one_import_export_transport,
                one_country,
                kwargs["twice_country"]
            )            
            response = requests.post(
                api_link, 
                json = {
                    "Start": "201601", 
                    "End": "202404", 
                    "IPortLevel": 1,  
                    "IPort": one_batch_ports,
                    "EPortLevel": 1,
                    "EPort": kwargs["twice_ports"],
                    "TWPortLevel": 1,
                    "TWPort": config["taiwan_port"]
                },
                headers = headers       
            )

        df_records = ast.literal_eval(response.text)
    except Exception as e:
        print(one_import_export_transport, one_batch_ports, kwargs)
        print(e)
        return None
    time.sleep(10)

    # Step3. 儲存資料
    if df_records.__len__() == 0:
        already_queried_api.append(
            f"{base_url_title}-{one_import_export_transport}-{one_country}-{kwargs['good_category']}" if base_url_title == "goods" else (
                f"{base_url_title}-{one_import_export_transport}-{one_country}" if one_import_export_transport in ["i", "e"] else f"{base_url_title}-{one_import_export_transport}-{one_country}-{kwargs['twice_country']}"
            )
        )
        return None
    else:
        df = pd.DataFrame.from_records(df_records)
        if isinstance(df, pd.Series):
            df = df.frame()
    
    if one_import_export_transport == "t":
        if "goods" in base_url:
            
            df.to_parquet(
                os.path.join("iMarine_dataset", file_name),
                compression = "gzip"
            )
        else:
            df.to_parquet(
                os.path.join("iMarine_dataset", file_name),
                compression = "gzip"
            )            

    else:
        if "goods" in base_url:
            df.to_parquet(
                os.path.join("iMarine_dataset", file_name),
                compression = "gzip"
            )
        else:
            df.to_parquet(
                os.path.join("iMarine_dataset", file_name),
                compression = "gzip"
            )     
    already_queried_api.append(
        f"{base_url_title}-{one_import_export_transport}-{one_country}-{kwargs['good_category']}" if base_url_title == "goods" else (
            f"{base_url_title}-{one_import_export_transport}-{one_country}" if one_import_export_transport in ["i", "e"] else f"{base_url_title}-{one_import_export_transport}-{one_country}-{kwargs['twice_country']}"
        )
    )    

if __name__ == "__main__":
    main()