import os
import argparse
import itertools
import pandas as pd
from typing import Literal
import dask.dataframe as dd
import tqdm.contrib.itertools
from dask.dataframe.io.io import from_map
# from dask.distributed import Client, LocalCluster
from utils.variables import config, request_authorization

country_port_dict = config["country"]
goods_category_dict = config["goods_category"]


# # 創建一個本地集群並設置工作節點數量
# cluster = LocalCluster(n_workers=20)  # 設置8個工作節點，可以根據需要調整
# client = Client(cluster)

"""
本排程主要是處理與資料前處理有關功能，包含港口資料合併、時間推移等。
"""

def main():

    # Step1. 把水果等交易行情資料合併成一個檔案
    # vegetable_df = merge_all_vegetable_or_fruit_data(
    #     folder_path = "./農產品市場交易行情/水果",
    #     keyword = "水果產品日"
    # )

    # Step2. 把蔬菜等交易行情資料合併成一個檔案，並進行 Time shifting 以利模型訓練
    goods_df = merge_all_goods_data(
        folder_path = "./iMarine_dataset",
        keyword = "-goods-",

    )

    # Step4. 把市場交易行情資料與貨物量資料合併


    return 

def merge_all_goods_data(
    folder_path: str,
    keyword: str,
    predict_level: Literal["port", "country"] = "port"
) -> pd.DataFrame:
    
    """
    合併所有與貨物有關的資料，並把重複的資料排除掉。
    """

    def read_all_related_files(
        import_or_export: str = Literal["-e-", "-i-"]
    ):

        # Step1. 確認所有與貨物出口相關的檔案
        fit_file_name = [
            os.path.join(folder_path, i) for i in os.listdir(folder_path)
            if keyword in i and import_or_export in i
        ]

        # Step2. 讀取與出口相關檔案
        dd_table = dd.read_parquet(fit_file_name, npartitions = 20)

        # Step3. 排除重複資料，並重新設定 Index
        dd_table = dd_table.drop_duplicates().reset_index(drop = True)
        return dd_table

    dd_export_table = read_all_related_files(
        import_or_export = "-e-"
    )
    dd_import_table = read_all_related_files(
        import_or_export = "-i-"
    )

    # Step4. 補上出口資訊、進口港與出口港定義
    dd_export_table = dd_export_table.assign(
        import_or_export = "export",
        source_port = dd_export_table["TW"],
        destination_port = dd_export_table["EP"], 
        source_region = "TW",
        destination_region = dd_export_table["EC"],
        year = dd_export_table["YM"].apply(
            lambda x: x.split("/")[0],
            meta = dd_export_table["YM"]
        ).astype("int"),
        month = dd_export_table["YM"].apply(
            lambda x: x.split("/")[1][1] if x.split("/")[1][0] == "0" else x.split("/")[1],
            meta = dd_export_table["YM"]
        ).astype("int")
    )
    dd_import_table = dd_import_table.assign(
        import_or_export = "import",
        source_port = dd_import_table["IP"],
        destination_port = dd_import_table["TW"], 
        source_region = dd_import_table["IC"],
        destination_region = "TW",
        year = dd_export_table["YM"].apply(
            lambda x: x.split("/")[0],
            meta = dd_export_table["YM"]
        ).astype("int"),
        month = dd_export_table["YM"].apply(
            lambda x: x.split("/")[1][1] if x.split("/")[1][0] == "0" else x.split("/")[1],
            meta = dd_export_table["YM"]
        ).astype("int")
    )    

    # 將各國港口合併總計
    dd_export_table = dd_export_table.groupby(
        by = [
            "year",
            "month",
            "import_or_export",
            "source_region",
            "source_port",
            "destination_region",
            "C1",
            "C2"
        ]
    )[["WT"]].sum()
    dd_export_table = dd_export_table.reset_index()

    dd_import_table = dd_import_table.groupby(
        by = [
            "year",
            "month",
            "import_or_export",
            "source_region",
            "destination_region",
            "destination_port",
            "C1",
            "C2"            
        ]
    )[["WT"]].sum()
    dd_import_table = dd_import_table.reset_index()

    # Step4. 產生所有時間段的虛擬資料，並與原始資料合併
    year = list(range(2016, 2023))
    month = list(range(1, 12))
    year_month_list = [
        *[
            (i, j)
            for i, j in itertools.product(year, month)
        ],
        *[(2024, 1), (2024, 2), (2024, 3), (2024, 4)]
    ]
    empty_data = [
        {
            "year": i[0],
            "month": i[1],
            "WT_zero": 0,
            "import_or_export": "e",
            "source_region": k,
            "source_port": l,
            "destination_region": m,
            "C1": n,
            "C2": o
        }
        for i, k, l, m, n, o in tqdm.contrib.itertools.product(
            year_month_list,
            dd_export_table["source_region"].unique().compute().tolist(),
            dd_export_table["source_port"].unique().compute().tolist(),
            dd_export_table["destination_region"].unique().compute().tolist(),
            dd_export_table["C1"].unique().compute().tolist(),
            dd_export_table["C2"].unique().compute().tolist()
        )
    ]
    dd_export_empty_table = dd.from_pandas(pd.DataFrame(empty_data), npartitions = 20).to_dask_dataframe()

    dd_export_table = dd_export_table.merge(
        right = dd_export_empty_table,
        how = "outer",
        on = ["year", "month", "import_or_export", "source_region", "source_port", "destination_region", "C1", "C2"]
    )
    dd_export_table = dd_export_table.assign(
        WT = dd_export_table["WT"].fillna(0.0) + dd_export_table["WT_zero"]
    )
    dd_export_table = dd_export_table.drop(columns = "WT_zero")

    empty_data = [
        {
            "year": i[0],
            "month": i[1],
            "WT_zero": 0,
            "import_or_export": "e",
            "source_region": k,
            "destination_region": l,
            "destination_port": m,
            "C1": n,
            "C2": o
        }
        for i, k, l, m, n, o in tqdm.contrib.itertools.product(
            year_month_list,
            dd_import_table["source_region"].unique().compute().tolist(),
            dd_import_table["destination_region"].unique().compute().tolist(),
            dd_import_table["destination_port"].unique().compute().tolist(),
            dd_import_table["C1"].unique().compute().tolist(),
            dd_import_table["C2"].unique().compute().tolist()
        )
    ]
    dd_import_empty_table = dd.from_pandas(pd.DataFrame(empty_data), npartitions = 20).to_dask_dataframe()

    dd_import_table = dd_import_table.merge(
        right = dd_import_empty_table,
        how = "outer",
        on = ["year", "month", "import_or_export", "source_region", "destination_region", "destination_port", "C1", "C2"]
    )
    dd_import_table = dd_import_table.assign(
        WT = dd_import_table["WT"].fillna(0.0) + dd_import_table["WT_zero"]
    )
    dd_import_table = dd_import_table.drop(columns = "WT_zero")

    # 把進口與出口的資料合併
    dd_table = dd.concat(
        [dd_export_table, dd_import_table],
        axis = 0
    ).reset_index(drop = True)

    # Step5. 依照進出口、來源國、來源港口、目的地國、目的地港口、貨物大類與貨物小類，將貨物重量往後推移一個
    dd_table_start = dd_table.groupby(
        by = [
            "import_or_export", 
            "source_region", 
            "source_port", 
            "destination_region", 
            "destination_port",
            "C1",
            "C2"
        ],
        as_index = False
    )
    dd_table_start = dd_table_start.apply(
        lambda x: x.sort_values(by = ["year", "month"])
    )[["year", "month", "WT"]]
    print(dd_table_start.head(20))
    dd_table_start = dd_table_start[["year", "month", "WT"]].shift(periods = 1)
    dd_table_start = dd_table_start.reset_index()
    
    
    # # Step6. 把起點與終點的貨物重量混合在一起
    # dd_table = dd.merge(
    #     left = dd_table,
    #     right = dd_table_start,
    #     how = "inner",
    #     on = [
    #         "import_or_export", 
    #         "source_region", 
    #         "source_port", 
    #         "destination_region", 
    #         "destination_port"
    #     ]
    # )
    # print(dd_table.head())

    return 

def read_excel_file_for_vegetable_and_fruit(
    file_name: str, 
    folder_path: str        
) -> pd.DataFrame:
    
    df = pd.read_excel(
        os.path.join(folder_path, file_name),
        header = 4
    ).iloc[:-1, :-1].reset_index(drop = True)
    return df

def merge_all_vegetable_or_fruit_data(
    folder_path: str,
    keyword: str
):
    
    """
    合併所有與蔬菜市場行情有關的資料，並把重複的資料排除掉。
    """



    # Step1. 讀取所有檔案
    fit_file_path = [
        i for i in os.listdir(folder_path)
        if keyword in i
    ]
    dd_table = from_map(
        read_excel_file_for_vegetable_and_fruit,
        fit_file_path,
        folder_path = folder_path
    )

    # Step2. 去除完全重複的資料
    dd_table = dd_table.drop_duplicates().reset_index(drop = True)

    # Step3. 從日期中提取出年、月跟日
    dd_table = dd_table.assign(
        年 = dd_table["日　　期"].apply(
            lambda x: x.split("/")[0] if x else None,
            meta = dd_table["日　　期"]
        ),
        月 = dd_table["日　　期"].apply(
            lambda x: x.split("/")[1] if x else None,
            meta = dd_table["日　　期"]
        ),
        日 = dd_table["日　　期"].apply(
            lambda x: x.split("/")[2] if x else None, 
            meta = dd_table["日　　期"]
        ),
    )

    # Step4. 整理一下資料型態
    dd_table = dd_table.assign(
        年 = lambda x: 1911 + x["年"].astype("int"),
        月 = dd_table["月"].apply(
            lambda x: x[1] if x[0] == "0" else x,
            meta = dd_table["月"]
        ).astype("int"),
        日 = dd_table["日"].apply(
            lambda x: x[1] if x[0] == "0" else x,
            meta = dd_table["日"]
        ).astype("int"),
        **{
            "平均價(元/公斤)": dd_table["平均價(元/公斤)"].astype("float"), 
            "交易量(公斤)": dd_table["交易量(公斤)"].astype("float") 
        }
    )    
    
    # Step5. 把相同市場、相同產品與相同日期的資料彙整成一個 List
    dd_table = dd_table.groupby(by = ["年", "月", "日", "產　　品"]).agg(
        {
            "平均價(元/公斤)": list,
            "交易量(公斤)": list
        }
    ).reset_index()

    print(dd_table.head())
    print(dd_table.info(verbose = True))
    return 

def merge_vegetable_data(
    main_df: pd.DataFrame,
    vegetable_df: pd.DataFrame
):
    
    """
    將主資料（預測為港口貨物進口量）與蔬菜市場行情資料合併。
    """

    return 

def merge_fruit_data(
    main_df: pd.DataFrame,
    vegetable_df: pd.DataFrame
):
    
    """
    將主資料（預測為港口貨物進口量）與蔬菜市場行情資料合併。
    """


    return 

def rolling_data():

    """
    將資料特定欄位位移至其他筆資料中。
    """

    return 

if __name__ == "__main__":
    main()