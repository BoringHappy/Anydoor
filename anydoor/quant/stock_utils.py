# -*- coding: utf-8 -*-
"""
@author : Demon Finch
@file   : stock_utils.py
@create : 2022/2/23 21:58
"""
from functools import lru_cache

import pandas as pd
from ..dbs.postgres import Postgres


def get_stock_type(stock_code):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    assert type(stock_code) is str, "stock code need str type"
    sh_head = ("50", "51", "60", "90", "110", "113", "132", "204", "5", "6", "9", "7")
    if len(stock_code) < 6:
        return ""
    if stock_code.startswith(("sh", "sz", "zz")):
        return stock_code
    else:
        return (
            "sh" + stock_code if stock_code.startswith(sh_head) else "sz" + stock_code
        )


@lru_cache()
def get_trade_cal():
    engine = Postgres(database="stock", schema="crawl")
    df = pd.read_sql("select * from crawl.trade_cal", con=engine.engine)
    return df.sort_values("trade_cal")


@lru_cache()
def is_trade_date(select_day=None):
    from datetime import date

    select_day = select_day or str(date.today())

    trade_days = [i.strftime("%Y-%m-%d") for i in get_trade_cal().trade_cal.to_list()]
    if select_day in trade_days:
        print(f"{select_day} 交易日")
        return True
    else:
        print(f"{select_day} 非交易日")
        return False
