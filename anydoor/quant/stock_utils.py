# -*- coding: utf-8 -*-
"""
Stock utility functions for market data processing
"""

sh_head = ("50", "51", "60", "90", "110", "113", "132", "204", "5", "6", "9", "7")


def get_stock_type(stock_code):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    assert type(stock_code) is str, "stock code need str type"

    if len(stock_code) < 6:
        return ""
    if stock_code.startswith(("sh", "sz", "zz")):
        return stock_code
    else:
        return (
            "sh" + stock_code if stock_code.startswith(sh_head) else "sz" + stock_code
        )
