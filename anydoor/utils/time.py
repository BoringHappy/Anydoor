# -*- coding:utf-8 -*-
"""
filename : time_function.py
create_time : 2022/1/2 14:04
author : Demon Finch
"""

from datetime import date, timedelta


class TimeUtils:
    @classmethod
    def last_day_of_month(cls, any_day: date) -> date:
        next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
        return next_month - timedelta(days=next_month.day)

    @classmethod
    def this_weekday(cls, day: date, weekday) -> date:
        """
        获取本周日期
        :param day: 日期
        :param weekday: 返回设定周几的日期
        """
        return day - timedelta(day.weekday()) + timedelta(weekday - 1)
