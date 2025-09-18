# -*- coding:utf-8 -*-
"""
Time utility functions for date manipulation and calendar operations.

This module provides utility functions for common date and time operations
including finding the last day of a month, calculating weekdays, and
determining nearest weekday dates.
"""

from datetime import date, timedelta


class TimeUtils:
    """
    Utility class for date and time operations.

    Provides static methods for common date calculations including
    month boundaries, weekday calculations, and relative date operations.
    """

    @classmethod
    def last_day_of_month(cls, any_day: date) -> date:
        """
        Calculate the last day of the month for a given date.

        Args:
            any_day (date): The input date

        Returns:
            date: The last day of the month containing the input date

        Example:
            >>> TimeUtils.last_day_of_month(date(2023, 2, 15))
            datetime.date(2023, 2, 28)
        """
        next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
        return next_month - timedelta(days=next_month.day)

    @classmethod
    def this_weekday(cls, day: date, weekday) -> date:
        """
        Get the date of the specified weekday in the same week as the given date.

        Args:
            day (date): The reference date
            weekday (int): Target weekday (0=Monday, 1=Tuesday, ..., 6=Sunday)

        Returns:
            date: The date of the specified weekday in the same week

        Example:
            >>> TimeUtils.this_weekday(date(2023, 3, 15), 0)  # Wednesday to Monday
            datetime.date(2023, 3, 13)
        """
        return day - timedelta(day.weekday()) + timedelta(weekday - 1)

    @classmethod
    def nearest_weekday(cls, day: date, weekday) -> date:
        """
        Get the nearest occurrence of the specified weekday relative to the given date.

        If the target weekday has already passed in the current week, returns
        the weekday from the previous week. Otherwise, returns the weekday
        from the current week.

        Args:
            day (date): The reference date
            weekday (int): Target weekday (0=Monday, 1=Tuesday, ..., 6=Sunday)

        Returns:
            date: The nearest occurrence of the specified weekday

        Example:
            >>> TimeUtils.nearest_weekday(date(2023, 3, 15), 0)  # Wednesday, nearest Monday
            datetime.date(2023, 3, 13)  # Previous Monday
        """
        if day.weekday() >= weekday:
            return cls.this_weekday(day, weekday)
        else:
            return cls.this_weekday(day, weekday) - timedelta(7)
