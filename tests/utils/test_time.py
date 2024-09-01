from anydoor.utils.time import TimeUtils
from datetime import date


def test_time_utils():
    assert TimeUtils.last_day_of_month(date(2022, 1, 1)) == date(2022, 1, 31)
    assert TimeUtils.this_weekday(date(2022, 1, 1), 1) == date(2021, 12, 27)
    assert TimeUtils.nearest_weekday(date(2022, 1, 1), 1) == date(2021, 12, 27)
    assert TimeUtils.nearest_weekday(date(2024, 8, 27), 1) == date(2024, 8, 26)
    assert TimeUtils.nearest_weekday(date(2024, 8, 27), 6) == date(2024, 8, 24)
    assert TimeUtils.nearest_weekday(date(2024, 8, 27), 6) != date(2024, 8, 25)
