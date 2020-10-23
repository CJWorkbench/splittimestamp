import datetime
import pandas as pd
from pandas.testing import assert_frame_equal

from splittimestamp import render


def dt(year=2000, month=1, day=1, hour=0, minute=0, second=0):
    return datetime.datetime(year, month, day, hour, minute, second)


def test_render_no_colname():
    result = render(
        pd.DataFrame({"A": [dt()]}),
        {
            "colname": "",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "date"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"A": [dt()]}))


def test_render_no_outcolname():
    result = render(
        pd.DataFrame({"A": [dt()]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "", "part": "date"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"A": [dt()]}))


def test_render_date():
    result = render(
        pd.DataFrame({"A": [dt(2000), dt(2001, 2, 3), pd.NaT]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "date"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": ["2000-01-01", "2001-02-03", None]}))


def test_render_date_null_time_is_none():
    result = render(
        pd.DataFrame({"A": [dt(2000), pd.NaT]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "date"}],
        },
    )
    assert result["B"][1] is None


def test_render_time_minutes():
    result = render(
        pd.DataFrame(
            {"A": [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), pd.NaT]}
        ),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "time_minutes"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": ["02:03", "13:00", None]}))


def test_render_time_seconds():
    result = render(
        pd.DataFrame(
            {"A": [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), pd.NaT]}
        ),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "time_seconds"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": ["02:03:04", "13:00:06", None]}))


def test_render_time_year():
    result = render(
        pd.DataFrame(
            {"A": [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), pd.NaT]}
        ),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "year"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [2000, 2001, None]}))  # float


def test_render_time_year_no_nulls():
    result = render(
        pd.DataFrame({"A": [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6)]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "year"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [2000, 2001]}))  # int


def test_render_time_month():
    result = render(
        pd.DataFrame(
            {"A": [dt(2000, 1, 1, 2, 3, 4), dt(2001, 12, 3, 13, 0, 6), pd.NaT]}
        ),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "month"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [1, 12, None]}))


def test_render_time_day():
    result = render(
        pd.DataFrame({"A": [dt(2000, 1, 1), dt(2001, 12, 31, 13), pd.NaT]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "day"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [1, 31, None]}))


def test_render_time_hour():
    result = render(
        pd.DataFrame({"A": [dt(2000, 1, 1, 0), dt(2001, 12, 31, 13), pd.NaT]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "hour"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [0, 13, None]}))


def test_render_time_minute():
    result = render(
        pd.DataFrame({"A": [dt(2000, 1, 1, 0, 0), dt(2001, 12, 31, 13, 42), pd.NaT]}),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "minute"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [0, 42, None]}))


def test_render_time_second():
    result = render(
        pd.DataFrame(
            {"A": [dt(2000, 1, 1, 0, 0, 0), dt(2001, 12, 31, 13, 42, 59), pd.NaT]}
        ),
        {
            "colname": "A",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "second"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"B": [0, 59, None]}))


def test_render_replace_input_column():
    result = render(
        pd.DataFrame({"A": [1], "B": [dt(2000)], "C": [1]}),
        {
            "colname": "B",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "year"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"A": [1], "B": [2000], "C": [1]}))


def test_render_replace_earlier_column_with_same_name():
    result = render(
        pd.DataFrame({"A": [1], "B": [1], "C": [dt(2000)], "D": [1]}),
        {
            "colname": "C",
            "timezone": "UTC",
            "outputs": [{"outcolname": "B", "part": "year"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"A": [1], "B": [2000], "D": [1]}))


def test_render_replace_later_column_with_same_name():
    result = render(
        pd.DataFrame({"A": [1], "B": [dt(2000)], "C": [1], "D": [1]}),
        {
            "colname": "B",
            "timezone": "UTC",
            "outputs": [{"outcolname": "C", "part": "year"}],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"A": [1], "C": [2000], "D": [1]}))


def test_render_replace_many_columns():
    result = render(
        pd.DataFrame(
            {"A": [1], "B": [1], "C": [dt(2000, 2, 3, 4)], "D": [1], "E": [1], "F": [1]}
        ),
        {
            "colname": "C",
            "timezone": "UTC",
            "outputs": [
                {"outcolname": "A", "part": "year"},
                {"outcolname": "F", "part": "month"},
                {"outcolname": "D", "part": "day"},
                {"outcolname": "G", "part": "hour"},
            ],
        },
    )
    assert_frame_equal(
        result,
        pd.DataFrame({"B": [1], "A": [2000], "F": [2], "D": [3], "G": [4], "E": [1]}),
    )


def test_render_duplicate_output_column_name():
    result = render(
        pd.DataFrame({"A": [1], "B": [dt(2000)], "C": [1]}),
        {
            "colname": "B",
            "timezone": "UTC",
            "outputs": [
                {"outcolname": "C", "part": "year"},
                {"outcolname": "C", "part": "year"},
            ],
        },
    )
    assert_frame_equal(result, pd.DataFrame({"A": [1], "C": [2000]}))


def test_render_timezone_positive_offset():
    result = render(
        pd.DataFrame(
            {
                "A": pd.to_datetime(
                    [
                        "2000-01-01T00:00",
                        "2000-01-01T04:59",
                        # DST starts 2am Apr 2, 2000 -- and becomes 3am
                        "2000-04-02T05:00",
                        "2000-04-02T06:00",
                        "2000-04-02T07:00",
                        # DST ends 2am Oct 29, 2000 -- and becomes 1am
                        "2000-10-29T05:00",
                        "2000-10-29T06:00",
                        "2000-10-29T07:00",
                    ]
                )
            }
        ),
        {
            "colname": "A",
            "timezone": "America/Montreal",
            "outputs": [
                {"outcolname": "date", "part": "date"},
                {"outcolname": "time_minutes", "part": "time_minutes"},
                {"outcolname": "year", "part": "year"},
            ],
        },
    )
    assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "date": [
                    "1999-12-31",
                    "1999-12-31",
                    "2000-04-02",
                    "2000-04-02",
                    "2000-04-02",
                    "2000-10-29",
                    "2000-10-29",
                    "2000-10-29",
                ],
                "time_minutes": [
                    "19:00",
                    "23:59",
                    "00:00",
                    "01:00",
                    "03:00",
                    "01:00",
                    "01:00",
                    "02:00",
                ],
                "year": [1999, 1999, 2000, 2000, 2000, 2000, 2000, 2000],
            }
        ),
    )


def test_render_date_timezone_converted_null_time_is_none():
    result = render(
        pd.DataFrame({"A": [dt(2000, 1, 2), pd.NaT]}),
        {
            "colname": "A",
            "timezone": "Pacific/Honolulu",
            "outputs": [{"outcolname": "B", "part": "date"}],
        },
    )
    assert result["B"][1] is None
