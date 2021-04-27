import datetime
from pathlib import Path

import pyarrow as pa
from cjwmodule.arrow.testing import assert_result_equals, make_column, make_table
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.spec.testing import param_factory

from splittimestamp import render_arrow_v1 as render

P = param_factory(Path(__name__).parent.parent / "splittimestamp.yaml")


def dt(year=2000, month=1, day=1, hour=0, minute=0, second=0):
    return datetime.datetime(year, month, day, hour, minute, second)


def test_render_no_colname_is_no_op():
    assert_result_equals(
        render(
            make_table(make_column("A", [dt()])),
            P(outputs=[dict(outcolname="B", part="date")]),
        ),
        ArrowRenderResult(make_table(make_column("A", [dt()]))),
    )


def test_render_no_outcolname_is_no_op():
    assert_result_equals(
        render(
            make_table(make_column("A", [dt()])),
            P(colname="A", outputs=[dict(outcolname="", part="date")]),
        ),
        ArrowRenderResult(make_table(make_column("A", [dt()]))),
    )


def test_render_date():
    assert_result_equals(
        render(
            make_table(make_column("A", [dt(2000), dt(2001, 2, 3), None])),
            P(colname="A", outputs=[dict(outcolname="B", part="date")]),
        ),
        ArrowRenderResult(
            make_table(
                make_column(
                    "B", [datetime.date(2000, 1, 1), datetime.date(2001, 2, 3), None]
                )
            )
        ),
    )


def test_render_time_minutes():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="time_minutes")]),
        ),
        ArrowRenderResult(make_table(make_column("B", ["02:03", "13:00", None]))),
    )


def test_render_time_seconds():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="time_seconds")]),
        ),
        ArrowRenderResult(make_table(make_column("B", ["02:03:04", "13:00:06", None]))),
    )


def test_render_number_year():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="year")]),
        ),
        ArrowRenderResult(
            make_table(make_column("B", [2000, 2001, None], pa.int16(), format="{:d}"))
        ),
    )


def test_render_number_month():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="month")]),
        ),
        ArrowRenderResult(
            make_table(make_column("B", [1, 2, None], pa.int8(), format="{:d}"))
        ),
    )


def test_render_number_day():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="day")]),
        ),
        ArrowRenderResult(
            make_table(make_column("B", [1, 3, None], pa.int8(), format="{:d}"))
        ),
    )


def test_render_number_hour():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="hour")]),
        ),
        ArrowRenderResult(
            make_table(make_column("B", [2, 13, None], pa.int8(), format="{:d}"))
        ),
    )


def test_render_number_minute():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="minute")]),
        ),
        ArrowRenderResult(
            make_table(make_column("B", [3, 0, None], pa.int8(), format="{:d}"))
        ),
    )


def test_render_number_second():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A", [dt(2000, 1, 1, 2, 3, 4), dt(2001, 2, 3, 13, 0, 6), None]
                )
            ),
            P(colname="A", outputs=[dict(outcolname="B", part="second")]),
        ),
        ArrowRenderResult(
            make_table(make_column("B", [4, 6, None], pa.int8(), format="{:d}"))
        ),
    )


def test_render_replace_input_column():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1]),
                make_column("B", [dt(2000, 1, 1)]),
                make_column("C", ["c"]),
            ),
            P(colname="B", outputs=[dict(outcolname="B", part="time_minutes")]),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [1]),
                make_column("B", ["00:00"]),
                make_column("C", ["c"]),
            ),
        ),
    )


def test_render_replace_earlier_column_with_same_name():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1]),
                make_column("B", [dt(2000, 1, 1)]),
                make_column("C", ["c"]),
            ),
            P(colname="B", outputs=[dict(outcolname="A", part="time_minutes")]),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", ["00:00"]),
                make_column("C", ["c"]),
            ),
        ),
    )


def test_render_replace_later_column_with_same_name():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1]),
                make_column("B", [dt(2000, 1, 1)]),
                make_column("C", ["c"]),
            ),
            P(colname="B", outputs=[dict(outcolname="C", part="time_minutes")]),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [1]),
                make_column("C", ["00:00"]),
            ),
        ),
    )


def test_render_replace_many_columns():
    assert_result_equals(
        render(
            make_table(
                make_column("A", ["a"]),
                make_column("B", ["b"]),
                make_column("C", [dt(2000, 2, 3, 4)]),
                make_column("D", ["d"]),
                make_column("E", ["e"]),
                make_column("F", ["f"]),
            ),
            P(
                colname="C",
                outputs=[
                    dict(outcolname="A", part="dateyear"),
                    dict(outcolname="F", part="datemonth"),
                    dict(outcolname="D", part="date"),
                    dict(outcolname="G", part="time_minutes"),
                ],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("B", ["b"]),
                make_column("A", [datetime.date(2000, 1, 1)], unit="year"),
                make_column("F", [datetime.date(2000, 2, 1)], unit="month"),
                make_column("D", [datetime.date(2000, 2, 3)]),
                make_column("G", ["04:00"]),
                make_column("E", ["e"]),
            ),
        ),
    )


def test_render_duplicate_output_column_name():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1]),
                make_column("B", [dt(2000, 1, 1)]),
            ),
            P(
                colname="B",
                outputs=[
                    dict(outcolname="C", part="time_minutes"),
                    dict(outcolname="C", part="time_seconds"),
                ],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [1]),
                make_column("C", ["00:00:00"]),
            ),
        ),
    )


def test_render_timezone_positive_offset():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A",
                    [
                        dt(2000, 1, 1),
                        dt(2000, 1, 1, 4, 59),
                        # DST starts 2am Apr 2, 2000 -- and becomes 3am
                        dt(2000, 4, 2, 5),
                        dt(2000, 4, 2, 6),
                        dt(2000, 4, 2, 7),
                        # DST ends 2am Oct 29, 2000 -- and becomes 1am
                        dt(2000, 10, 29, 5),
                        dt(2000, 10, 29, 6),
                        dt(2000, 10, 29, 7),
                    ],
                )
            ),
            P(
                colname="A",
                timezone="America/Montreal",
                outputs=[
                    dict(outcolname="date", part="date"),
                    dict(outcolname="time_minutes", part="time_minutes"),
                    dict(outcolname="year", part="year"),
                ],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column(
                    "date",
                    [
                        datetime.date(1999, 12, 31),
                        datetime.date(1999, 12, 31),
                        datetime.date(2000, 4, 2),
                        datetime.date(2000, 4, 2),
                        datetime.date(2000, 4, 2),
                        datetime.date(2000, 10, 29),
                        datetime.date(2000, 10, 29),
                        datetime.date(2000, 10, 29),
                    ],
                ),
                make_column(
                    "time_minutes",
                    [
                        "19:00",
                        "23:59",
                        "00:00",
                        "01:00",
                        "03:00",
                        "01:00",
                        "01:00",
                        "02:00",
                    ],
                ),
                make_column(
                    "year",
                    [1999, 1999, 2000, 2000, 2000, 2000, 2000, 2000],
                    pa.int16(),
                    format="{:d}",
                ),
            )
        ),
    )


def test_render_date_timezone_converted_null_time_is_none():
    assert_result_equals(
        render(
            make_table(make_column("A", [None], pa.timestamp("ns"))),
            P(
                colname="A",
                timezone="Pacific/Honolulu",
                outputs=[dict(outcolname="B", part="time_minutes")],
            ),
        ),
        ArrowRenderResult(make_table(make_column("B", [None], pa.utf8()))),
    )
