import datetime
import os
import time
from typing import Any, Callable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute
from cjwmodule.arrow.types import ArrowRenderResult


def _extract_struct_times_from_array(
    array: pa.Array,
) -> List[Optional[time.struct_time]]:
    unix_timestamps = pa.compute.divide(array.view(pa.int64()), 1_000_000_000)
    unix_timestamp_list = unix_timestamps.to_pylist()
    return [None if ts is None else time.localtime(ts) for ts in unix_timestamp_list]


def _extract_struct_times_from_chunked_array(
    input_chunked_array: pa.ChunkedArray,
) -> List[List[Optional[time.struct_time]]]:
    return [
        _extract_struct_times_from_array(chunk) for chunk in input_chunked_array.chunks
    ]


def _build_date_builder(name: str, part: str):
    if part == "date":
        unit = "day"

        def fn(st: time.struct_time) -> datetime.date:
            return datetime.date(st.tm_year, st.tm_mon, st.tm_mday)

    elif part == "dateweek":
        unit = "week"

        def fn(st: time.struct_time) -> datetime.date:
            return datetime.date.fromordinal(
                datetime.date(st.tm_year, st.tm_mon, st.tm_mday).toordinal()
                - st.tm_wday
            )

    elif part == "datemonth":
        unit = "month"

        def fn(st: time.struct_time) -> datetime.date:
            return datetime.date(st.tm_year, st.tm_mon, 1)

    elif part == "datequarter":
        unit = "quarter"

        def fn(st: time.struct_time) -> datetime.date:
            return datetime.date(
                st.tm_year, [0, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10][st.tm_mon], 1
            )

    else:
        unit = "year"

        def fn(st: time.struct_time) -> datetime.date:
            return datetime.date(st.tm_year, 1, 1)

    return pa.field(name, pa.date32(), metadata={"unit": unit}), fn


def _build_string_builder(name: str, part: str):
    if part == "time_minutes":
        fmt = "{3:02d}:{4:02d}".format
    else:
        fmt = "{3:02d}:{4:02d}:{5:02d}".format

    def fn(st: time.struct_time) -> str:
        return fmt(*st)

    return pa.field(name, pa.utf8()), fn


def _build_number_builder(name: str, part: str):
    pa_type, struct_time_index = {
        "year": (pa.int16(), 0),
        "month": (pa.int8(), 1),
        "day": (pa.int8(), 2),
        "hour": (pa.int8(), 3),
        "minute": (pa.int8(), 4),
        "second": (pa.int8(), 5),
    }[part]

    def fn(st: time.struct_time) -> int:
        return st[struct_time_index]

    return pa.field(name, pa_type, metadata={"format": "{:d}"}), fn


def _build_output_array(
    field: pa.Field,
    struct_times: List[Optional[time.struct_time]],
    fn: Callable[[time.struct_time], Any],
) -> pa.Array:
    values = [None if st is None else fn(st) for st in struct_times]
    return pa.array(values, field.type)


def _build_output_column(
    name: str, struct_times: List[List[Optional[time.struct_time]]], part: str
) -> Tuple[pa.Field, pa.ChunkedArray]:
    if part in {"date", "dateweek", "datemonth", "datequarter", "dateyear"}:
        field, fn = _build_date_builder(name, part)
    elif part in {"time_minutes", "time_seconds"}:
        field, fn = _build_string_builder(name, part)
    else:
        field, fn = _build_number_builder(name, part)

    chunked_array = pa.chunked_array(
        [_build_output_array(field, l, fn) for l in struct_times], field.type
    )
    return field, chunked_array


def render_arrow_v1(table, params, **kwargs):
    if not params["colname"] or not any(o["outcolname"] for o in params["outputs"]):
        return ArrowRenderResult(table)

    os.environ["TZ"] = params["timezone"]
    time.tzset()

    # Prepare times as Python `time.struct_time`
    struct_times = _extract_struct_times_from_chunked_array(table[params["colname"]])

    # unique-ize output columns. Python dict iterates in insertion order
    unique_out_columns = {o["outcolname"]: o["part"] for o in params["outputs"]}
    output_colnames = frozenset(unique_out_columns.keys())

    # the output table will be:
    # * input table...
    # * ... minus params["colname"]...
    # * ... minus all the columns with names we're going to replace
    # * ... plus all the columns we're creating
    input_index = table.schema.get_field_index(params["colname"])
    insert_index = input_index - sum(
        1
        for colname in output_colnames
        if 0 <= table.schema.get_field_index(colname) < input_index
    )

    # Drop all the columns we'll be replacing
    table = table.drop(
        [
            params["colname"],
            *(
                colname
                for colname in table.column_names
                if colname in output_colnames and colname != params["colname"]
            ),
        ]
    )

    for i, (colname, part) in enumerate(unique_out_columns.items()):
        field, data = _build_output_column(colname, struct_times, part)
        table = table.add_column(insert_index + i, field, data)
    return ArrowRenderResult(table)
