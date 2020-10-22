import datetime
from typing import NamedTuple

import pandas as pd


def _extract(series: pd.DatetimeIndex, part: str) -> pd.Index:
    if part == "date":
        return series.strftime("%Y-%m-%d")
    elif part == "time_minutes":
        return series.strftime("%H:%M")
    elif part == "time_seconds":
        return series.strftime("%H:%M:%S")
    elif part == "year":
        return series.year
    elif part == "month":
        return series.month
    elif part == "day":
        return series.day
    elif part == "hour":
        return series.hour
    elif part == "minute":
        return series.minute
    elif part == "second":
        return series.second
    else:
        raise NotImplemented("Unhandled part %r" % part)


def render(table, params):
    if not params["colname"] or not any(o["outcolname"] for o in params["outputs"]):
        return table

    column = table[params["colname"]]
    dt = pd.DatetimeIndex(column, tz="UTC").tz_convert(params["timezone"])
    index = table.columns.get_loc(column.name)

    # Delete the input column
    del table[params["colname"]]
    # Place new columns in the spot where the input column was
    for output in params["outputs"]:
        outcolname = output["outcolname"]
        if outcolname:
            if outcolname in table.columns:
                # delete existing column from somewhere else in the table
                nixed_index = table.columns.get_loc(outcolname)
                del table[outcolname]
                if nixed_index < index:
                    index -= 1
            table.insert(index, output["outcolname"], _extract(dt, output["part"]))
            index += 1

    return table
