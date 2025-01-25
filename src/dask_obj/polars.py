from io import StringIO
from itertools import islice

import fsspec
import polars as pl
from polars import List as PlList, Struct as PlStruct


def pl_read_ndjson_fsspec(file, lines=None):
    with fsspec.open(file, "rt", compression="infer") as f:
        if lines is not None:
            data = "".join(islice(f, lines))
        else:
            data = f.read()
    data = StringIO(data)
    return pl.read_ndjson(data)


def unnest(df):
    """Unnest a dataframe with struct columns."""
    data = []
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == pl.Struct:
            fields = df[col].struct.fields
            _rename_fields = [f"{col}_{f}" for f in fields]
            rename_fields = []
            for f in _rename_fields:
                if f in columns:
                    i = 1
                    while f"{f}_{i}" in columns:
                        i += 1
                    f = f"{f}_{i}"
                rename_fields.append(f)
            unnested = (
                df[col]
                .struct.rename_fields(rename_fields)
                .to_frame()
                .unnest(col)
                .pipe(unnest)
            )
            data.extend(unnested.get_columns())
            columns.extend(unnested.columns)
        else:
            data.append(df[col])
            columns.append(col)
    out = pl.DataFrame(data)
    return out


def explode(df):
    """Explode a dataframe with list columns."""
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == PlList:
            df = df.explode(col)
    return df


def expand(df):
    """Expand a dataframe with struct and list columns."""
    while any(_ == PlStruct or _ == PlList for _ in df.dtypes):
        df = df.pipe(explode).pipe(unnest)
    return df
