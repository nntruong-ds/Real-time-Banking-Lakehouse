"""
Kiểm tra DataFrame mà các job ghi ra khớp ĐÚNG THỨ TỰ và KIỂU với câu CREATE TABLE.

Iceberg ghi theo vị trí cột: đổi thứ tự cột trong code mà quên sửa DDL (hoặc ngược
lại) thì job lỗi lúc chạy, hoặc tệ hơn là ghi nhầm cột. Bài test bắt lỗi đó trên máy.
"""

import re

from batch_backfill import read_validated_csv
from common import tables
from helpers import validated_from_events
from replay import row_to_event
from sample_data import make_rows, write_csv

KEY = b"k" * 40


def ddl_columns(create_sql: str) -> str:
    """Lấy phần "tên KIỂU, ..." trong CREATE TABLE, bỏ COMMENT."""
    body = create_sql[create_sql.index("(") + 1: create_sql.index("USING")]
    body = body[: body.rindex(")")]
    cols = []
    for line in body.splitlines():
        line = re.sub(r"\s+COMMENT\s+'[^']*'", "", line).strip().rstrip(",")
        if line:
            name, dtype = line.split()[:2]
            cols.append(f"{name} {dtype}")
    return ", ".join(cols)


def fields(schema):
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


def expected(spark, create_sql):
    return fields(spark.createDataFrame([], ddl_columns(create_sql)).schema)


def check_all(spark, validated):
    assert fields(tables.valid_rows(validated).schema) == expected(spark, tables.CREATE_BRONZE)
    assert fields(tables.invalid_rows(validated).schema) == expected(spark, tables.CREATE_DLQ)
    silver = tables.to_silver(tables.dedup_batch(tables.valid_rows(validated)), KEY)
    assert fields(silver.schema) == expected(spark, tables.CREATE_SILVER)


def test_stream_path_matches_ddl(spark):
    check_all(spark, validated_from_events(spark, [row_to_event(make_rows(1)[0])]))


def test_batch_path_matches_ddl(spark, tmp_path):
    path = str(tmp_path / "one.csv")
    write_csv(make_rows(3), path)
    check_all(spark, read_validated_csv(spark, path))
