#!/usr/bin/env python3
import pathlib
import re
import sys
from textwrap import dedent

# Map markdown types -> PySpark T.*
TYPE_MAP = {
    "string": "T.StringType()",
    "int": "T.IntegerType()",
    "integer": "T.IntegerType()",
    "long": "T.LongType()",
    "bigint": "T.LongType()",
    "double": "T.DoubleType()",
    "float": "T.FloatType()",
    "decimal": "T.DoubleType()",  # adjust if you carry precision/scale
    "boolean": "T.BooleanType()",
    "date": "T.DateType()",
    "timestamp": "T.TimestampType()",
}


def parse_markdown_table(md_text: str):
    rows = []
    lines = [line.rstrip() for line in md_text.splitlines() if line.strip()]

    # find table start (line with pipes and header separator after it)
    for i in range(len(lines) - 1):
        if lines[i].count("|") >= 2 and re.match(
            r"^\s*\|?[-:\s|]+\|?\s*$", lines[i + 1]
        ):
            start = i
            break
    else:
        raise SystemExit(
            "Could not find a markdown table with a header separator (---)."
        )

    # collect table lines until a blank or non-pipe
    tbl = []
    for j in range(start, len(lines)):
        if "|" in lines[j]:
            tbl.append(lines[j])
        else:
            break

    # split rows into cells
    def split_row(line):
        # remove leading/trailing pipes, split, and strip
        parts = [c.strip() for c in line.strip().strip("|").split("|")]
        return parts

    header = [h.lower() for h in split_row(tbl[0])]
    try:
        c_idx = header.index("column_name")
        t_idx = header.index("type")
    except ValueError:
        raise SystemExit("Table must have headers 'column_name' and 'type'.")

    # data rows (skip header + separator)
    for row in tbl[2:]:
        cells = split_row(row)
        if len(cells) < max(c_idx, t_idx) + 1:
            continue
        col = cells[c_idx].strip()
        typ = cells[t_idx].strip().lower()
        rows.append((col, typ))
    return rows


def main(md_path: str, out_path: str):
    md = pathlib.Path(md_path).read_text(encoding="utf-8")
    rows = parse_markdown_table(md)
    if not rows:
        raise SystemExit("No data rows parsed from schema table.")

    # build StructType code
    struct_lines = []
    for col, typ in rows:
        if typ not in TYPE_MAP:
            raise SystemExit(
                f"Unmapped type '{typ}' for column '{col}'. "
                f"Add a mapping in TYPE_MAP."
            )
        struct_lines.append(f'    T.StructField("{col}", {TYPE_MAP[typ]}, True),')

    code = dedent(
        f"""\
    # Auto-generated from docs/schema.md. Do not edit by hand.
    from pyspark.sql import types as T

    SCHEMA_COLUMNS = { [c for c,_ in rows] }

    SCHEMA = T.StructType([
    {chr(10).join(struct_lines)}
    ])
    """
    )

    pathlib.Path(out_path).write_text(code, encoding="utf-8")
    print(f"Wrote schema with {len(rows)} columns -> {out_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: gen_schema_from_md.py <docs/schema.md> <databricks/notebooks/schema.py>"
        )
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
