"""
Utility functions for reading the standardised text2sql datasets presented in
`"Improving Text to SQL Evaluation Methodology" <https://arxiv.org/abs/1806.09029>`_
"""
import json
import os
import sqlite3
from collections import defaultdict
from typing import List, Dict, Optional
import json
import sqlite3
from nltk import word_tokenize

from allennlp.common import JsonDict

# from spider_evaluation.process_sql import get_tables_with_alias, parse_sql


class TableColumn:
    def __init__(
        self,
        name: str,
        text: str,
        column_type: str,
        is_primary_key: bool,
        foreign_key: Optional[str],
    ):
        self.name = name
        self.text = text
        self.column_type = column_type
        self.is_primary_key = is_primary_key
        self.foreign_key = foreign_key


class Table:
    def __init__(self, name: str, text: str, columns: List[TableColumn]):
        self.name = name
        self.text = text
        self.columns = columns


def read_dataset_schema(schema_path: str) -> Dict[str, List[Table]]:
    schemas: Dict[str, Dict[str, Table]] = defaultdict(dict)
    with open(schema_path, "r") as f:
        dbs_json_blob = json.load(f)
        for db in dbs_json_blob:
            db_id = db["db_id"]

            column_id_to_table = {}
            column_id_to_column = {}

            for i, (column, text, column_type) in enumerate(
                zip(db["column_names_original"], db["column_names"], db["column_types"])
            ):
                table_id, column_name = column
                _, column_text = text

                table_name = db["table_names_original"][table_id].lower()

                if table_name not in schemas[db_id]:
                    table_text = db["table_names"][table_id]
                    schemas[db_id][table_name] = Table(table_name, table_text, [])

                if column_name == "*":
                    continue

                is_primary_key = i in db["primary_keys"]
                table_column = TableColumn(
                    column_name.lower(), column_text, column_type, is_primary_key, None
                )
                schemas[db_id][table_name].columns.append(table_column)
                column_id_to_table[i] = table_name
                column_id_to_column[i] = table_column

            for (c1, c2) in db["foreign_keys"]:
                foreign_key = (
                    column_id_to_table[c2] + ":" + column_id_to_column[c2].name
                )
                column_id_to_column[c1].foreign_key = foreign_key

    return {**schemas}


def read_dataset_values(db_id: str, dataset_path: str, tables: List[str]):
    db = os.path.join(dataset_path, db_id, db_id + ".sqlite")
    try:
        conn = sqlite3.connect(db)
    except Exception as e:
        raise Exception(f"Can't connect to SQL: {e} in path {db}")
    conn.text_factory = str
    cursor = conn.cursor()

    values = {}
    c = "1"  # TODO: fixme
    if False:  # fixme
        for table in tables:
            try:
                cursor.execute(f"SELECT * FROM {table.name} LIMIT {c}")
                values[table] = cursor.fetchall()
            except:
                conn.text_factory = lambda x: str(x, "latin1")
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM {table.name} LIMIT {c}")
                values[table] = cursor.fetchall()

    return values


def ent_key_to_name(key):
    parts = key.split(":")
    if parts[0] == "table":
        return parts[1]
    elif parts[0] == "column":
        _, _, table_name, column_name = parts
        return f"{table_name}@{column_name}"
    else:
        return parts[1]


def fix_number_value(ex: JsonDict):
    """
    There is something weird in the dataset files - the `query_toks_no_value` field anonymizes all values,
    which is good since the evaluator doesn't check for the values. But it also anonymizes numbers that
    should not be anonymized: e.g. LIMIT 3 becomes LIMIT 'value', while the evaluator fails if it is not a number.
    """

    def split_and_keep(s, sep):
        if not s:
            return [""]  # consistent with string.split()

        # Find replacement character that is not used in string
        # i.e. just use the highest available character plus one
        # Note: This fails if ord(max(s)) = 0x10FFFF (ValueError)
        p = chr(ord(max(s)) + 1)

        return s.replace(sep, p + sep + p).split(p)

    # input is tokenized in different ways... so first try to make splits equal
    query_toks = ex["query_toks"]
    ex["query_toks"] = []
    for q in query_toks:
        ex["query_toks"] += split_and_keep(q, ".")

    i_val, i_no_val = 0, 0
    while i_val < len(ex["query_toks"]) and i_no_val < len(ex["query_toks_no_value"]):
        if ex["query_toks_no_value"][i_no_val] != "value":
            i_val += 1
            i_no_val += 1
            continue

        i_val_end = i_val
        while (
            i_val + 1 < len(ex["query_toks"])
            and i_no_val + 1 < len(ex["query_toks_no_value"])
            and ex["query_toks"][i_val_end + 1].lower()
            != ex["query_toks_no_value"][i_no_val + 1].lower()
        ):
            i_val_end += 1

        if (
            i_val == i_val_end
            and ex["query_toks"][i_val] in ["1", "2", "3", "4", "5"]
            and ex["query_toks"][i_val - 1].lower() == "limit"
        ):
            ex["query_toks_no_value"][i_no_val] = ex["query_toks"][i_val]
        i_val = i_val_end

        i_val += 1
        i_no_val += 1

    return ex


_schemas_cache = None


def get_schema_from_db_id(db_id: str, tables_file: str):
    class Schema:
        """
        Simple schema which maps table&column to a unique identifier
        """

        def __init__(self, schema, table):
            self._schema = schema
            self._table = table
            self._idMap = self._map(self._schema, self._table)

        @property
        def schema(self):
            return self._schema

        @property
        def idMap(self):
            return self._idMap

        def _map(self, schema, table):
            column_names_original = table["column_names_original"]
            table_names_original = table["table_names_original"]
            # print 'column_names_original: ', column_names_original
            # print 'table_names_original: ', table_names_original
            for i, (tab_id, col) in enumerate(column_names_original):
                if tab_id == -1:
                    idMap = {"*": i}
                else:
                    key = table_names_original[tab_id].lower()
                    val = col.lower()
                    idMap[key + "." + val] = i

            for i, tab in enumerate(table_names_original):
                key = tab.lower()
                idMap[key] = i

            return idMap

    def get_schemas_from_json(fpath):
        global _schemas_cache

        if _schemas_cache is not None:
            return _schemas_cache

        with open(fpath) as f:
            data = json.load(f)
        db_names = [db["db_id"] for db in data]

        tables = {}
        schemas = {}
        for db in data:
            db_id = db["db_id"]
            schema = {}  # {'table': [col.lower, ..., ]} * -> __all__
            column_names_original = db["column_names_original"]
            table_names_original = db["table_names_original"]
            tables[db_id] = {
                "column_names_original": column_names_original,
                "table_names_original": table_names_original,
            }
            for i, tabn in enumerate(table_names_original):
                table = str(tabn.lower())
                cols = [
                    str(col.lower()) for td, col in column_names_original if td == i
                ]
                schema[table] = cols
            schemas[db_id] = schema

        _schemas_cache = schemas, db_names, tables
        return _schemas_cache

    schemas, db_names, tables = get_schemas_from_json(tables_file)
    schema = Schema(schemas[db_id], tables[db_id])
    return schema


def sanitize(query):
    query = query.replace('"', "'")
    if query.endswith(";"):
        query = query[:-1]
    for i in [1, 2, 3, 4, 5]:
        query = query.replace(f"t{i}", f"T{i}")
    for agg in ["count", "min", "max", "sum", "avg"]:
        query = query.replace(f"{agg} (", f"{agg}(")
    for agg in ["COUNT", "MIN", "MAX", "SUM", "AVG"]:
        query = query.replace(f"{agg} (", f"{agg}(")
    return query

def disambiguate_items2(
    db_id: str, query_toks: List[str], tables_file: str, allow_aliases: bool
) -> List[str]:
    schema = get_schema_from_db_id(db_id, tables_file)
    # lower all the tokens
    for i in range(len(query_toks)):
        query_toks[i] = query_toks[i].lower()

    # add table name
    for i in range(len(query_toks)):
        for key in schema.schema.keys():
            if query_toks[i] in schema.schema[key]:
                query_toks[i] = key + '.' + query_toks[i]

    return ' '.join(query_toks)


def disambiguate_items(
    db_id: str, query_toks: List[str], tables_file: str, allow_aliases: bool
) -> List[str]:
    """
    we want the query tokens to be non-ambiguous - so we can change each column name to explicitly
    tell which table it belongs to

    parsed sql to sql clause is based on supermodel.gensql from syntaxsql
    """

    schema = get_schema_from_db_id(db_id, tables_file)
    fixed_toks = []
    i = 0
    while i < len(query_toks):
        tok = query_toks[i]
        if tok == "value" or tok == "'value'":
            # TODO: value should alawys be between '/" (remove first if clause)
            new_tok = f'"{tok}"'
        elif tok in ["!", "<", ">"] and query_toks[i + 1] == "=":
            new_tok = tok + "="
            i += 1
        elif i + 1 < len(query_toks) and query_toks[i + 1] == ".":
            new_tok = "".join(query_toks[i : i + 3])
            i += 2
        else:
            new_tok = tok
        fixed_toks.append(new_tok)
        i += 1

    toks = fixed_toks

    tables_with_alias = get_tables_with_alias(schema.schema, toks)

    _, sql, mapped_entities = parse_sql(
        toks, 0, tables_with_alias, schema, mapped_entities_fn=lambda: []
    )

    for i, new_name in mapped_entities:
        curr_tok = toks[i]
        if "." in curr_tok and allow_aliases:
            parts = curr_tok.split(".")
            assert len(parts) == 2
            toks[i] = parts[0] + "." + new_name
        else:
            toks[i] = new_name.replace("@", ".")

    if not allow_aliases:
        toks = [tok for tok in toks if tok not in ["as", "t1", "t2", "t3", "t4"]]

    toks = [f"'value'" if tok == '"value"' else tok for tok in toks]
    query_tokens = " ".join(toks)
    # query_tokens = query_tokens
    for agg in ["count", "min", "max", "sum", "avg"]:
        query_tokens = query_tokens.replace(f"{agg} (", f"{agg}(")
    return query_tokens


################################
# Assumptions:
#   1. sql is correct
#   2. only table name has alias
#   3. only one intersect/union/except
#
# val: number(float)/string(str)/sql(dict)
# col_unit: (agg_id, col_id, isDistinct(bool))
# val_unit: (unit_op, col_unit1, col_unit2)
# table_unit: (table_type, col_unit/sql)
# cond_unit: (not_op, op_id, val_unit, val1, val2)
# condition: [cond_unit1, 'and'/'or', cond_unit2, ...]
# sql {
#   'select': (isDistinct(bool), [(agg_id, val_unit), (agg_id, val_unit), ...])
#   'from': {'table_units': [table_unit1, table_unit2, ...], 'conds': condition}
#   'where': condition
#   'groupBy': [col_unit1, col_unit2, ...]
#   'orderBy': ('asc'/'desc', [val_unit1, val_unit2, ...])
#   'having': condition
#   'limit': None/limit value
#   'intersect': None/sql
#   'except': None/sql
#   'union': None/sql
# }
################################


CLAUSE_KEYWORDS = (
    "select",
    "from",
    "where",
    "group",
    "order",
    "limit",
    "intersect",
    "union",
    "except",
)
JOIN_KEYWORDS = ("join", "on", "as")

WHERE_OPS = (
    "not",
    "between",
    "=",
    ">",
    "<",
    ">=",
    "<=",
    "!=",
    "in",
    "like",
    "is",
    "exists",
)
UNIT_OPS = ("none", "-", "+", "*", "/")
AGG_OPS = ("none", "max", "min", "count", "sum", "avg")
TABLE_TYPE = {
    "sql": "sql",
    "table_unit": "table_unit",
}

COND_OPS = ("and", "or")
SQL_OPS = ("intersect", "union", "except")
ORDER_OPS = ("desc", "asc")
mapped_entities = []


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """

    def __init__(self, schema):
        self._schema = schema
        self._idMap = self._map(self._schema)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema):
        idMap = {"*": "__all__"}
        id = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = (
                    "__" + key.lower() + "." + val.lower() + "__"
                )
                id += 1

        for key in schema:
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1

        return idMap


def get_schema(db):
    """
    Get database's schema, which is a dict with table name as key
    and list of column names as value
    :param db: database path
    :return: schema dict
    """

    schema = {}
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # fetch table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [str(table[0].lower()) for table in cursor.fetchall()]

    # fetch table info
    for table in tables:
        cursor.execute("PRAGMA table_info({})".format(table))
        schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

    return schema


def get_schema_from_json(fpath):
    with open(fpath) as f:
        data = json.load(f)

    schema = {}
    for entry in data:
        table = str(entry["table"].lower())
        cols = [str(col["column_name"].lower()) for col in entry["col_data"]]
        schema[table] = cols

    return schema


def tokenize(string):
    string = str(string)
    string = string.replace(
        "'", '"'
    )  # ensures all string values wrapped by "" problem??
    quote_idxs = [idx for idx, char in enumerate(string) if char == '"']
    assert len(quote_idxs) % 2 == 0, "Unexpected quote"

    # keep string value as token
    vals = {}
    for i in range(len(quote_idxs) - 1, -1, -2):
        qidx1 = quote_idxs[i - 1]
        qidx2 = quote_idxs[i]
        val = string[qidx1 : qidx2 + 1]
        key = "__val_{}_{}__".format(qidx1, qidx2)
        string = string[:qidx1] + key + string[qidx2 + 1 :]
        vals[key] = val

    toks = [word.lower() for word in word_tokenize(string)]
    # replace with string value token
    for i in range(len(toks)):
        if toks[i] in vals:
            toks[i] = vals[toks[i]]

    # find if there exists !=, >=, <=
    eq_idxs = [idx for idx, tok in enumerate(toks) if tok == "="]
    eq_idxs.reverse()
    prefix = ("!", ">", "<")
    for eq_idx in eq_idxs:
        pre_tok = toks[eq_idx - 1]
        if pre_tok in prefix:
            toks = toks[: eq_idx - 1] + [pre_tok + "="] + toks[eq_idx + 1 :]

    return toks


def scan_alias(toks):
    """Scan the index of 'as' and build the map for all alias"""
    as_idxs = [idx for idx, tok in enumerate(toks) if tok == "as"]
    alias = {}
    for idx in as_idxs:
        alias[toks[idx + 1]] = toks[idx - 1]
    return alias


def get_tables_with_alias(schema, toks):
    tables = scan_alias(toks)
    for key in schema:
        assert key not in tables, "Alias {} has the same name in table".format(key)
        tables[key] = key
    return tables


def parse_col(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    :returns next idx, column id
    """
    global mapped_entities
    tok = toks[start_idx]
    if tok == "*":
        return start_idx + 1, schema.idMap[tok]

    if "." in tok:  # if token is a composite
        alias, col = tok.split(".")
        key = tables_with_alias[alias] + "." + col
        mapped_entities.append((start_idx, tables_with_alias[alias] + "@" + col))
        return start_idx + 1, schema.idMap[key]

    assert (
        default_tables is not None and len(default_tables) > 0
    ), "Default tables should not be None or empty"

    for alias in default_tables:
        table = tables_with_alias[alias]
        if tok in schema.schema[table]:
            key = table + "." + tok
            mapped_entities.append((start_idx, table + "@" + tok))
            return start_idx + 1, schema.idMap[key]

    assert False, "Error col: {}".format(tok)


def parse_col_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    :returns next idx, (agg_op id, col_id)
    """
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    isDistinct = False
    if toks[idx] == "(":
        isBlock = True
        idx += 1

    if toks[idx] in AGG_OPS:
        agg_id = AGG_OPS.index(toks[idx])
        idx += 1
        assert idx < len_ and toks[idx] == "("
        idx += 1
        if toks[idx] == "distinct":
            idx += 1
            isDistinct = True
        idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables)
        assert idx < len_ and toks[idx] == ")"
        idx += 1
        return idx, (agg_id, col_id, isDistinct)

    if toks[idx] == "distinct":
        idx += 1
        isDistinct = True
    agg_id = AGG_OPS.index("none")
    idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables)

    if isBlock:
        assert toks[idx] == ")"
        idx += 1  # skip ')'

    return idx, (agg_id, col_id, isDistinct)


def parse_val_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    if toks[idx] == "(":
        isBlock = True
        idx += 1

    col_unit1 = None
    col_unit2 = None
    unit_op = UNIT_OPS.index("none")

    idx, col_unit1 = parse_col_unit(
        toks, idx, tables_with_alias, schema, default_tables
    )
    if idx < len_ and toks[idx] in UNIT_OPS:
        unit_op = UNIT_OPS.index(toks[idx])
        idx += 1
        idx, col_unit2 = parse_col_unit(
            toks, idx, tables_with_alias, schema, default_tables
        )

    if isBlock:
        assert toks[idx] == ")"
        idx += 1  # skip ')'

    return idx, (unit_op, col_unit1, col_unit2)


def parse_table_unit(toks, start_idx, tables_with_alias, schema):
    """
    :returns next idx, table id, table name
    """
    idx = start_idx
    len_ = len(toks)
    key = tables_with_alias[toks[idx]]

    if idx + 1 < len_ and toks[idx + 1] == "as":
        idx += 3
    else:
        idx += 1

    return idx, schema.idMap[key], key


def parse_value(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)

    isBlock = False
    if toks[idx] == "(":
        isBlock = True
        idx += 1

    if toks[idx] == "select":
        idx, val = parse_sql(toks, idx, tables_with_alias, schema)
    elif '"' in toks[idx]:  # token is a string value
        val = toks[idx]
        idx += 1
    else:
        try:
            val = float(toks[idx])
            idx += 1
        except:
            end_idx = idx
            while (
                end_idx < len_
                and toks[end_idx] != ","
                and toks[end_idx] != ")"
                and toks[end_idx] != "and"
                and toks[end_idx] not in CLAUSE_KEYWORDS
                and toks[end_idx] not in JOIN_KEYWORDS
            ):
                end_idx += 1

            idx, val = parse_col_unit(
                toks[:end_idx], start_idx, tables_with_alias, schema, default_tables
            )
            idx = end_idx

    if isBlock:
        assert toks[idx] == ")"
        idx += 1

    return idx, val


def parse_condition(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)
    conds = []

    while idx < len_:
        idx, val_unit = parse_val_unit(
            toks, idx, tables_with_alias, schema, default_tables
        )
        not_op = False
        if toks[idx] == "not":
            not_op = True
            idx += 1

        assert (
            idx < len_ and toks[idx] in WHERE_OPS
        ), "Error condition: idx: {}, tok: {}".format(idx, toks[idx])
        op_id = WHERE_OPS.index(toks[idx])
        idx += 1
        val1 = val2 = None
        if op_id == WHERE_OPS.index(
            "between"
        ):  # between..and... special case: dual values
            idx, val1 = parse_value(
                toks, idx, tables_with_alias, schema, default_tables
            )
            assert toks[idx] == "and"
            idx += 1
            idx, val2 = parse_value(
                toks, idx, tables_with_alias, schema, default_tables
            )
        else:  # normal case: single value
            idx, val1 = parse_value(
                toks, idx, tables_with_alias, schema, default_tables
            )
            val2 = None

        conds.append((not_op, op_id, val_unit, val1, val2))

        if idx < len_ and (
            toks[idx] in CLAUSE_KEYWORDS
            or toks[idx] in (")", ";")
            or toks[idx] in JOIN_KEYWORDS
        ):
            break

        if idx < len_ and toks[idx] in COND_OPS:
            conds.append(toks[idx])
            idx += 1  # skip and/or

    return idx, conds


def parse_select(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)

    assert toks[idx] == "select", "'select' not found"
    idx += 1
    isDistinct = False
    if idx < len_ and toks[idx] == "distinct":
        idx += 1
        isDistinct = True
    val_units = []

    while idx < len_ and toks[idx] not in CLAUSE_KEYWORDS:
        agg_id = AGG_OPS.index("none")
        if toks[idx] in AGG_OPS:
            agg_id = AGG_OPS.index(toks[idx])
            idx += 1
        idx, val_unit = parse_val_unit(
            toks, idx, tables_with_alias, schema, default_tables
        )
        val_units.append((agg_id, val_unit))
        if idx < len_ and toks[idx] == ",":
            idx += 1  # skip ','

    return idx, (isDistinct, val_units)


def parse_from(toks, start_idx, tables_with_alias, schema):
    """
    Assume in the from clause, all table units are combined with join
    """
    assert "from" in toks[start_idx:], "'from' not found"

    len_ = len(toks)
    idx = toks.index("from", start_idx) + 1
    default_tables = []
    table_units = []
    conds = []

    while idx < len_:
        isBlock = False
        if toks[idx] == "(":
            isBlock = True
            idx += 1

        if toks[idx] == "select":
            idx, sql = parse_sql(toks, idx, tables_with_alias, schema)
            table_units.append((TABLE_TYPE["sql"], sql))
        else:
            if idx < len_ and toks[idx] == "join":
                idx += 1  # skip join
            idx, table_unit, table_name = parse_table_unit(
                toks, idx, tables_with_alias, schema
            )
            table_units.append((TABLE_TYPE["table_unit"], table_unit))
            default_tables.append(table_name)
        if idx < len_ and toks[idx] == "on":
            idx += 1  # skip on
            idx, this_conds = parse_condition(
                toks, idx, tables_with_alias, schema, default_tables
            )
            if len(conds) > 0:
                conds.append("and")
            conds.extend(this_conds)

        if isBlock:
            assert toks[idx] == ")"
            idx += 1
        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
            break

    return idx, table_units, conds, default_tables


def parse_where(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != "where":
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_group_by(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)
    col_units = []

    if idx >= len_ or toks[idx] != "group":
        return idx, col_units

    idx += 1
    assert toks[idx] == "by"
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, col_unit = parse_col_unit(
            toks, idx, tables_with_alias, schema, default_tables
        )
        col_units.append(col_unit)
        if idx < len_ and toks[idx] == ",":
            idx += 1  # skip ','
        else:
            break

    return idx, col_units


def parse_order_by(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)
    val_units = []
    order_type = "asc"  # default type is 'asc'

    if idx >= len_ or toks[idx] != "order":
        return idx, val_units

    idx += 1
    assert toks[idx] == "by"
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, val_unit = parse_val_unit(
            toks, idx, tables_with_alias, schema, default_tables
        )
        val_units.append(val_unit)
        if idx < len_ and toks[idx] in ORDER_OPS:
            order_type = toks[idx]
            idx += 1
        if idx < len_ and toks[idx] == ",":
            idx += 1  # skip ','
        else:
            break

    return idx, (order_type, val_units)


def parse_having(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != "having":
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_limit(toks, start_idx):
    idx = start_idx
    len_ = len(toks)

    if idx < len_ and toks[idx] == "limit":
        idx += 2
        try:
            limit_val = int(toks[idx - 1])
        except Exception:
            limit_val = '"value"'
        return idx, limit_val

    return idx, None


def parse_sql(toks, start_idx, tables_with_alias, schema, mapped_entities_fn=None):
    global mapped_entities

    if mapped_entities_fn is not None:
        mapped_entities = mapped_entities_fn()
    isBlock = False  # indicate whether this is a block of sql/sub-sql
    len_ = len(toks)
    idx = start_idx

    sql = {}
    if toks[idx] == "(":
        isBlock = True
        idx += 1

    # parse from clause in order to get default tables
    from_end_idx, table_units, conds, default_tables = parse_from(
        toks, start_idx, tables_with_alias, schema
    )
    sql["from"] = {"table_units": table_units, "conds": conds}
    # select clause
    _, select_col_units = parse_select(
        toks, idx, tables_with_alias, schema, default_tables
    )
    idx = from_end_idx
    sql["select"] = select_col_units
    # where clause
    idx, where_conds = parse_where(toks, idx, tables_with_alias, schema, default_tables)
    sql["where"] = where_conds
    # group by clause
    idx, group_col_units = parse_group_by(
        toks, idx, tables_with_alias, schema, default_tables
    )
    sql["groupBy"] = group_col_units
    # having clause
    idx, having_conds = parse_having(
        toks, idx, tables_with_alias, schema, default_tables
    )
    sql["having"] = having_conds
    # order by clause
    idx, order_col_units = parse_order_by(
        toks, idx, tables_with_alias, schema, default_tables
    )
    sql["orderBy"] = order_col_units
    # limit clause
    idx, limit_val = parse_limit(toks, idx)
    sql["limit"] = limit_val

    idx = skip_semicolon(toks, idx)
    if isBlock:
        assert toks[idx] == ")"
        idx += 1  # skip ')'
    idx = skip_semicolon(toks, idx)

    # intersect/union/except clause
    for op in SQL_OPS:  # initialize IUE
        sql[op] = None
    if idx < len_ and toks[idx] in SQL_OPS:
        sql_op = toks[idx]
        idx += 1
        idx, IUE_sql = parse_sql(toks, idx, tables_with_alias, schema)
        sql[sql_op] = IUE_sql

    if mapped_entities_fn is not None:
        return idx, sql, mapped_entities
    else:
        return idx, sql


def load_data(fpath):
    with open(fpath) as f:
        data = json.load(f)
    return data


def get_sql(schema, query):
    toks = tokenize(query)
    tables_with_alias = get_tables_with_alias(schema.schema, toks)
    _, sql = parse_sql(toks, 0, tables_with_alias, schema)

    return sql


def skip_semicolon(toks, start_idx):
    idx = start_idx
    while idx < len(toks) and toks[idx] == ";":
        idx += 1
    return idx
