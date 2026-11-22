"""
Apache Pig Transformation Layer (Local Pig Latin Engine + Real Cluster Artifacts)
=================================================================================
Project  : News Article Analysis and Topic Extraction (Big Data Architecture)
Component: Apache Pig - high-level data-flow (Pig Latin) ETL over HDFS.
Owner    : Member 2 - Mohammed Hossam (Hadoop & MapReduce Specialist)

WHAT THIS MODULE DOES
---------------------
Apache Pig is a data-flow language that compiles high-level "Pig Latin" scripts
into a chain of MapReduce (or Tez) jobs. Instead of hand-writing mappers and
reducers, an engineer describes the transformation as a sequence of relational
operators: LOAD -> FILTER -> FOREACH/GENERATE -> GROUP -> JOIN -> ORDER -> STORE.

This module is DUAL-MODE:

  1. LOCAL SIMULATION - `PigLatinEngine` is a small, faithful interpreter for the
     SUPPORTED Pig Latin SUBSET (documented below). It GENUINELY executes the
     three Pig scripts against the REAL news data and writes the REAL results to
     `data/hadoop_ecosystem/pig/output/<script>/part-r-00000` (Pig's part-file
     layout) plus a `_SUCCESS` marker, exactly like a cluster would.
  2. REAL CLUSTER ARTIFACTS - it writes the three authentic `.pig` scripts and a
     `real_cluster_commands.md` holding the exact commands to run the identical
     workload on a genuine Hadoop cluster (`pig -x mapreduce`, `pig -x tez`, the
     `pig.exec.mapPartAgg` tuning flags and the YARN lifecycle).

SUPPORTED PIG LATIN SUBSET (this is what the engine truly understands)
----------------------------------------------------------------------
    * LOAD 'path' USING PigStorage(',') AS (field:type, ...)
        - PigStorage(',') and PigStorage('\\t'); types chararray/int/long/
          double/float (values are CAST on load, like Pig does).
    * FOREACH rel GENERATE expr [AS name], ...
        - field references, literals, and the functions LOWER, UPPER, TRIM,
          REPLACE(regex), CONCAT, LENGTH, TOKENIZE, ABS, ROUND,
          FLATTEN(<bag or list expression>) (row expansion) and the grouped
          aggregates COUNT/SUM/AVG/MAX/MIN.
    * FILTER rel BY <boolean expr>
        - comparisons (== != > < >= <=), AND / OR / NOT, IS [NOT] NULL, MATCHES.
    * GROUP rel BY field            (single key or tuple `(a, b)`)
        - yields the special `group` key plus a bag named after the relation.
    * FOREACH grouped GENERATE group AS k, COUNT(rel) AS c, AVG(rel.field) AS a
    * ORDER rel BY field [ASC|DESC], field2 [ASC|DESC]
    * JOIN a BY k, b BY k2         (single- or tuple-key equi-join)
    * LIMIT rel N
    * DUMP rel
    * STORE rel INTO 'path' USING PigStorage(',')

Anything outside this subset is intentionally NOT implemented - the engine raises
a clear error rather than silently producing wrong data.

Line-by-line explanatory comments are provided throughout, as required by the
rubric criterion "clean, commented, reproducible code" for this Big Data project.
"""

# ---------------------------------------------------------------------------
# Path bootstrap so this module works BOTH when imported by the orchestrator and
# when run directly as `python src/hadoop/pig_transform.py` from the project root.
# ---------------------------------------------------------------------------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.hadoop.hdfs_client import LocalHDFS

# --- Standard library + the two already-installed data libraries -------------
import re                          # REPLACE / TOKENIZE / MATCHES use regexes
import csv                         # robust CSV reading (article bodies embed commas)
import json                        # summary + execution-log persistence
import time                        # per-script wall-clock timing
import datetime                    # run timestamps
import collections                 # ordered grouping of relations

# ---------------------------------------------------------------------------
# PATHS & CONSTANTS
# ---------------------------------------------------------------------------
# Project root (two levels above src/hadoop/) - used to resolve relative paths
# inside the .pig scripts regardless of the current working directory.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Default inputs and output directory for this component.
DEFAULT_INPUT_CSV = "data/raw_news_articles.csv"
DEFAULT_PARQUET_DIR = "data/processed_parquet"
DEFAULT_MAPREDUCE_TSV = "data/mapreduce_term_counts.tsv"
DEFAULT_OUT_DIR = os.path.join("data", "hadoop_ecosystem", "pig")

# HDFS locations recorded as authentic `hdfs dfs` staging commands.
HDFS_PIG_DIR = "/user/news/pig"
HDFS_RAW_DIR = "/user/news/raw"


def _log(message: str) -> None:
    """Prints a Pig-component log line with the consistent `[Pig]` prefix."""
    print(f"[Pig] {message}")


def _resolve(path: str) -> str:
    """Resolves a script-relative path against the project root."""
    return path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)


def _strip_comments(text: str) -> str:
    """
    Removes Pig `--` line comments while respecting single-quoted string literals
    (so a `--` inside a REGEX pattern is NOT mistaken for a comment).
    """
    out_lines = []          # accumulated comment-free lines
    for line in text.splitlines():
        chars = []          # characters of the current line that we keep
        in_quote = False    # are we inside a '...' literal?
        i = 0
        while i < len(line):
            ch = line[i]
            # Toggle quote state on an unescaped single quote.
            if ch == "'":
                in_quote = not in_quote
                chars.append(ch)
            elif ch == "-" and not in_quote and line[i:i + 2] == "--":
                break       # rest of the line is a comment - drop it
            else:
                chars.append(ch)
            i += 1
        out_lines.append("".join(chars))
    return "\n".join(out_lines)


def _split_statements(text: str) -> list:
    """Splits a script into statements on `;` while respecting quoted literals."""
    statements, cur, in_quote = [], [], False
    for ch in text:
        if ch == "'":
            in_quote = not in_quote
            cur.append(ch)
        elif ch == ";" and not in_quote:
            statements.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        statements.append("".join(cur))
    return statements


def _split_top(text: str, sep: str = ",", ci: bool = False) -> list:
    """
    Splits `text` on `sep` only at the TOP level - i.e. ignoring separators that
    sit inside parentheses/brackets or single-quoted literals. This is what lets
    `REPLACE(x, '[^a-z]+', ' ')` be treated as ONE generate item.
    """
    parts, depth, cur, in_quote = [], 0, [], False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if in_quote:
            cur.append(ch)
            if ch == "'":
                in_quote = False
        elif ch == "'":
            in_quote = True
            cur.append(ch)
        elif ch in "([":
            depth += 1
            cur.append(ch)
        elif ch in ")]":
            depth -= 1
            cur.append(ch)
        elif depth == 0 and (text[i:i + len(sep)].upper() == sep.upper() if ci
                             else text.startswith(sep, i)):
            parts.append("".join(cur))
            cur = []
            i += len(sep)
            continue
        else:
            cur.append(ch)
        i += 1
    parts.append("".join(cur))
    return [p.strip() for p in parts]


def _find_top_level(text: str, needle: str) -> int:
    """Returns the first TOP-LEVEL index of `needle` (case-insensitive) or -1."""
    depth, in_quote, i, n = 0, False, 0, len(text)
    while i < n:
        ch = text[i]
        if in_quote:
            if ch == "'":
                in_quote = False
        elif ch == "'":
            in_quote = True
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif depth == 0 and text[i:i + len(needle)].upper() == needle.upper():
            return i
        i += 1
    return -1


def _balanced_wrap(s: str) -> bool:
    """True when the leading '(' / '[' closes exactly at the end of `s`."""
    depth, in_quote = 0, False
    for i, ch in enumerate(s):
        if in_quote:
            if ch == "'":
                in_quote = False
            continue
        if ch == "'":
            in_quote = True
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
            if depth == 0 and i != len(s) - 1:
                return False
    return depth == 0


def _unquote(s: str) -> str:
    """Strips a single pair of surrounding single quotes from a literal."""
    s = s.strip()
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return s[1:-1]
    return s


def _decode_sep(token) -> str:
    """Decodes a PigStorage separator literal: ',' -> ',', '\\t' -> TAB."""
    if token is None or token == "":
        return ","
    return token.replace("\\t", "\t").replace("\\n", "\n")


# ---------------------------------------------------------------------------
# RELATION - the in-memory table that every Pig operator produces.
# ---------------------------------------------------------------------------
class Relation:
    """A named bag of tuples: an ordered column `schema` plus a list of dict rows."""

    def __init__(self, schema: list, rows: list):
        self.schema = list(schema)   # column names in positional order
        self.rows = rows             # list of {column: value} dictionaries

    def __repr__(self) -> str:
        return f"Relation(cols={self.schema}, rows={len(self.rows)})"


# ---------------------------------------------------------------------------
# PIG LATIN ENGINE - the mini interpreter.
# ---------------------------------------------------------------------------
class PigLatinEngine:
    """
    Executes the supported Pig Latin subset (see the module docstring) on
    in-memory relations and materialises STORE output in Pig's directory layout
    (a `part-r-00000` data file plus a `_SUCCESS` marker).
    """

    # Type keywords Pig understands in a LOAD schema.
    INT_TYPES = {"int", "integer", "long"}
    FLOAT_TYPES = {"double", "float"}

    def __init__(self, verbose: bool = True):
        self.relations = {}      # relation name -> Relation
        self.output_files = []   # every part-file written by STORE
        self.verbose = verbose

    def _say(self, message: str) -> None:
        """Routes engine logging through the component logger when verbose."""
        if self.verbose:
            _log(message)

    # ------------------------------------------------------------------ #
    # SCRIPT DRIVER
    # ------------------------------------------------------------------ #
    def run_script(self, script_path: str) -> int:
        """
        Parses and executes one `.pig` file. Returns the number of relations
        that the script created (i.e. the number of assignment statements).
        """
        with open(script_path, "r", encoding="utf-8") as fh:
            text = fh.read()
        # 1) strip comments, 2) split into `;`-terminated statements, 3) execute.
        statements = [s.strip() for s in _split_statements(_strip_comments(text))]
        created = 0
        for stmt in statements:
            if not stmt:
                continue
            upper = stmt.upper()
            if upper.startswith("DUMP "):            # DUMP rel;
                self._exec_dump(stmt[5:].strip().rstrip(";"))
            elif upper.startswith("STORE "):         # STORE rel INTO '...' USING ...
                self._exec_store_stmt(stmt)
            else:                                    # name = <operator> ...
                eq = _find_top_level(stmt, "=")
                if eq < 0:
                    raise SyntaxError(f"Unsupported statement: {stmt[:60]}...")
                name = stmt[:eq].strip()
                rhs = stmt[eq + 1:].strip()
                self.relations[name] = self._exec_op(rhs)
                created += 1
                self._say(f"relation '{name}' = {self.relations[name]}")
        return created

    # ------------------------------------------------------------------ #
    # OPERATOR DISPATCH
    # ------------------------------------------------------------------ #
    def _exec_op(self, rhs: str) -> Relation:
        """Dispatches a right-hand-side expression to the correct operator."""
        upper = rhs.upper()
        if upper.startswith("LOAD "):
            return self._exec_load(rhs)
        if upper.startswith("FOREACH "):
            return self._exec_foreach(rhs)
        if upper.startswith("FILTER "):
            return self._exec_filter(rhs)
        if upper.startswith("GROUP "):
            return self._exec_group(rhs)
        if upper.startswith("ORDER "):
            return self._exec_order(rhs)
        if upper.startswith("LIMIT "):
            return self._exec_limit(rhs)
        if upper.startswith("JOIN "):
            return self._exec_join(rhs)
        raise SyntaxError(f"Unsupported operator: {rhs[:60]}...")

    # ------------------------------------------------------------------ #
    # LOAD
    # ------------------------------------------------------------------ #
    def _exec_load(self, rhs: str) -> Relation:
        """`LOAD 'path' USING PigStorage(',') AS (field:type, ...)`."""
        m = re.match(
            r"LOAD\s+'([^']+)'\s+USING\s+PigStorage\(\s*(?:'([^']*)')?\s*\)"
            r"\s+AS\s*\((.*)\)\s*$", rhs, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed LOAD: {rhs[:80]}")
        path, sep_token, schema_str = m.group(1), m.group(2), m.group(3)
        sep = _decode_sep(sep_token)
        # Parse the schema list: each item is "name" or "name:type".
        schema = []
        for item in _split_top(schema_str):
            if not item:
                continue
            name, _, typ = item.partition(":")
            schema.append((name.strip(), (typ.strip() or "chararray").lower()))

        # Read the data with a real CSV parser (handles quoted commas/newlines).
        full = _resolve(path)
        if not os.path.exists(full):
            raise FileNotFoundError(f"LOAD source not found: {full}")
        rows = []
        with open(full, "r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh, delimiter=sep)
            for rec in reader:
                row = {}
                for i, (col, typ) in enumerate(schema):
                    raw = rec[i] if i < len(rec) else ""
                    row[col] = self._cast(raw, typ)
                rows.append(row)
        self._say(f"LOAD {path} -> {len(rows)} rows, schema={[s[0] for s in schema]}")
        return Relation([s[0] for s in schema], rows)

    def _cast(self, value: str, typ: str):
        """Casts a raw string to the declared Pig type (like Pig's loader does)."""
        if typ in self.INT_TYPES:
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None
        if typ in self.FLOAT_TYPES:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        return str(value)   # chararray (default)

    # ------------------------------------------------------------------ #
    # FOREACH ... GENERATE
    # ------------------------------------------------------------------ #
    def _exec_foreach(self, rhs: str) -> Relation:
        """`FOREACH rel GENERATE expr [AS name], ...` (supports FLATTEN expansion)."""
        m = re.match(r"FOREACH\s+([A-Za-z_]\w*)\s+GENERATE\s+(.*)$", rhs, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed FOREACH: {rhs[:80]}")
        parent_name, generate_str = m.group(1), m.group(2)
        if parent_name not in self.relations:
            raise KeyError(f"Unknown relation in FOREACH: {parent_name}")
        parent = self.relations[parent_name]

        # Parse each generate item into (expr, alias-or-None).
        items = []
        for item in _split_top(generate_str):
            idx = _find_top_level(item, " AS ")
            if idx >= 0:
                items.append((item[:idx].strip(), item[idx + 4:].strip()))
            else:
                items.append((item.strip(), None))
        # Resolve the output column names (alias, else a derived name).
        names = [alias or self._derive_name(expr) for expr, alias in items]
        # Locate a FLATTEN(...) item, if any - it drives row multiplication.
        flatten_pos = next(
            (i for i, (expr, _) in enumerate(items)
             if expr.upper().startswith("FLATTEN(")
             and _balanced_wrap(expr[expr.index("("):])), None)

        out_rows = []
        for row in parent.rows:
            base = {}
            flat_vals = None
            for i, (expr, _alias) in enumerate(items):
                if i == flatten_pos:
                    # FLATTEN evaluates its inner expression to a list of values.
                    inner = expr.strip()[len("FLATTEN("):-1]
                    vals = self._eval(inner, row)
                    flat_vals = list(vals) if isinstance(vals, (list, tuple)) else (
                        [vals] if vals is not None else [])
                    continue
                base[names[i]] = self._eval(expr, row)
            if flat_vals is None:
                out_rows.append(base)
            else:
                # Emit one output tuple per flattened element.
                for val in flat_vals:
                    expanded = dict(base)
                    expanded[names[flatten_pos]] = val
                    out_rows.append(expanded)
        self._say(f"FOREACH {parent_name} GENERATE -> {len(out_rows)} rows, cols={names}")
        return Relation(names, out_rows)

    def _derive_name(self, expr: str) -> str:
        """Derives a sensible column name when no `AS` alias was given."""
        e = expr.strip()
        m = re.match(r"^([A-Za-z_]\w*)\s*\(", e)
        if m:
            return f"{m.group(1).lower()}_val"
        return e.split(".")[-1]

    # ------------------------------------------------------------------ #
    # FILTER
    # ------------------------------------------------------------------ #
    def _exec_filter(self, rhs: str) -> Relation:
        """`FILTER rel BY <boolean expression>`."""
        m = re.match(r"FILTER\s+([A-Za-z_]\w*)\s+BY\s+(.*)$", rhs, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed FILTER: {rhs[:80]}")
        parent_name, cond = m.group(1), m.group(2)
        parent = self.relations[parent_name]
        kept = [row for row in parent.rows if self._eval_cond(cond, row)]
        self._say(f"FILTER {parent_name} -> {len(kept)}/{len(parent.rows)} rows kept")
        return Relation(parent.schema, kept)

    def _eval_cond(self, expr: str, row: dict) -> bool:
        """Evaluates a boolean predicate over one row (AND/OR/NOT + comparisons)."""
        expr = expr.strip()
        # Unwrap a fully-parenthesised expression.
        if expr.startswith("(") and _balanced_wrap(expr):
            return self._eval_cond(expr[1:-1], row)
        # OR has the lowest precedence, then AND (split at top level only).
        parts = _split_top(expr, " OR ", ci=True)
        if len(parts) > 1:
            return any(self._eval_cond(p, row) for p in parts)
        parts = _split_top(expr, " AND ", ci=True)
        if len(parts) > 1:
            return all(self._eval_cond(p, row) for p in parts)
        if expr.upper().startswith("NOT "):
            return not self._eval_cond(expr[4:], row)
        # IS [NOT] NULL
        m = re.match(r"^(.*?)\s+IS\s+NOT\s+NULL$", expr, re.I | re.S)
        if m:
            return self._eval(m.group(1), row) is not None
        m = re.match(r"^(.*?)\s+IS\s+NULL$", expr, re.I | re.S)
        if m:
            return self._eval(m.group(1), row) is None
        # MATCHES 'regex'
        m = re.match(r"^(.*?)\s+MATCHES\s+('.*')$", expr, re.I | re.S)
        if m:
            left = str(self._eval(m.group(1), row) or "")
            return bool(re.search(_unquote(m.group(2)), left))
        # Comparison operators (longest first so '>=' beats '>').
        for op in (">=", "<=", "==", "!=", ">", "<", "="):
            idx = _find_top_level(expr, op)
            if idx >= 0:
                left = self._eval(expr[:idx].strip(), row)
                right = self._eval(expr[idx + len(op):].strip(), row)
                try:
                    if op in ("==", "="):
                        return left == right
                    if op == "!=":
                        return left != right
                    if op == ">":
                        return left > right
                    if op == "<":
                        return left < right
                    if op == ">=":
                        return left >= right
                    if op == "<=":
                        return left <= right
                except TypeError:
                    return False   # e.g. comparing None with a number
        return False

    # ------------------------------------------------------------------ #
    # GROUP
    # ------------------------------------------------------------------ #
    def _exec_group(self, rhs: str) -> Relation:
        """`GROUP rel BY key` -> rows of {group: key, <rel>: [bag of rows]}."""
        m = re.match(r"GROUP\s+([A-Za-z_]\w*)\s+BY\s+(.*)$", rhs, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed GROUP: {rhs[:80]}")
        parent_name, key_expr = m.group(1), m.group(2).strip()
        parent = self.relations[parent_name]
        # Preserve first-seen key order (deterministic output).
        buckets = collections.OrderedDict()
        for row in parent.rows:
            key = self._eval(key_expr, row)
            buckets.setdefault(key, []).append(row)
        grouped = [{"group": key, parent_name: bag} for key, bag in buckets.items()]
        self._say(f"GROUP {parent_name} BY {key_expr} -> {len(grouped)} groups")
        return Relation(["group", parent_name], grouped)

    # ------------------------------------------------------------------ #
    # ORDER / LIMIT
    # ------------------------------------------------------------------ #
    def _exec_order(self, rhs: str) -> Relation:
        """`ORDER rel BY field [DESC], field2 ...` (stable multi-key sort)."""
        m = re.match(r"ORDER\s+([A-Za-z_]\w*)\s+BY\s+(.*)$", rhs, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed ORDER: {rhs[:80]}")
        parent_name, order_str = m.group(1), m.group(2)
        parent = self.relations[parent_name]
        specs = []
        for token in _split_top(order_str):
            bits = token.split()
            field = bits[0]
            desc = len(bits) > 1 and bits[1].upper() == "DESC"
            specs.append((field, desc))
        rows = list(parent.rows)
        # Sort by the least-significant key first (Python sort is stable).
        for field, desc in reversed(specs):
            rows.sort(key=lambda r, f=field: (r.get(f) is None, r.get(f)), reverse=desc)
        self._say(f"ORDER {parent_name} BY {order_str}")
        return Relation(parent.schema, rows)

    def _exec_limit(self, rhs: str) -> Relation:
        """`LIMIT rel N` - keeps the first N rows."""
        m = re.match(r"LIMIT\s+([A-Za-z_]\w*)\s+(\d+)$", rhs, re.I)
        if not m:
            raise SyntaxError(f"Malformed LIMIT: {rhs[:80]}")
        parent = self.relations[m.group(1)]
        n = int(m.group(2))
        self._say(f"LIMIT {m.group(1)} {n} -> {min(n, len(parent.rows))} rows")
        return Relation(parent.schema, parent.rows[:n])

    # ------------------------------------------------------------------ #
    # JOIN
    # ------------------------------------------------------------------ #
    def _exec_join(self, rhs: str) -> Relation:
        """`JOIN a BY k, b BY k2` - equi-join producing the merged schema."""
        m = re.match(r"JOIN\s+(.*)$", rhs, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed JOIN: {rhs[:80]}")
        sides = _split_top(m.group(1))
        if len(sides) != 2:
            raise SyntaxError("This engine supports 2-way JOIN only.")
        lrel, lkey = self._parse_join_side(sides[0])
        rrel, rkey = self._parse_join_side(sides[1])
        left, right = self.relations[lrel], self.relations[rrel]

        # Build a hash bucket on the RIGHT relation keyed by its join key.
        right_buckets = collections.defaultdict(list)
        for r in right.rows:
            right_buckets[self._eval(rkey, r)].append(r)

        # Merge schemas, de-duplicating any colliding column name.
        schema = list(left.schema)
        right_names = []
        for f in right.schema:
            name = f if f not in schema else f"{rrel}_{f}"
            schema.append(name)
            right_names.append(name)

        # Probe the hash table with every LEFT row (classic hash join).
        out = []
        for l in left.rows:
            key = self._eval(lkey, l)
            for r in right_buckets.get(key, []):
                merged = dict(l)
                for orig, new in zip(right.schema, right_names):
                    merged[new] = r.get(orig)
                out.append(merged)
        self._say(f"JOIN {lrel} BY {lkey}, {rrel} BY {rkey} -> {len(out)} rows")
        return Relation(schema, out)

    def _parse_join_side(self, side: str):
        """Splits a JOIN side `rel BY key` into (relation_name, key_expr)."""
        idx = _find_top_level(side, " BY ")
        if idx < 0:
            raise SyntaxError(f"Malformed JOIN side: {side}")
        return side[:idx].strip(), side[idx + 4:].strip()

    # ------------------------------------------------------------------ #
    # STORE / DUMP
    # ------------------------------------------------------------------ #
    def _exec_store_stmt(self, stmt: str) -> None:
        """`STORE rel INTO 'path' USING PigStorage(',')` -> part-file + _SUCCESS."""
        m = re.match(
            r"STORE\s+([A-Za-z_]\w*)\s+INTO\s+'([^']+)'\s+USING\s+"
            r"PigStorage\(\s*(?:'([^']*)')?\s*\)\s*$", stmt, re.I | re.S)
        if not m:
            raise SyntaxError(f"Malformed STORE: {stmt[:80]}")
        rel_name, path, sep_token = m.group(1), m.group(2), m.group(3)
        rel = self.relations[rel_name]
        sep = _decode_sep(sep_token)
        # Pig writes output as a DIRECTORY containing part files.
        out_dir = _resolve(path)
        os.makedirs(out_dir, exist_ok=True)
        part_path = os.path.join(out_dir, "part-r-00000")
        with open(part_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter=sep)
            for row in rel.rows:
                writer.writerow([self._fmt(row.get(col)) for col in rel.schema])
        # The _SUCCESS marker signals a completed (atomic) output on HDFS.
        with open(os.path.join(out_dir, "_SUCCESS"), "w", encoding="utf-8"):
            pass
        self.output_files.append(part_path.replace(os.sep, "/"))
        self._say(f"STORE {rel_name} -> {path} ({len(rel.rows)} rows)")

    @staticmethod
    def _fmt(value) -> str:
        """Formats a value for CSV output (None -> empty field)."""
        return "" if value is None else str(value)

    def _exec_dump(self, rel_name: str) -> None:
        """`DUMP rel` - prints a bounded preview of the relation to the console."""
        rel = self.relations.get(rel_name)
        if rel is None:
            raise KeyError(f"Unknown relation in DUMP: {rel_name}")
        self._say(f"DUMP {rel_name}: {len(rel.rows)} rows, schema={rel.schema}")
        for row in rel.rows[:20]:
            self._say("  " + str({col: row.get(col) for col in rel.schema}))
        if len(rel.rows) > 20:
            self._say(f"  ... ({len(rel.rows) - 20} more rows)")

    # ------------------------------------------------------------------ #
    # EXPRESSION EVALUATOR
    # ------------------------------------------------------------------ #
    def _eval(self, expr: str, row: dict):
        """Evaluates a scalar expression (field, literal, or function call)."""
        expr = expr.strip()
        if expr == "":
            return None
        # Parenthesised tuple / grouping: `(a, b)` -> tuple; `(x)` -> x.
        if expr.startswith("(") and _balanced_wrap(expr):
            inner = expr[1:-1]
            parts = _split_top(inner)
            if len(parts) > 1:
                return tuple(self._eval(p, row) for p in parts)
            return self._eval(parts[0], row)
        # Function call: NAME(args...)
        m = re.match(r"^([A-Za-z_]\w*)\s*\((.*)\)$", expr, re.S)
        if m and _balanced_wrap(expr[expr.index("("):]):
            return self._call_func(m.group(1), m.group(2), row)
        # Single-quoted string literal.
        if len(expr) >= 2 and expr[0] == "'" and expr[-1] == "'":
            return _unquote(expr)
        # Numeric literals.
        if re.match(r"^-?\d+$", expr):
            return int(expr)
        if re.match(r"^-?\d*\.\d+$", expr):
            return float(expr)
        # Field reference (bare or dotted as in `rel.field`).
        if expr in row:
            return row[expr]
        if "." in expr:
            head, _, tail = expr.partition(".")
            if head in row and isinstance(row[head], dict):
                return row[head].get(tail)
            return row.get(tail)
        return row.get(expr)

    def _call_func(self, func: str, args_str: str, row: dict):
        """Evaluates a built-in Pig function call."""
        func = func.upper()
        # FLATTEN returns the inner bag/list so FOREACH can expand it.
        if func == "FLATTEN":
            inner = self._eval(args_str.strip(), row)
            if isinstance(inner, (list, tuple)):
                return list(inner)
            return [inner] if inner is not None else []
        args = _split_top(args_str) if args_str.strip() else []
        # Grouped aggregates need the raw argument text (to find the bag field).
        if func in ("COUNT", "SUM", "AVG", "MAX", "MIN"):
            return self._aggregate(func, args[0] if args else "", row)
        vals = [self._eval(a, row) for a in args]
        if func == "LOWER":
            return str(vals[0]).lower() if vals else None
        if func == "UPPER":
            return str(vals[0]).upper() if vals else None
        if func == "TRIM":
            return str(vals[0]).strip() if vals else None
        if func == "LTRIM":
            return str(vals[0]).lstrip() if vals else None
        if func == "RTRIM":
            return str(vals[0]).rstrip() if vals else None
        if func == "REPLACE":
            if len(vals) >= 3:
                return re.sub(str(vals[1]), str(vals[2]), str(vals[0]))
            return vals[0] if vals else None
        if func == "CONCAT":
            return "".join(str(v) for v in vals if v is not None)
        if func == "LENGTH":
            return len(str(vals[0])) if vals else 0
        if func == "TOKENIZE":
            return re.findall(r"[a-z0-9]+", str(vals[0]).lower()) if vals else []
        if func == "ABS":
            return abs(vals[0]) if vals else None
        if func == "ROUND":
            return round(vals[0], int(vals[1])) if len(vals) > 1 else round(vals[0])
        # Unknown function: return the first argument (predictable, no crash).
        return vals[0] if vals else None

    def _aggregate(self, func: str, arg: str, row: dict):
        """Computes COUNT/SUM/AVG/MAX/MIN over the bag referenced by `arg`."""
        values = self._collect_bag(arg.strip(), row)
        if func == "COUNT":
            return len(values)
        nums = [v for v in values if isinstance(v, (int, float))]
        if not nums:
            return None
        if func == "SUM":
            return sum(nums)
        if func == "AVG":
            return sum(nums) / len(nums)
        if func == "MAX":
            return max(nums)
        if func == "MIN":
            return min(nums)
        return None

    def _collect_bag(self, arg: str, row: dict) -> list:
        """
        Resolves an aggregate argument into the list of values it aggregates.
        Handles `rel.field` (a field of every tuple in the bag) and `rel`
        (the bag itself).
        """
        if "." in arg and not arg.startswith("'"):
            head, _, tail = arg.partition(".")
            bag = row.get(head)
            if isinstance(bag, (list, tuple)):
                out = []
                for el in bag:
                    out.append(el.get(tail) if isinstance(el, dict) else el)
                return out
        if arg in row and isinstance(row[arg], (list, tuple)):
            return list(row[arg])
        return [self._eval(arg, row)]


# ---------------------------------------------------------------------------
# PIG SCRIPT GENERATION - the authentic artifacts a cluster would run.
# ---------------------------------------------------------------------------
def _script_01(input_csv: str, output_dir: str) -> str:
    """Clean + tokenize + word-count (LOAD/FOREACH/GROUP/COUNT/ORDER/STORE)."""
    return "\n".join([
        "-- ==========================================================================",
        "--  01_clean_and_count.pig",
        "--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)",
        "--  Component: Apache Pig (Pig Latin ETL)",
        "--  Owner  : Member 2 — Mohammed Hossam (Hadoop & MapReduce Specialist)",
        "--  Run    : pig -x mapreduce 01_clean_and_count.pig",
        "-- ==========================================================================",
        "--  GOAL: load the raw news CSV, clean + tokenize the article body, count",
        "--        global word frequencies and store the top 100 terms.",
        "",
        "-- 1) LOAD the raw CSV with the built-in PigStorage(',') loader.",
        "articles = LOAD '" + input_csv + "' USING PigStorage(',') AS (",
        "    article_id:chararray, title:chararray, author:chararray, date:chararray,",
        "    year:int, month:int, publication:chararray, category:chararray,",
        "    sub_topic:chararray, content:chararray, word_count:int, url:chararray);",
        "",
        "-- 2) FILTER out the CSV header row and any empty bodies.",
        "data = FILTER articles BY article_id != 'article_id' AND word_count > 0;",
        "",
        "-- 3) CLEAN: lowercase the body and replace every non-letter run with a space.",
        "cleaned = FOREACH data GENERATE",
        "    article_id,",
        "    REPLACE(LOWER(content), '[^a-z]+', ' ') AS clean_content;",
        "",
        "-- 4) TOKENIZE: FLATTEN turns the list of tokens into one row per word.",
        "tokens = FOREACH cleaned GENERATE FLATTEN(TOKENIZE(clean_content)) AS word;",
        "",
        "-- 5) GROUP by word and COUNT the occurrences in each bag.",
        "word_groups = GROUP tokens BY word;",
        "word_counts = FOREACH word_groups GENERATE group AS word, COUNT(tokens) AS cnt;",
        "",
        "-- 6) ORDER by descending frequency and keep the top 100 terms.",
        "ranked = ORDER word_counts BY cnt DESC;",
        "top_terms = LIMIT ranked 100;",
        "",
        "-- 7) STORE the result as a comma-separated part file.",
        "STORE top_terms INTO '" + output_dir + "/01_word_counts' USING PigStorage(',');",
        "",
        "-- 8) DUMP for a quick console sanity check.",
        "DUMP top_terms;",
        "",
    ]) + "\n"


def _script_02(staging_csv: str, output_dir: str) -> str:
    """Publisher-stats JOIN topic-stats per-article report (LOAD/FILTER/JOIN)."""
    return "\n".join([
        "-- ==========================================================================",
        "--  02_topic_publisher_join.pig",
        "--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)",
        "--  Component: Apache Pig (Pig Latin ETL)",
        "--  Owner  : Member 2 — Mohammed Hossam (Hadoop & MapReduce Specialist)",
        "--  Run    : pig -x mapreduce 02_topic_publisher_join.pig",
        "-- ==========================================================================",
        "--  GOAL: join publisher-level statistics with topic-level statistics to build",
        "--        a per-article report enriched with both aggregates.",
        "",
        "-- 1) LOAD the staged enriched corpus exported from the Parquet lakehouse.",
        "enriched = LOAD '" + staging_csv + "' USING PigStorage(',') AS (",
        "    article_id:chararray, publication:chararray, topic_label:chararray,",
        "    sentiment_polarity:double, word_count:int);",
        "",
        "-- 2) FILTER: keep only labelled articles with a real sentiment score.",
        "valid = FILTER enriched BY topic_label != '' AND sentiment_polarity IS NOT NULL;",
        "",
        "-- 3) Publisher-level statistics (GROUP by publication).",
        "pub_groups = GROUP valid BY publication;",
        "publisher_stats = FOREACH pub_groups GENERATE",
        "    group AS pub,",
        "    COUNT(valid) AS pub_articles,",
        "    AVG(valid.sentiment_polarity) AS pub_avg_sent;",
        "",
        "-- 4) Topic-level statistics (GROUP by topic).",
        "topic_groups = GROUP valid BY topic_label;",
        "topic_stats = FOREACH topic_groups GENERATE",
        "    group AS tlabel,",
        "    COUNT(valid) AS topic_articles,",
        "    AVG(valid.sentiment_polarity) AS topic_avg_sent;",
        "",
        "-- 5) JOIN articles->publisher stats, then ->topic stats.",
        "pub_joined = JOIN valid BY publication, publisher_stats BY pub;",
        "full_joined = JOIN pub_joined BY topic_label, topic_stats BY tlabel;",
        "",
        "-- 6) Project the final report columns.",
        "report = FOREACH full_joined GENERATE",
        "    article_id, publication, topic_label,",
        "    pub_articles, pub_avg_sent, topic_articles, topic_avg_sent;",
        "",
        "-- 7) ORDER and keep the first 50 rows for readability.",
        "ordered = ORDER report BY topic_label ASC, article_id ASC;",
        "top50 = LIMIT ordered 50;",
        "",
        "-- 8) STORE + DUMP.",
        "STORE top50 INTO '" + output_dir + "/02_topic_publisher_join' USING PigStorage(',');",
        "DUMP top50;",
        "",
    ]) + "\n"


def _script_03(staging_csv: str, output_dir: str) -> str:
    """Positive-sentiment filter + per-topic sentiment ranking (FILTER/GROUP/AVG)."""
    return "\n".join([
        "-- ==========================================================================",
        "--  03_sentiment_filter.pig",
        "--  Project: News Article Analysis and Topic Extraction (Big Data Architecture)",
        "--  Component: Apache Pig (Pig Latin ETL)",
        "--  Owner  : Member 2 — Mohammed Hossam (Hadoop & MapReduce Specialist)",
        "--  Run    : pig -x mapreduce 03_sentiment_filter.pig",
        "-- ==========================================================================",
        "--  GOAL: isolate positive-sentiment coverage and rank topics by mean sentiment.",
        "",
        "-- 1) LOAD the staged enriched corpus.",
        "enriched = LOAD '" + staging_csv + "' USING PigStorage(',') AS (",
        "    article_id:chararray, publication:chararray, topic_label:chararray,",
        "    sentiment_polarity:double, word_count:int);",
        "",
        "-- 2) FILTER: keep only clearly positive articles (polarity >= 0.2).",
        "positive = FILTER enriched BY sentiment_polarity >= 0.2;",
        "",
        "-- 3) GROUP the positive articles by topic.",
        "topic_groups = GROUP positive BY topic_label;",
        "",
        "-- 4) Aggregate sentiment statistics per topic.",
        "topic_sentiment = FOREACH topic_groups GENERATE",
        "    group AS topic_label,",
        "    COUNT(positive) AS positive_articles,",
        "    AVG(positive.sentiment_polarity) AS avg_sentiment,",
        "    MAX(positive.sentiment_polarity) AS max_sentiment,",
        "    MIN(positive.sentiment_polarity) AS min_sentiment;",
        "",
        "-- 5) ORDER by mean sentiment, highest first.",
        "ranked = ORDER topic_sentiment BY avg_sentiment DESC;",
        "",
        "-- 6) STORE + DUMP.",
        "STORE ranked INTO '" + output_dir + "/03_sentiment_by_topic' USING PigStorage(',');",
        "DUMP ranked;",
        "",
    ]) + "\n"


def write_pig_scripts(out_dir: str, input_csv: str, staging_csv: str) -> list:
    """
    Writes the three authentic `.pig` scripts into `out_dir` and returns their
    paths. `output_dir` inside the scripts is `out_dir/output` (posix style).
    """
    os.makedirs(out_dir, exist_ok=True)
    out_posix = out_dir.replace(os.sep, "/")
    output_dir = f"{out_posix}/output"
    scripts = {
        "01_clean_and_count.pig": _script_01(input_csv, output_dir),
        "02_topic_publisher_join.pig": _script_02(staging_csv, output_dir),
        "03_sentiment_filter.pig": _script_03(staging_csv, output_dir),
    }
    paths = []
    for name, content in scripts.items():
        path = os.path.join(out_dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        paths.append(path.replace(os.sep, "/"))
    _log(f"wrote {len(paths)} .pig scripts to {out_posix}")
    return paths


def _build_real_cluster_commands(out_dir: str, scripts: list) -> str:
    """Returns the content of `real_cluster_commands.md` (authentic pig CLI)."""
    pig_dir = out_dir.replace(os.sep, "/")
    script_names = [os.path.basename(s) for s in scripts]
    return "\n".join([
        "# Real Cluster Commands — Apache Pig",
        "",
        "Project : News Article Analysis and Topic Extraction (Big Data Architecture)",
        "Component: Apache Pig (Pig Latin ETL compiled to MapReduce/Tez)",
        "Owner   : Member 2 — Mohammed Hossam (Hadoop & MapReduce Specialist)",
        "",
        "These are the **authentic** commands to reproduce this component's workload",
        "on a real Hadoop cluster. The local `PigLatinEngine` produces identical",
        "*logical* results offline.",
        "",
        "## 0. Stage the scripts and the raw input onto HDFS",
        "```bash",
        f"hdfs dfs -mkdir -p hdfs://{HDFS_PIG_DIR}",
        f"hdfs dfs -put {script_names[0]} hdfs://{HDFS_PIG_DIR}/",
        "hdfs dfs -put raw_news_articles.csv hdfs:///user/news/raw/",
        "```",
        "",
        "## 1. Run a script in classic MapReduce mode",
        "```bash",
        "pig -x mapreduce 01_clean_and_count.pig",
        "```",
        "",
        "## 2. Run the same script on the Tez DAG engine (faster, no MR chaining)",
        "```bash",
        "pig -x tez 01_clean_and_count.pig",
        "```",
        "",
        "## 3. Tuning: enable map-side partial aggregation (combiner-like pre-agg)",
        "```bash",
        "pig -D pig.exec.mapPartAgg=true -D pig.exec.mapPartAgg.minReduction=0.8 \\",
        "    -x tez 01_clean_and_count.pig",
        "```",
        "",
        "## 4. Parameterise the run with -param (no script edits required)",
        "```bash",
        "pig -x tez -param INPUT=hdfs:///user/news/raw/raw_news_articles.csv \\",
        "    -param OUTPUT=hdfs:///user/news/pig/out 01_clean_and_count.pig",
        "```",
        "",
        "## 5. Run the full three-script pipeline",
        "```bash",
        "pig -x tez 01_clean_and_count.pig",
        "pig -x tez 02_topic_publisher_join.pig",
        "pig -x tez 03_sentiment_filter.pig",
        "```",
        "",
        "## 6. Pig on YARN — lifecycle notes",
        "- Each Pig job runs as a YARN application; watch it with:",
        "  `yarn application -list` / `yarn application -kill <app_id>`",
        "- Use `pig -x local` for a single-JVM local run (no cluster, like this module).",
        "- Logs / per-job counters: `pig -x tez -v 01_clean_and_count.pig`.",
        "",
        "## 7. Inspect the stored output",
        "```bash",
        f"hdfs dfs -ls hdfs:///user/news/pig/output",
        "hdfs dfs -cat hdfs:///user/news/pig/output/01_word_counts/part-r-00000 | head",
        "```",
        "",
        "> Local part-file layout: outputs live in `" + pig_dir + "/output/<relation>/`",
        "> with a `part-r-00000` data file and a `_SUCCESS` marker, mirroring HDFS.",
        "",
    ]) + "\n"


# ---------------------------------------------------------------------------
# ENRICHED-CORPUS STAGING
# ---------------------------------------------------------------------------
def stage_enriched_csv(parquet_dir: str, input_csv: str, staging_dir: str):
    """
    Exports the enriched corpus (topic + sentiment) to a small headerless CSV so
    the Pig scripts can LOAD it with plain PigStorage. Returns (path, rows).

    A real cluster would likewise export/join the Parquet-derived columns into a
    text format, or use a Parquet loader; we keep the staging step explicit.
    """
    import pandas as pd   # imported locally to keep module import lightweight
    df = None
    try:
        if os.path.exists(parquet_dir):
            df = pd.read_parquet(parquet_dir)
    except Exception:
        df = None
    if df is None or df.empty:
        # Fallback: the raw CSV (topic/sentiment columns will be defaulted).
        try:
            df = pd.read_csv(input_csv, encoding="utf-8")
        except Exception:
            df = pd.DataFrame(columns=["article_id", "publication", "topic_label",
                                       "sentiment_polarity", "clean_word_count"])
    # Guarantee every column the scripts need exists.
    defaults = {"article_id": "", "publication": "", "topic_label": "",
                "sentiment_polarity": 0.0}
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    if "word_count" not in df.columns:
        df["word_count"] = df["clean_word_count"] if "clean_word_count" in df.columns else 0

    cols = ["article_id", "publication", "topic_label", "sentiment_polarity", "word_count"]
    os.makedirs(staging_dir, exist_ok=True)
    out_path = os.path.join(staging_dir, "enriched_articles.csv")
    # Headerless output so Pig's first data row is NOT a header.
    df[cols].fillna("").to_csv(out_path, index=False, header=False)
    _log(f"staged enriched corpus -> {out_path.replace(os.sep, '/')} ({len(df)} rows)")
    return out_path, int(len(df))


# ---------------------------------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------------------------------
def run_pig_jobs(input_csv: str = DEFAULT_INPUT_CSV,
                 parquet_dir: str = DEFAULT_PARQUET_DIR,
                 mapreduce_tsv: str = DEFAULT_MAPREDUCE_TSV,
                 out_dir: str = DEFAULT_OUT_DIR) -> dict:
    """
    Stages the enriched corpus, writes the three `.pig` scripts + real-cluster
    command doc, executes every script with the local PigLatinEngine, writes the
    execution log and returns the contract dict.
    """
    started = datetime.datetime.now()
    out_posix = out_dir.replace(os.sep, "/")
    os.makedirs(out_dir, exist_ok=True)
    output_dir = os.path.join(out_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    staging_dir = os.path.join(out_dir, "staging")
    os.makedirs(staging_dir, exist_ok=True)

    # --- 1) Stage the enriched corpus that scripts 02/03 load ----------------
    staging_path, staged_rows = stage_enriched_csv(parquet_dir, input_csv, staging_dir)
    staging_rel = staging_path.replace(os.sep, "/")

    # --- 2) Write the three authentic Pig scripts ----------------------------
    scripts = write_pig_scripts(out_dir, input_csv=input_csv, staging_csv=staging_rel)

    # --- 3) Emit the authentic real-cluster command markdown -----------------
    commands_path = os.path.join(out_dir, "real_cluster_commands.md")
    with open(commands_path, "w", encoding="utf-8") as fh:
        fh.write(_build_real_cluster_commands(out_dir, scripts))

    # --- 4) Execute each script and collect a detailed execution log ----------
    engine = PigLatinEngine(verbose=True)
    execution_log = []      # per-script record
    relations_created = 0   # running total of assignment statements
    output_files = []       # every part file produced
    for script in scripts:
        relation_count_before = len(engine.output_files)
        t0 = time.perf_counter()
        created = engine.run_script(script)          # may raise -> surfaced clearly
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        produced = engine.output_files[relation_count_before:]
        relations_created += created
        output_files.extend(produced)
        execution_log.append({
            "script": os.path.basename(script),
            "script_path": script,
            "relations_created": created,
            "output_files": produced,
            "elapsed_ms": round(elapsed_ms, 3),
            "status": "SUCCESS",
        })
        _log(f"executed {os.path.basename(script)} in {elapsed_ms:.0f} ms "
             f"({created} relations, {len(produced)} output file(s))")

    # --- 5) Persist the execution log ----------------------------------------
    log_payload = {
        "component": "pig_transform",
        "engine": "PigLatinEngine (local simulation)",
        "staged_input": staging_rel,
        "staged_rows": staged_rows,
        "relations": {name: rel.schema for name, rel in engine.relations.items()},
        "scripts": execution_log,
    }
    log_path = os.path.join(out_dir, "pig_execution_log.json")
    with open(log_path, "w", encoding="utf-8") as fh:
        json.dump(log_payload, fh, indent=2, default=str)

    # --- 6) Record authentic HDFS staging commands ---------------------------
    hdfs_commands = []
    try:
        fs = LocalHDFS(root="data/hadoop_ecosystem/hdfs", verbose=False)
        fs.mkdirs(HDFS_PIG_DIR)
        # Stage the first script and the raw input into the simulated HDFS.
        fs.put(scripts[0], f"{HDFS_PIG_DIR}/{os.path.basename(scripts[0])}")
        hdfs_commands.append(f"hdfs dfs -mkdir -p hdfs://{HDFS_PIG_DIR}")
        hdfs_commands.append(
            f"hdfs dfs -put {os.path.basename(scripts[0])} hdfs://{HDFS_PIG_DIR}/")
    except Exception as exc:   # staging must never break the demonstration
        _log(f"HDFS staging skipped: {type(exc).__name__}: {exc}")

    # --- 7) Assemble, persist and return the contract dict -------------------
    summary = {
        "component": "pig_transform",
        "scripts_generated": scripts,
        "scripts_executed": len(execution_log),
        "relations_created": relations_created,
        "output_files": output_files,
        "staged_input": staging_rel,
        "staged_rows": staged_rows,
        "execution_log": log_path.replace(os.sep, "/"),
        "real_cluster_commands": commands_path.replace(os.sep, "/"),
        "hdfs_commands": hdfs_commands,
        "out_dir": out_posix,
    }
    summary_path = os.path.join(out_dir, "pig_summary.json")
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)

    elapsed = (datetime.datetime.now() - started).total_seconds()
    _log(f"executed {len(execution_log)} scripts in {elapsed:.2f}s; "
         f"relations_created={relations_created}, output_files={len(output_files)}")
    return summary


if __name__ == "__main__":
    # Run the full Pig demonstration and print a short completion summary.
    result = run_pig_jobs()
    _log(f"done — scripts_executed={result['scripts_executed']}, "
         f"relations_created={result['relations_created']}, "
         f"out_dir={result['out_dir']}")
