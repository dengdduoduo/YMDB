import hashlib
import re
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple

from . import options
from .logging import Level, log
from .types import (ExecutionStats, Value, Result, Bindings, Binding,
                    ServerCrashedException, ServerErrorException)


def _generate_keyspace_name(data_file: Path) -> str:
    rel = data_file.relative_to(options.TEST_SUITES)
    name = str(rel).replace("/", "_").replace("\\", "_").replace(".", "_")
    short = hashlib.md5(name.encode()).hexdigest()[:12]
    return f"{options.MDB_KEYSPACE_PREFIX}_{short}"


def _keyspace_from_data(data_file: Optional[Path]) -> Optional[str]:
    if data_file is None:
        return None
    return _generate_keyspace_name(data_file)


def import_data(executable: Path, data_file: Path, keyspace: str) -> None:
    cmd = [
        str(executable),
        f"--mdb_masters={options.MDB_MASTERS}",
        f"--mdb_keyspace={keyspace}",
        "import",
        str(data_file),
    ]
    log(Level.DEBUG, f"Importing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        stderr = result.stderr[:500]
        log(Level.ERROR, f"Import failed for {data_file.name}: {stderr}")
        raise RuntimeError(f"Import failed: {stderr}")


def verify_cluster_alive() -> bool:
    cmd = [
        str(options.MDB_CLI_PATH),
        f"--mdb_masters={options.MDB_MASTERS}",
        f"--mdb_keyspace=system",
        "query",
        "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _parse_value(raw_val: str) -> Value:
    val = raw_val.strip()
    if val.startswith("<") and val.endswith(">"):
        return Value(value=val[1:-1], type_=Value.Type.IRI, datatype="", language="")
    elif val.startswith("_:"):
        return Value(value=val, type_=Value.Type.BNODE, datatype="", language="")
    elif val.startswith('"'):
        content = val[1:]
        datatype = ""
        language = ""

        caret_idx = content.rfind("^^<")
        if caret_idx != -1:
            datatype = content[caret_idx + 3:].rstrip(">")
            content = content[:caret_idx]
            quote_end = content.rfind('"')
            if quote_end != -1:
                content = content[:quote_end]
        else:
            at_idx = content.rfind('"@')
            if at_idx != -1:
                language = content[at_idx + 2:]
                content = content[:at_idx]
            else:
                quote_end = content.rfind('"')
                if quote_end != -1:
                    content = content[:quote_end]

        return Value(value=content, type_=Value.Type.LITERAL, datatype=datatype, language=language)
    else:
        return Value(value=val, type_=Value.Type.LITERAL, datatype="", language="")


def parse_mdb_cli_output(output: str) -> Tuple[Optional[Result], Optional[bool]]:
    if not output.strip():
        return Result(variables=set(), bindings=[]), None

    lines = output.strip().split("\n")

    is_ask = False
    var_names: list[str] = []
    bindings: Bindings = []
    current_binding: Binding = {}

    for line in lines:
        line = line.rstrip()

        if line == "---":
            if current_binding:
                bindings.append(current_binding)
                current_binding = {}
            continue

        if "\t" in line:
            parts = line.split("\t", 1)
            if len(parts) == 2:
                var_name = parts[0].strip()
                if var_name.startswith("?"):
                    var_name = var_name[1:]
                raw_val = parts[1].strip()

                if var_name == "_ask_result":
                    is_ask = True
                    if raw_val.lower() == "true":
                        return None, True
                    elif raw_val.lower() == "false":
                        return None, False

                value = _parse_value(raw_val)
                current_binding[var_name] = value
                if var_name not in var_names:
                    var_names.append(var_name)

    if current_binding:
        bindings.append(current_binding)

    if is_ask:
        if bindings and "_ask_result" in bindings[0]:
            val = bindings[0]["_ask_result"].value
            return None, val.lower() == "true"
        return None, False

    variables = set(var_names)
    return Result(variables=variables, bindings=bindings), None


def execute_query(db_dir: Optional[str], test, stats: ExecutionStats,
                  bad_query: bool = False) -> Tuple[Optional[Result], Optional[bool]]:
    query_str = test.query.read_text()

    keyspace = db_dir if db_dir else "system"
    if db_dir is None and test.data:
        keyspace = _generate_keyspace_name(test.data)

    cmd = [
        str(options.MDB_CLI_PATH),
        f"--mdb_masters={options.MDB_MASTERS}",
        f"--mdb_keyspace={keyspace}",
        "query",
        query_str,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=options.TIMEOUT)

        if bad_query:
            if result.returncode != 0:
                raise ServerErrorException()
            return None, None

        if result.returncode != 0:
            stderr = result.stderr[:500] if result.stderr else "(no stderr)"
            stdout = result.stdout[:500] if result.stdout else "(no stdout)"

            if "Segmentation fault" in stderr or "SIGSEGV" in stderr or "signal" in stderr.lower():
                log(Level.ERROR, f"Server crashed on {test.query.name}: {stderr}")
                stats.error += 1
                raise ServerCrashedException(f"YB crashed on query {test.query}")

            known_error_keywords = [
                "ERROR", "Exception", "Parse error", "parsing error",
                "not supported", "UnsupportedQuery", "semantic error",
                "syntax error", "not implemented",
                "failed", "failed to execute"
            ]
            is_expected_error = any(kw.lower() in (stderr + stdout).lower() for kw in known_error_keywords)

            if is_expected_error:
                log(Level.ERROR, f"Query error on {test.query.name}: {stderr[:200]}")
                stats.error += 1
                return None, None
            else:
                log(Level.ERROR, f"Unexpected mdb_cli failure on {test.query.name}: stderr={stderr[:200]} stdout={stdout[:200]}")
                stats.error += 1
                return None, None

        stdout_text = result.stdout

        if test.graph_output:
            return Result(variables=set(), bindings=[]), None

        parsed_result, ask_result = parse_mdb_cli_output(stdout_text)
        return parsed_result, ask_result

    except subprocess.TimeoutExpired:
        log(Level.WARNING, f"Timeout on {test.query.name}")
        stats.error += 1
        return None, None
    except ServerCrashedException:
        raise
    except ServerErrorException:
        raise
    except Exception as e:
        log(Level.ERROR, f"Exception executing {test.query.name}: {e}")
        stats.error += 1
        raise
