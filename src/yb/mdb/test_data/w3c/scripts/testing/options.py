from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

TEST_SUITES = PROJECT_ROOT / "test_suites"
INTERNAL_TESTS = TEST_SUITES / "internal"
OFFICIAL_TESTS = TEST_SUITES / "sparql11"

ROOT_MANIFESTS = [
    TEST_SUITES / "sparql10" / "manifest-evaluation.ttl",
    TEST_SUITES / "sparql11" / "manifest-sparql11-query.ttl",
    TEST_SUITES / "sparql11" / "manifest-sparql11-results.ttl",
]

HOST = "127.0.0.1"
PORT = 8080
TIMEOUT = 60

NOT_SUPPORTED_KEYWORDS = ["from", "graph"]

CANONICALIZE_URI = True
CANONICALIZE_BOOLEANS = True
CANONICALIZE_NUMBERS = True
CANONICALIZE_LANGUAGES = True
TRANSLATE_DATATYPES = {}

CWD = PROJECT_ROOT

LOGGING_LEVELS = {
    "CORRECT": True,
    "ERROR": True,
    "BEGIN": True,
    "END": False,
    "WARNING": True,
    "SKIPPED": True,
    "OUTPUT": True,
    "SUMMARY": True,
    "DEBUG": False,
    "SERVER_LOG": False,
}

YB_BUILD_DIR = Path("/home/nonitem/yugabyte-db/build/release-clang21-dynamic-ninja")
MDB_CLI_PATH = YB_BUILD_DIR / "bin" / "mdb_cli"
MDB_MASTERS = "127.0.0.1:7100"
MDB_KEYSPACE_PREFIX = "w3c_test"
MDB_USE_TRANSACTIONS = True
MDB_USE_PARTITION_LOAD = True

YB_CTL_PATH = Path("/home/nonitem/yugabyte-db/bin/yb-ctl")

MDB_EXECUTABLE = Path("/home/nonitem/MillenniumDB/build/native/release/bin/mdb")
MDB_PORT = 8080
