import lance
import random

DIM = 768
NROWS = 300
NUM_PARTS = 4
NUM_SUB_VEC = 32
NUM_FRAGS = 4

NQUERIES = 64
K = 10
NPROBES = 2
REFINE = 1

ALL_COLUMNS = ["vector", "filter"]
VECTOR_ONLY = ["vector"]

COLUMNS = VECTOR_ONLY
ROW_ID = False

base_dir = f"dataset_dim={DIM}_nrows={NROWS}_nparts={NUM_PARTS}_nsubvec={NUM_SUB_VEC}_nfrag={NUM_FRAGS}"

dataset = lance.dataset(base_dir)

sample_indices = random.sample(range(NROWS), NQUERIES)
queries = dataset.take(sample_indices).column("vector").chunk(0)

from lance.tracing import trace_to_chrome
trace_to_chrome(level='debug')

for query in queries:
    nearest = {
        "column": "vector",
        "q": query.values,
        "k": K,
        "nprobes": NPROBES,
        "refine_factor": REFINE,
    }
    dataset.scanner(columns=COLUMNS, limit=K, nearest=nearest)