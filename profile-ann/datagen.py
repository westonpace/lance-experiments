import numpy as np
import pyarrow as pa

import lance

DIM = 768
NROWS = 300
NUM_PARTS = 4
NUM_SUB_VEC = 32
NUM_FRAGS = 4

vector_data = np.random.uniform(0, 1000, DIM * NROWS).astype('f')
filter_data = np.arange(0, NROWS)

table = pa.Table.from_pydict({
    "vector": pa.FixedSizeListArray.from_arrays(vector_data, DIM),
    "filter": filter_data
})

base_dir = f"dataset_dim={DIM}_nrows={NROWS}_nparts={NUM_PARTS}_nsubvec={NUM_SUB_VEC}_nfrag={NUM_FRAGS}"

rows_per_frag = int(NROWS / NUM_FRAGS)

dataset = lance.write_dataset(table, base_dir, max_rows_per_file=rows_per_frag, mode="create")
dataset.create_index(
    column="vector",
    index_type="IVF_PQ",
    metric_type="L2",
    num_partitions=NUM_PARTS,
    num_sub_vectors=NUM_SUB_VEC,
    num_bits=8
)