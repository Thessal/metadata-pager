# Metadata loader

A pager for handling terabytes of metadata

Python (slow)

### Sample Usage

```python
from metadata_loader import DmgrTagGroup, DmgrTags, TagLoader
from links_clustering import EvictingCacheWrapper

# Data Statistics
tags_info = DmgrTags()
groups_info = DmgrTagGroup()
tag_loader = TagLoader()

# Metadata and data (WIP)
tags = TagLoader(groups_info, tags_info)
datasets = DataLoader(tags)
dataset_1 = datasets.poisson_sample(density=0.2)
dataset_2 = datasets.batch_shuffle(batch_size=1000)

# Use
cache = EvictingCacheWrapper(0.1, 0.05, 1.0, True, 10)
for data in dataset:
    similar_vectors = cache.push(new_key=100, new_vector=np.array([1,0,0,0,0]), top_n=10)
    model.train(similar_vectors)
```