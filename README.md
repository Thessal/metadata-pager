# Metadata loader

A pager for handling terabytes of metadata

Python (slow)

### Sample Usage

```python
from group_dmgr import DmgrTagGroup
from tag_dmgr import DmgrTags
from loader import TagLoader
from dataset import DataLoader

# Data Statistics
tag_group_info = DmgrTagGroup()
tag_info = DmgrTags()

# Metadata and data
tags = TagLoader(tag_group_info, tag_info)
datasets = DataLoader(tags)
dataset_1 = datasets.poisson_sample(density=0.2)
dataset_2 = datasets.batch_shuffle(batch_size=1000)

# Use
from links_cluster.cache_wrapper import EvictingCacheWrapper
cache = EvictingCacheWrapper(0.1, 0.05, 1.0, True, 10)
for data in dataset:
    similar_vectors = cache.push(new_key=100, new_vector=np.array([1,0,0,0,0]), top_n=10)
    model.train(similar_vectors)
```