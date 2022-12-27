# Metadata stream loader (WIP)

A pager for handling terabytes of metadata

"It works for me"

### Sample Usage

```python
from group_dmgr import DmgrTagGroup
from tag_dmgr import DmgrTag
from loader import DmgrData

# Statistics for data encoding
tag_group_info = DmgrTagGroup()
tag_info = DmgrTag()

# Metadata and actual data
dataloader = DmgrData(cfg, tag_group_info, tag_info)
dataset_1 = dataloader.poisson_sample(density=0.2)
dataset_2 = dataloader.batch_shuffle(batch_size=1000)
```