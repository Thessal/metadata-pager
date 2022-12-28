import json
import pandas as pd
import numpy as np
import lzma, tarfile
import os
import re
from .config import CFG
from .util import calc_self_hash

class DmgrTags:
    def __init__(self, srcdir=CFG["tag"]["src"], cachedir=CFG["tag"]["dst"], count_thres=1000):
        self.hash_ = calc_self_hash(str(__file__), locals(), self.__init__.__code__.co_varnames)

        self.srcdir = srcdir
        self.cachedir = cachedir

        cache_file = f"{cachedir}/tag_index_{self.hash_}.pkl"
        if not os.path.isfile(cache_file):
            self.load_tags_index(min_count=count_thres).to_pickle(cache_file)
        self.tags_index = pd.read_pickle(cache_file)

    def load_tags_index(self, min_count=1000):
        with open(f"{self.srcdir}", "r") as f:
            tags_d = [json.loads(line) for line in f.readlines()]

        df_tags = pd.DataFrame(tags_d)
        df_tags_0 = df_tags.query("category=='0'").copy()
        df_tags_0["post_count"] = df_tags_0["post_count"].astype(int)
        df_tags_0 = df_tags_0.sort_values("post_count", ascending=False)
        df_tags_0_TOP = df_tags_0[df_tags_0["post_count"] > min_count]
        return df_tags_0_TOP['name']

    def index(self, tags):
        idxs = [self.tags_index.index.get_loc(x) for x in tags if (x in self.tags_index)]
        return list(set(idxs))

    def get(self, idxs):
        tags = [self.tags_index.index[idx] for idx in idxs]
        return tags

    def encode(self, tags):
        one_hot = np.zeros(len(self.tags_index.index))
        one_hot[self.index(tags)] = 1
        return one_hot

    def decode(self, one_hot):
        tags = self.get([i for i, x in enumerate(one_hot) if x > 0])
        return tags

    def __len__(self):
        return len(self.tags_index.index)

    def __getitem(self, x):
        assert type(x) == 'list'
        if type(x[0]) == 'str':
            return self.index(x)
        elif type(x[0]) == 'int':
            return self.get(x)
