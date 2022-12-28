import json
import pandas as pd
import urllib
import os
from .util import calc_self_hash
from .config import CFG
import numpy as np

class DmgrTagGroup:
    def __init__(self, srcdir=CFG["group"]["src"], cachedir=CFG["group"]["dst"], tag_src=CFG["tag"]["src"],
                 max_group_level=5, group_min_freq=10000,
                 include_pools=False, include_artists=False,
                 ):
        self.hash_ = calc_self_hash(str(__file__), locals(), self.__init__.__code__.co_varnames)
        self.max_group_level = max_group_level
        self.include = {
            "tags": True,
            "pools": include_pools,
            "artists": include_artists,
        }
        self.rules = {
            "tags": (
                "https://danbooru.donmai.us/wiki_pages/",
                lambda _, tag_url, pattern: urllib.parse.unquote(tag_url).replace(pattern, "")
            ),
            "pools": (
                "https://danbooru.donmai.us/pools/",
                lambda name, tag_url, pattern: "pool " + urllib.parse.unquote(tag_url).replace(pattern,
                                                                                               "") + ": " + name
            ),
            "artists": (
                "https://danbooru.donmai.us/artists/show_or_new?name=",
                lambda name, tag_url, pattern: urllib.parse.unquote(tag_url).replace(pattern, "") + "_(artist)"
            ),
        }
        self.load(cachedir, srcdir, tag_src, group_min_freq)

    def load(self, cachedir, srcdir, tag_src, group_min_freq):
        cache_file = f"{cachedir}/tag_group_{self.hash_}.pkl"
        if not os.path.isfile(cache_file):
            self.build(srcdir).to_pickle(cache_file)
        self.tag_group = pd.read_pickle(cache_file)

        cache_file_count = f"{cachedir}/tag_group_count_{self.hash_}.pkl"
        cache_file_map = f"{cachedir}/tag_group_map_{self.hash_}.pkl"
        if not (os.path.isfile(cache_file_count) and os.path.isfile(cache_file_map)):
            df_tag_group_count, tag_group_map = self.build_index(tag_src, group_min_freq)
            df_tag_group_count.to_pickle(cache_file_count)
            tag_group_map.to_pickle(cache_file_map)
        self.tag_group_count, self.tag_group_map = pd.read_pickle(cache_file_count), pd.read_pickle(cache_file_map)

    def build_index(self, tag_src, min_freq=10000):
        with open(tag_src, "r") as f:
            tags_cnt = pd.DataFrame([
                json.loads(line) for line in f.readlines()
            ]).set_index('name')["post_count"].astype(int).to_dict()

        # Merge tag and group table
        tag_group_count = {k: 0 for k in list(set([y for x in self.tag_group.set_index("tag").values for y in x]))}
        map_fail, map_success = [], []
        for k, v in self.tag_group.set_index("tag").iterrows():
            group_tags = [x for x in v.tolist() if (x != "")]
            count = tags_cnt[k] if k in tags_cnt else 0
            if k not in tags_cnt:
                map_fail.append(k)
            else:
                map_success.append(k)
            for group_tag in group_tags:
                tag_group_count[group_tag] += count
        print(f"group mapping : {len(map_success)} success, {len(map_fail)} failure")

        # Filter groups by frequency
        df_tag_group_count = pd.Series(tag_group_count).sort_values(ascending=False)
        df_tag_group_count = df_tag_group_count[df_tag_group_count > min_freq]
        df_tag_group_count = pd.Series({
            k: x for k, x in df_tag_group_count.iteritems() if
            not any(map(lambda x: k in x, df_tag_group_count[k:].iloc[1:].keys()))
        })

        tag_group_ = self.tag_group.set_index("tag")
        tag_group_map = pd.Series({
            k: [
                df_tag_group_count.index.get_loc(x) for x in tag_group_.loc[k].values.ravel() if
                (x in df_tag_group_count)  # Use frequent group only
            ]
            for k in map_success
        })
        tag_group_map = tag_group_map[tag_group_map.apply(len) > 1]  # Remove empty group mapping

        return df_tag_group_count, tag_group_map  # Group List, Tag-GroupIndex table

    def traverse(self, info_lst, levels=[]):
        """DFS Tag group"""
        infos = []
        for info in info_lst:
            if type(info) != list:
                continue
            elif all(type(x) == str for x in info):
                if (len(info) == 2) and (info[1].startswith('http')):
                    tag_desc, tag_url = info

                    # Get tag from url
                    for k, v in self.include.items():
                        if v:
                            pattern, rule = self.rules[k]
                            if pattern in tag_url:
                                tag_url = rule(tag_desc, tag_url, pattern)
                    if "http" in tag_url:
                        tag = None
                        continue
                    else:
                        tag = tag_url

                    # # Skip metatags such as 'tag_group:dogs', 'Disclaimer:English', because they are already handled
                    # if len(levels)>2 and levels[2] == "Metatags":
                    #     continue

                    levels = [y for y in [x.strip() for x in levels] if y != '']
                    assert self.max_group_level >= len(levels)
                    zfill_levels = (levels + [''] * self.max_group_level)[:self.max_group_level]

                    infos.append(zfill_levels + [tag])
                else:
                    # print(info)
                    continue
            else:
                level = [info[0]] if type(info[0]) == str else []
                infos.extend(self.traverse(info, levels + level))
        return infos

    def build(self, srcdir):
        source_file = f"{srcdir}"
        with open(source_file, "rt") as f:
            tag_groups = json.load(f)

        tag_enum = self.traverse(tag_groups)
        df_tag = pd.DataFrame(tag_enum, dtype=str)

        # Caculate unique group path
        unique_tag = (df_tag + "/").cumsum(axis=1)[df_tag != ""].fillna("").applymap(lambda x: x[:-1]).iloc[:, :-1]
        unique_tag["tag"] = df_tag[df_tag != ""].apply(lambda x: x.dropna().tolist()[-1], axis=1)

        return unique_tag

    def index(self, tags):
        idxs = [y for x in tags if (x in self.tag_group_map) for y in self.tag_group_map[x]]
        return list(set(idxs))

    def get(self, idxs):
        groups = [self.tag_group_count.index[idx] for idx in idxs]
        return groups

    def encode(self, tags):
        one_hot = np.zeros(len(self.tag_group_map.index))
        one_hot[self.index(tags)] = 1
        return one_hot

    def decode(self, one_hot):
        # NOTE: Not 1-to-1
        groups = self.get([i for i, x in enumerate(one_hot) if x > 0])
        return groups

    def __len__(self):
        return len(self.tag_group_map.index)

    def __getitem(self, x):
        assert type(x) == 'list'
        if type(x[0]) == 'str':
            return self.index(x)
        elif type(x[0]) == 'int':
            return self.get(x)
