import json
import pandas as pd
import urllib
import os
from config import CFG


class DmgrTagGroup:
    def __init__(self, srcdir=CFG["group"]["src"], cachedir=CFG["group"]["dst"], max_group_level=5,
                 include_pools=False, include_artists=False
                 ):
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
                lambda name, tag_url, pattern: "artist:" + urllib.parse.unquote(tag_url).replace(pattern, "")
            ),
        }
        cache_file = f"{cachedir}/tag_group.pkl"
        if not os.path.isfile(cache_file):
            self.build(srcdir).to_pickle(cache_file)
        self.tag_group = pd.read_pickle(cache_file)

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
        source_file = f"{srcdir}/e.json"  # https://github.com/wangyi041228/danbooru_tags
        with open(source_file, "rt") as f:
            tag_groups = json.load(f)

        tag_enum = self.traverse(tag_groups)
        df_tag = pd.DataFrame(tag_enum, dtype=str)

        # Caculate unique group path
        unique_tag = (df_tag + "/").cumsum(axis=1)[df_tag != ""].fillna("").applymap(lambda x: x[:-1])
        unique_tag["tag"] = df_tag[df_tag != ""].apply(lambda x: x.dropna().tolist()[-1], axis=1)

        return unique_tag
