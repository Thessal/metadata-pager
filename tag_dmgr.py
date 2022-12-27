import json
import pandas as pd
import numpy as np
import os
import pickle


class DmgrTag():
    def __init__(self):
        pass

    def load_common_tags(self, min_count=1000):
        with open("./tags/tags000000000000.json", "r") as f:
            tags_d = [json.loads(line) for line in f.readlines()]

        df_tags = pd.DataFrame(tags_d)
        df_tags_0 = df_tags.query("category=='0'")
        df_tags_0["post_count"] = df_tags_0["post_count"].astype(int)
        df_tags_0 = df_tags_0.sort_values("post_count")
        df_tags_0_TOP = df_tags_0[df_tags_0["post_count"] > min_count]
        return df_tags_0_TOP
