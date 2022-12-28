import json
import lzma, tarfile
import os
import requests
import io
import re
from .config import CFG
from .util import calc_self_hash


def page(args):
    cachedir, cachedir_block, block = args
    command = (
            'xzcat ' + cachedir + ' | grep -E "\\"id\\": \\"[0-9]*' + block + '\\"" | xz > ' +
            cachedir_block
    )
    print(command)
    os.system(command)


class TagLoader:
    def __init__(self, dmgr_group, dmgr_tag, srcdir=CFG["tag"]["src_all"], cachedir=CFG["tag"]["dst"], debug=False):
        locals_ = locals()
        group_n, tag_n = len(dmgr_group), len(dmgr_tag)
        locals_["group_n"], locals_["tag_n"] = group_n, tag_n
        varnames = ["srcdir", "cachedir", "debug", "group_n", "tag_n"]
        hash_ = "" if debug else calc_self_hash(str(__file__), locals_, varnames)
        self.dmgr_tag = dmgr_tag
        self.dmgr_group = dmgr_group
        self.srcdir = srcdir
        self.cachedir = cachedir + f"/metadata_processed_{hash_}_{group_n}_{tag_n}.json.xz"
        self.cachedir_block_dir = cachedir + f"./metadata_processed_{hash_}_{group_n}_{tag_n}/"
        self.cachedir_block = lambda idx: cachedir + f"./metadata_processed_{hash_}_{group_n}_{tag_n}/{idx}.json.xz"
        raw_data = self.load_raw()
        self.process(raw_data)

    def load_raw(self):
        """Loads raw metadata"""
        tags_lst = {}
        with tarfile.open(name=self.srcdir, mode='r|xz') as tar:
            for tarinfo in tar:
                print(tarinfo.name)
                if tarinfo.isreg():  # regular file
                    tags_lst[tarinfo.name] = []
                    with tar.extractfile(tarinfo) as f:
                        for line in f.readlines():
                            try:
                                result = json.loads(line)
                                yield result
                            except Exception as e:
                                print(f"[json load] {e} : {line}")

    def process(self, raw_data):
        if not os.path.isfile(self.cachedir):
            with open(self.cachedir, 'wb', buffering=1024 * 1024) as f:
                lzc = lzma.LZMACompressor()
                data = b""
                for x in raw_data:
                    if (int(x["score"]) > 5) and (x['file_ext'].lower() in ['jpg', 'jpeg', 'bmp', 'png', 'gif']):
                        tag_names = [y['name'] for y in x['tags']]
                        tag_processed = {
                            "id": x['id'],
                            "pools": x["pools"],
                            "file_ext": x['file_ext'],
                            "tags_": self.dmgr_tag.index(tag_names),
                            "groups_": self.dmgr_group.index(tag_names),
                        }
                        data += lzc.compress((json.dumps(tag_processed) + '\n').encode(encoding='utf-8'))
                data += lzc.flush()
                f.write(data)
            self.page_all()

    def page_all(self):
        from multiprocessing import Pool
        import os
        import itertools
        os.system("mkdir " + self.cachedir_block_dir)

        blocks = list(map(lambda x: str(x).zfill(3), range(1000)))
        args_all = zip(itertools.repeat(self.cachedir), [self.cachedir_block(b) for b in blocks], blocks)
        with Pool(5) as p:
            p.map(page, args_all)

    def load(self):

        def metadata_gen(blocks):
            if blocks is None:
                blocks = []
            else:
                blocks = list(map(lambda x: str(x).zfill(3), blocks))

            def _metadata():
                with lzma.open(self.cachedir, mode='rb') as f:
                    for line in f:
                        yield line

            for idx in map(lambda x: str(x).zfill(3), range(1000)):
                if idx not in blocks:
                    continue

                block_cache_path = self.cachedir_block(idx)
                if not os.path.isfile(block_cache_path):
                    # FIXME
                    # Note : grep is much much faster than python
                    # for file in $(seq 100 998); do echo ${file}; xzcat ../metadata_processed_1000.json.xz | grep -E "\"id\": \"[0-9]*${file}\"" | xz > 1000_${file}.json.xz; done;,
                    print(f"building cache {idx}")
                    metadata = _metadata()
                    with open(block_cache_path, 'wb', buffering=1024 * 1024) as f:
                        lzc = lzma.LZMACompressor()
                        data = b""
                        for x in metadata:
                            if re.match(b'{"id": "[0-9]*' + (idx.encode(encoding='utf-8', errors='strict')) + b'",', x):
                                data += lzc.compress(x)
                        data += lzc.flush()
                        f.write(data)

                result = {}
                with lzma.open(block_cache_path, mode='rb') as f:
                    for line in f:
                        info = json.loads(line)
                        result[info['id']] = info
                yield idx.zfill(4), result

        return metadata_gen
