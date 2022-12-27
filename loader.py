import json
import pandas as pd
import numpy as np
import lzma, tarfile
import os
import requests
import io
import pickle
import re
from config import CFG
from util import calc_self_hash

class Encoder:
    def __init__(self):
        pass


class GroupTagFlattenEncodder(Encoder):
    def __init__(self):
        pass


class HierarchicalEncodder(Encoder):
    def __init__(self):
        pass


class TagOnlyEncodder(Encoder):
    def __init__(self):
        pass


class Sampler:
    def __init__(self):
        pass


class DmgrMetaData:
    def __init__(self, cachedir=CFG["tag"]["dst"]):
        hash_ = calc_self_hash(str(__file__), locals(), self.__init__.__code__.co_varnames)

        # cache_file = f"{cachedir}/data_index{self.hash_}.pkl"
        # if not os.path.isfile(cache_file):
        #     self.build().to_pickle(cache_file)
        # self.data_index = pd.read_pickle(cache_file)

    def build_generate(self):
        def get_tag():
            tags_lst = {}
            with tarfile.open(name='./tags/metadata.json.tar.xz', mode='r|xz') as tar:
                # tarinfo = tar.next()
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

        cache_path = f"metadata_processed_{cfg['MIN_COUNT']}.json.xz"
        if not os.path.isfile(cache_path):
            tags_gen = get_tag()
            with open(cache_path, 'wb', buffering=1024 * 1024) as f:
                lzc = lzma.LZMACompressor()
                data = b""
                for x in tags_gen:
                    if (int(x["score"]) > 5) and (x['file_ext'].lower() in ['jpg', 'jpeg', 'bmp', 'png', 'gif']):
                        tag_processed = {
                            "id": x['id'],
                            "pools": x["pools"],
                            "file_ext": x['file_ext'],
                            "tags_": cfg["make_label"](
                                [t["name"] for t in x["tags"] if (t["name"] in cfg["visible_tags"])]),
                        }
                        data += lzc.compress((json.dumps(tag_processed) + '\n').encode(encoding='utf-8'))
                data += lzc.flush()
                f.write(data)

        def metadata_gen(blocks):
            if blocks is None:
                blocks = []
            else:
                blocks = list(map(lambda x: str(x).zfill(3), blocks))

            def _metadata():
                with lzma.open(cache_path, mode='rb') as f:
                    for line in f:
                        yield line

            for idx in map(lambda x: str(x).zfill(3), range(1000)):
                if idx not in blocks:
                    continue

                block_cache_path = f"./metadata_processed/{cfg['MIN_COUNT']}_{idx}.json.xz"
                if not os.path.isfile(block_cache_path):
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

                # Yield
                result = {}
                with lzma.open(block_cache_path, mode='rb') as f:
                    for line in f:
                        info = json.loads(line)
                        result[info['id']] = info
                yield idx.zfill(4), result

        return metadata_gen


class DmgrData:
    def __init__(self, metadata_gen, N, train_test="train", verbose=True, normalize_label=False):
        self.verbose = verbose
        self.normalize_label = normalize_label
        self.metadata_gen = metadata_gen
        if train_test == "train":
            self.blocks = list(range(999))
        else:
            self.blocks = [999]
        self.encoder = tf.keras.layers.CategoryEncoding(output_mode="multi_hot", num_tokens=N)
        self.encode = lambda x: self.encoder([y for y in x if y < N])
        self.status = {"block": "", "id": "", "epoch": -1}

    def status(self):
        print(self.status)

    def _print(self, *args):
        if self.verbose:
            print(*args)

    def gen(self):
        blocks = self.metadata_gen(self.blocks)
        self.status["epoch"] += 1
        for block, metadata_dict in blocks:
            self.status["block"] = block
            if len(metadata_dict) == 0:
                continue
            self._print(f"Requesting block {block}")
            response = requests.get(f"{data_server}{block}.tar")
            assert (response.status_code == 200)
            self._print(f"Loaded block {block}")

            with io.BytesIO(response.content) as io_tar:
                with tarfile.open(fileobj=io_tar, mode='r|') as tar:
                    self._print(f"Opened block {block}")
                    for tarinfo in tar:
                        if tarinfo.isreg():  # regular file
                            img_id, img_ext = os.path.splitext(os.path.basename(tarinfo.name))
                            img_ext = img_ext.replace(".", "")
                            self.status["id"] = img_id
                            if img_id not in metadata_dict:
                                self._print(f"{img_id} not found in metadata : {self.status}")
                                continue
                            info = metadata_dict[img_id]
                            img_id = info["id"]
                            img_ext = info["file_ext"]
                            label = info["tags_"]
                            if len(label) < 3:
                                self._print(f"[error] {label}")
                                continue
                            with tar.extractfile(tarinfo) as f:
                                self._print(f"Read file {img_id, img_ext, label}")
                                try:
                                    if info["id"] in ['3928385', '2598999']:
                                        # {'block': '0385', 'id': '3928385'} jpeg::Uncompress failed. Invalid JPEG data or crop window. [Op:DecodeImage]
                                        continue
                                    image = tf.image.decode_image(f.read(), channels=3, expand_animations=False,
                                                                  dtype=tf.uint8)
                                    if self.normalize_label:
                                        label_enc = tf.keras.utils.normalize(self.encode(label))
                                    else:
                                        label_enc = self.encode(label)
                                    if (image.shape[0] != 512) or (image.shape[1] != 512):
                                        continue
                                    yield image, tf.squeeze(label_enc)
                                except:
                                    print(f"{info} : failed")


def prepare_dataset(cfg, repeat=True, mode="all_tags"):
    if ("train_dataset" in cfg) and ("test_dataset" in cfg):
        return cfg
    metadata_gen, INPUT_IMAGE_SIZE = cfg["metadata_gen"], cfg["INPUT_IMAGE_SIZE"]
    BUFFER_SIZE, BATCH_SIZE = cfg["BUFFER_SIZE"], cfg["BATCH_SIZE"]
    #     test_data_count = 1000
    #     metadata_sort = sorted(metadata,key=lambda x: x["id"][-3:])

    reader_train = Datagen(metadata_gen, len(cfg[mode]), train_test="train", verbose=False, normalize_label=False)
    reader_test = Datagen(metadata_gen, len(cfg[mode]), train_test="test", verbose=False, normalize_label=False)
    cfg["reader_train"] = reader_train
    cfg["reader_test"] = reader_test

    output_signature = (
        tf.TensorSpec(shape=(INPUT_IMAGE_SIZE, INPUT_IMAGE_SIZE, 3), dtype=tf.uint8),
        tf.TensorSpec(shape=(len(cfg[mode])), dtype=tf.float32),
    )
    dataset_train = tf.data.Dataset.from_generator(reader_train.gen, output_signature=output_signature)
    dataset_test = tf.data.Dataset.from_generator(reader_test.gen, output_signature=output_signature)
    if repeat:
        train_dataset = dataset_train.repeat().shuffle(BUFFER_SIZE).batch(BATCH_SIZE)
        test_dataset = dataset_test.repeat().shuffle(BUFFER_SIZE).batch(BATCH_SIZE)
    else:
        train_dataset = dataset_train.shuffle(BUFFER_SIZE).batch(BATCH_SIZE)
        test_dataset = dataset_test.shuffle(BUFFER_SIZE).batch(BATCH_SIZE)
    cfg["train_dataset"] = train_dataset
    cfg["test_dataset"] = test_dataset
    return cfg

# cfg.pop("train_dataset")
