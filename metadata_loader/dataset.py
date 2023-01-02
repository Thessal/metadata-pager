from collections import OrderedDict
import requests
import io
import os
import tarfile


class TarWrapper:
    def __init__(self, prefix, block_idx, stream=False):
        self.block_idx = block_idx
        self.stream = stream
        self.prefix = prefix

    def __enter__(self):
        response = requests.get(f"{self.prefix}/{self.block_idx}.tar")
        assert (response.status_code == 200)
        # print(f"Loaded block {block}")

        self.buffer = io.BytesIO(response.content)
        self.tar = tarfile.open(fileobj=self.buffer, mode='r|' if self.stream else 'r')
        self.tarinfos = (tarinfo for tarinfo in self.tar if tarinfo.isreg())  # regular file

        if not self.stream:
            self.tar_data = {}
            for tarinfo in self.tarinfos:
                img_id, img_ext, image = self.parse_tarinfo(tarinfo)
                self.tar_data[img_id] = (img_ext, image)

        return self

    def parse_tarinfo(self, tarinfo):
        img_id, img_ext = os.path.splitext(os.path.basename(tarinfo.name))
        img_ext = img_ext.replace(".", "")
        with self.tar.extractfile(tarinfo) as f:
            image = f.read()
            return img_id, img_ext, image

    def query(self, item_idx):
        assert not self.stream
        img_ext, image = self.tar_data[item_idx]
        return img_ext, image

    def make_generator(self):
        assert self.stream
        for tarinfo in self.tarinfos:
            img_id, img_ext, image = self.parse_tarinfo(tarinfo)
            yield img_id, img_ext, image

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.buffer.close()
        self.tar.close()


class Pages:
    # Manual Pager
    def __init__(self, prefix, max_pages, metadata_gen=None):
        self.prefix = prefix
        self.max_pages = max_pages
        self.metadata_gen = metadata_gen
        self.pages = OrderedDict()

    def page_out(self, block_idx=None):
        if block_idx is None:
            page = self.pages.pop(list(self.pages)[0])
            page.__exit__(None, None, None)
        else:
            if block_idx in self.pages:
                page = self.pages.pop(block_idx)
                page.__exit__(None, None, None)

    def page_in(self, block_idx):
        if block_idx not in self.pages:
            self.pages[block_idx] = TarWrapper(self.prefix, block_idx, stream=False)
            self.pages[block_idx].__enter__()
        if len(self.pages) > self.max_pages:
            self.page_out()

    def get(self, metadata_id, metadata_ext):
        block_idx = str(metadata_id)[-4:]
        if block_idx in self.pages:
            # Load from memory
            img_ext, image = self.pages[block_idx].query(metadata_id)
        else:
            # Load from disk
            img_ext = metadata_ext
            image = requests.get(f"{self.prefix}/{metadata_id}.{metadata_ext}")
        return img_ext, image

    def make_generator(self):
        # Sequential data generator
        for block in range(1000):
            with TarWrapper(self.prefix, str(block).zfill(4), stream=True) as stream:
                generator = stream.make_generator()
            for item in generator:
                img_id, img_ext, image = item
                yield img_id, img_ext, image

# # cfg.pop("train_dataset")
#
# class Encoder:
#     def __init__(self):
#         pass
#
#
# class GroupTagFlattenEncodder(Encoder):
#     def __init__(self):
#         pass
#
#
# class HierarchicalEncodder(Encoder):
#     def __init__(self):
#         pass
#
#
# class TagOnlyEncodder(Encoder):
#     def __init__(self):
#         pass
#
#
# class Sampler:
#     def __init__(self):
#         pass
