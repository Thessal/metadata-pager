from metadata_loader import DmgrTagGroup, DmgrTags, TagLoader
from config import CFG

class TestTag:
    def setup_method(self):
        """Setup for tests."""
        self.tags_info = DmgrTags(srcdir=CFG["test_tag"]["src"], cachedir=CFG["test_tag"]["dst"])
        self.groups_info = DmgrTagGroup(
            srcdir=CFG["test_group"]["src"], cachedir=CFG["test_group"]["dst"], tag_src=CFG["test_tag"]["src"], )
        self.tag_loader = TagLoader(
            self.groups_info, self.tags_info,
            srcdir=CFG["test_loader"]["src"],
            cachedir=CFG["test_loader"]["dst"],
            debug=True,
        )

    def test_init(self):
        print(self.dmgr)
