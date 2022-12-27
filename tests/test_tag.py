import tag_dmgr
from config import CFG


class TestTag:
    def setup_method(self):
        """Setup for tests."""
        self.dmgr = tag_dmgr.DmgrTag(srcdir=CFG["test_tag"]["src"], cachedir=CFG["test_tag"]["dst"])

    def test_init(self):
        print(self.dmgr)
