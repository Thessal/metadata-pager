import group_dmgr
from config import CFG

class TestTagGroup:
    def setup_method(self):
        """Setup for tests."""
        self.dmgr = group_dmgr.DmgrTagGroup(srcdir=CFG["test_group"]["src"], cachedir=CFG["test_group"]["dst"])

    def test_init(self):
        print(self.dmgr)