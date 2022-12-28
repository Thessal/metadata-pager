from metadata_loader import DmgrTagGroup
from config import CFG

class TestTagGroup:
    def setup_method(self):
        """Setup for tests."""
        self.dmgr = DmgrTagGroup(
            srcdir=CFG["test_group"]["src"], cachedir=CFG["test_group"]["dst"], tag_src=CFG["test_tag"]["src"],)

    def test_init(self):
        print(self.dmgr)
