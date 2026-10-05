"""Behavioral regressions for source/asset admission, without Node or Go loads."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("dashboard_input", Path(__file__).with_name("dashboard-input.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class DashboardInputAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dist = self.root / "dist"
        self.dist.mkdir()
        self.index = self.dist / "index.html"
        self.index.write_text("<html>bounded test fixture</html>")
        self.source_file = self.root / "source.ts"
        self.source_file.write_text("export const value = 1;")
        self.manifest = self.root / "dashboard-input.json"
        self.head = "same-checkout-head"
        for name, value in {"DIST": self.dist, "MANIFEST": self.manifest}.items():
            self.enterContext(patch.object(module, name, value))
        self.enterContext(patch.object(module, "identity", self.identity))
        receipt = self.identity()
        receipt["assets_sha256"], receipt["bytes"] = module.assets()
        self.manifest.write_text(json.dumps(receipt))

    def identity(self):
        return {"head": self.head, "source_sha256": module.digest([self.source_file], self.root)}

    def test_same_head_source_mutation_is_rejected(self):
        self.source_file.write_text("export const value = 2;")
        with self.assertRaisesRegex(ValueError, "does not match"):
            module.verify()

    def test_asset_corruption_is_rejected(self):
        self.index.write_text("<html>corrupted fixture</html>")
        with self.assertRaisesRegex(ValueError, "does not match"):
            module.verify()

    def test_other_checkout_head_is_rejected(self):
        self.head = "different-checkout-head"
        with self.assertRaisesRegex(ValueError, "does not match"):
            module.verify()

    def test_unadmitted_input_cannot_be_used(self):
        self.manifest.unlink()
        with self.assertRaises(FileNotFoundError):
            module.verify()

    def test_oversized_input_is_rejected_even_with_matching_receipt(self):
        with patch.object(module, "LIMIT", self.index.stat().st_size - 1):
            with self.assertRaisesRegex(ValueError, "exceeds"):
                module.verify()

    def test_symlink_asset_is_rejected(self):
        (self.dist / "source-link").symlink_to(self.source_file)
        with self.assertRaisesRegex(ValueError, "symlinks"):
            module.verify()

    def test_source_directory_symlinks_are_rejected(self):
        web = self.root / "web"
        external = self.root / "external"
        external.mkdir()
        (external / "main.tsx").write_text("export const value = 1;")
        with patch.object(module, "WEB", web):
            with self.subTest("source root"):
                web.symlink_to(external, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "symlinked source directory"):
                    module.source()
                web.unlink()
            with self.subTest("nested source"):
                web.mkdir()
                (web / "src").symlink_to(external, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "symlinked source directory"):
                    module.source()


if __name__ == "__main__":
    unittest.main()
