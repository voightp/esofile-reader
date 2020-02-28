import unittest
from tests import ROOT
import os
from esofile_reader.eso_file import EsoFile
from esofile_reader.utils.exceptions import MultiEnvFileRequired


class TestMultipleEnvs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(ROOT, "eso_files/multiple_environments.eso")
        cls.efs = EsoFile.process_multi_env_file(path)

    def test_file_names(self):
        names = [
            "multiple_environments",
            "multiple_environments - CAMP MABRY ANN HUM_N 99.6% CONDNS DP=>MCDB",
            "multiple_environments - CAMP MABRY ANN HTG WIND 99.6% CONDNS WS=>MCDB",
            "multiple_environments - CAMP MABRY ANN HTG 99.6% CONDNS DB",
            "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS WB=>MDB",
            "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS ENTH=>MDB",
            "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS DP=>MDB",
            "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS DB=>MWB",
        ]
        for ef, nm in zip(self.efs, names):
            self.assertEqual(ef.file_name, nm)

    def test_complete(self):
        for ef in self.efs:
            self.assertTrue(ef.complete)

    def test_tree(self):
        trees = [ef.search_tree.str_tree() for ef in self.efs]
        self.assertEqual(len(set(trees)), 1)

    def test_multienv_file_required(self):
        with self.assertRaises(MultiEnvFileRequired):
            EsoFile(os.path.join(ROOT, "eso_files/multiple_environments.eso"))


if __name__ == '__main__':
    unittest.main()
