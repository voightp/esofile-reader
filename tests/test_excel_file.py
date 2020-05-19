import unittest
from pathlib import Path

from esofile_reader.excel_file import ExcelFile
from tests import ROOT


class TestExcelFile(unittest.TestCase):
    def test_populate_content(self):
        path = Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx")
        ef = ExcelFile(path)
