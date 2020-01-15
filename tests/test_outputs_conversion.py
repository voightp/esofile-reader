import unittest
import pandas as pd
from esofile_reader.outputs.convertor import *


class TestOutputsConversion(unittest.TestCase):
    def test_apply_conversion(self):
        columns = pd.MultiIndex.from_tuples([(1, "bar"), (2, "baz")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [2])
        print(out)

    def test_convert_units(self):
        pass

    def test_update_multiindex(self):
        pass

    def test_verify_units(self):
        pass

    def test_get_n_steps(self):
        pass

    def test_rate_to_energy(self):
        pass
