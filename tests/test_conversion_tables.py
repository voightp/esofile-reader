from unittest import TestCase

from esofile_reader.conversion_tables import energy_table, rate_table


class TestConversionTables(TestCase):
    def test_energy_table(self):
        test_val = 3600000  # J
        test_units = ["Wh", "kWh", "MWh", "kJ", "MJ", "GJ", "Btu", "kBtu", "MBtu"]
        test_results = [1000, 1, 0.001, 3600, 3.6, 0.0036,
                        3412.141156, 3.412141156, 0.003412141156]

        for u, r in zip(test_units, test_results):
            orig, new, ratio = energy_table(u)
            self.assertEqual(new, u)
            self.assertAlmostEqual(test_val / ratio, r, 6)

    def test_energy_table_per_area(self):
        test_val = 3600000  # J/m2
        test_units = ["Wh", "kWh", "MWh", "kJ", "MJ", "GJ", "Btu", "kBtu", "MBtu"]
        test_out_units = ["Wh/m2", "kWh/m2", "MWh/m2", "kJ/m2", "MJ/m2", "GJ/m2",
                          "Btu/ft2", "kBtu/ft2", "MBtu/ft2"]
        test_results = [1000, 1, 0.001, 3600, 3.6, 0.0036,
                        316.99829861, 0.31699829, 0.00031699]

        for u, ou, r in zip(test_units, test_out_units, test_results):
            orig, new, ratio = energy_table(u, per_area=True)
            self.assertEqual(new, ou)
            self.assertAlmostEqual(test_val / ratio, r, 6)

    def test_energy_table_invalid(self):
        self.assertIsNone(energy_table("foo"))

    def test_rate_table(self):
        test_val = 1000000  # W
        test_units = ["kW", "MW", "Btu/h", "kBtu/h", "MBtu/h"]
        test_results = [1000, 1, 3412141.285851795, 3412.1412858, 3.41214128]

        for u, r in zip(test_units, test_results):
            orig, new, ratio = rate_table(u)
            self.assertEqual(new, u)
            self.assertAlmostEqual(test_val / ratio, r, 6)

    def test_rate_table_per_area(self):
        test_val = 1000000  # W/m2
        test_units = ["kW", "MW", "Btu/h", "kBtu/h", "MBtu/h"]
        test_out_units = ["kW/m2", "MW/m2", "Btu/h-ft2", "kBtu/h-ft2", "MBtu/h-ft2"]
        test_results = [1000, 1, 316998.310637, 316.998310637, 0.316998310637]

        for u, ou, r in zip(test_units, test_out_units, test_results):
            orig, new, ratio = rate_table(u, per_area=True)
            self.assertEqual(new, ou)
            self.assertAlmostEqual(test_val / ratio, r, 6)

    def test_rate_table_invalid(self):
        self.assertIsNone(rate_table("foo"))
