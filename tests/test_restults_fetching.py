import unittest
from esofile_reader import EsoFile, DiffFile, TotalsFile, Variable
from esofile_reader.constants import *


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ef = EsoFile("../tests/eso_files/eplusout_all_intervals.eso")

    def test_basic_standard_results(self):
        v = Variable(None, None, None, None)
        r = self.ef.get_results(v)
        self.assertEqual(r.shape, (17521, 114))
        self.assertEqual(r.columns.names, ['key', 'variable', 'units'])
        self.assertEqual(r.index.names, ["file", "timestamp"])

        intervals = [TS, H, D, M, A, RP]
        shapes = [(17520, 9), (8760, 9), (365, 9), (12, 9), (1, 9), (1, 9)]

        for interval, shape in zip(intervals, shapes):
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK2:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK4:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK1:ZONE1", "Zone Air Relative Humidity", "%"),
                Variable(interval, "CHILLER", "Chiller Electric Power", "W"),
                Variable(interval, "CHILLER", "Chiller Electric Energy", "J"),
                Variable(interval, "non", "existing", "variable")
            ]
            r = self.ef.get_results(variables)
            self.assertEqual(r.shape, shape)


        if __name__ == '__main__':
            unittest.main()
