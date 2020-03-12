import os
from esofile_reader import EsoFile

ROOT = os.path.dirname(os.path.abspath(__file__))
EF1 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))
EF2 = EsoFile(os.path.join(ROOT, "eso_files/eplusout2.eso"))
EF_ALL_INTERVALS = EsoFile(os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"))
