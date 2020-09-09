import logging
import os
from pathlib import Path
from esofile_reader import EsoFile, logger

logger.setLevel(logging.ERROR)

ROOT = os.path.dirname(os.path.abspath(__file__))
EF1 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))
EF2 = EsoFile(os.path.join(ROOT, "eso_files/eplusout2.eso"))
EF_ALL_INTERVALS = EsoFile(os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"))

EF1_PEAKS = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"), ignore_peaks=False)
EF2_PEAKS = EsoFile(os.path.join(ROOT, "eso_files/eplusout2.eso"), ignore_peaks=False)
EF_ALL_INTERVALS_PEAKS = EsoFile(
    os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"), ignore_peaks=False
)
