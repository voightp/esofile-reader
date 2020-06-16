from unittest import TestCase

from esofile_reader.id_generator import incremental_id_gen


class TestIdGenerators(TestCase):
    def test_incremental_id_gen_checklist(self):
        checklist = [1, 2, 5, 7]
        id_gen = incremental_id_gen(checklist=checklist)
        id1 = next(id_gen)
        id2 = next(id_gen)
        id3 = next(id_gen)
        id4 = next(id_gen)
        id5 = next(id_gen)
        id6 = next(id_gen)

        self.assertEqual(0, id1)
        self.assertEqual(3, id2)
        self.assertEqual(4, id3)
        self.assertEqual(6, id4)
        self.assertEqual(8, id5)
        self.assertEqual(9, id6)

    def test_incremental_id_gen_start(self):
        id_gen = incremental_id_gen(start=20)
        id1 = next(id_gen)
        id2 = next(id_gen)
        id3 = next(id_gen)

        self.assertEqual(20, id1)
        self.assertEqual(21, id2)
        self.assertEqual(22, id3)

    def test_incremental_id_gen_checklist_start(self):
        checklist = [1, 2, 5, 7]
        id_gen = incremental_id_gen(start=3, checklist=checklist)
        id1 = next(id_gen)
        id2 = next(id_gen)
        id3 = next(id_gen)
        id4 = next(id_gen)

        self.assertEqual(3, id1)
        self.assertEqual(4, id2)
        self.assertEqual(6, id3)
        self.assertEqual(8, id4)
