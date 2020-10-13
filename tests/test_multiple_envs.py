from esofile_reader.exceptions import MultiEnvFileRequired
from tests.session_fixtures import *

MULTIPLE_ENVS_PATH = Path(ROOT_PATH, "eso_files", "multiple_environments.eso")


@pytest.fixture(scope="module")
def eso_files():
    return EsoFile.from_multi_env_eso_file(MULTIPLE_ENVS_PATH)


def test_file_names(eso_files):
    test_names = [
        "multiple_environments",
        "multiple_environments - CAMP MABRY ANN HUM_N 99.6% CONDNS DP=>MCDB",
        "multiple_environments - CAMP MABRY ANN HTG WIND 99.6% CONDNS WS=>MCDB",
        "multiple_environments - CAMP MABRY ANN HTG 99.6% CONDNS DB",
        "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS WB=>MDB",
        "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS ENTH=>MDB",
        "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS DP=>MDB",
        "multiple_environments - CAMP MABRY ANN CLG .4% CONDNS DB=>MWB",
    ]
    names = [ef.file_name for ef in eso_files]
    assert names == test_names


def test_complete(eso_files):
    for ef in eso_files:
        assert ef.complete


def test_tree(eso_files):
    trees = [ef.search_tree.__repr__() for ef in eso_files]
    assert len(set(trees)) == 1


def test_ids(eso_files):
    all_ids = [ef.tables.get_all_variable_ids() for ef in eso_files]
    first_file_ids = all_ids[0]
    assert all(map(lambda x: x == first_file_ids, all_ids[1:]))


def test_multienv_file_required():
    with pytest.raises(MultiEnvFileRequired):
        EsoFile(MULTIPLE_ENVS_PATH)
