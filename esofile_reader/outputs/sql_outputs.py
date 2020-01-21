from uuid import uuid1
from esofile_reader import EsoFile
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, Boolean, Sequence, Text, inspect, select
import pandas as pd

# engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///test.db', echo=True)
metadata = MetaData(engine)

file = Table(
    "resultfiles", metadata,
    Column("id", Integer, Sequence('db_id_seq'), primary_key=True),
    Column("file_path", String(120)),
    Column("file_name", String(50)),
    Column("file_timestamp", DateTime),
    Column("table_timestep", String(50)),
    Column("table_hourly", String(50)),
    Column("table_daily", String(50)),
    Column("table_monthly", String(50)),
    Column("table_annual", String(50)),
    Column("table_runperiod", String(50))
)

timestamps = Table(
    "timestamps", metadata,
    Column("file_id", Integer, Sequence('db_id_seq'), foreign_key=True),
)

file.create()


def merge_df_values(df: pd.DataFrame, separator: str = " ") -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df


def results_table_generator(file_id, interval, df):
    name = f"outputs-{interval}-{file_id}]"
    table = Table(
        name, metadata,
        Column("id", Integer, primary_key=True, index=True),
        Column("interval", String(50)),
        Column("key", String(50)),
        Column("variable", String(50)),
        Column("units", String(50)),
        Column("values", Text)
    )
    table.create()

    sr = merge_df_values(df, "\t")
    ins = []
    for index, values in sr.iteritems():
        ins.append(
            {
                "id": index[0],
                "interval": index[1],
                "key": index[2],
                "variable": index[3],
                "units": index[4],
                "values": values
            }
        )

    with engine.connect() as conn:
        conn.execute(table.insert(), ins)
    return name


def timestamps_table_generator(file_id, interval, date_range):
    name = f"index-{interval}-{file_id}]"
    table = Table(
        name, metadata,
        Column("timestamps", Text),
    )
    table.create()
    return name


def process_file(id_, result_file):
    ins = file.insert().values(
        id=id_,
        file_path=result_file.file_path,
        file_name=result_file.file_name,
        file_timestamp=result_file.created
    )
    with engine.connect() as conn:
        conn.execute(ins)

    for interval, df in result_file._outputs.items():
        results_table_generator(id_, interval, df.only_numeric)
        timestamps_table_generator(id_, interval, df.index)


if __name__ == "__main__":
    ef = EsoFile(r"C:\Users\vojtechp1\PycharmProjects\eso_reader\tests\eso_files\eplusout1.eso",
                 report_progress=False)
    process_file(1, ef)

    inspector = inspect(engine)

    # Get table information
    print(inspector.get_columns("resultfiles"))
