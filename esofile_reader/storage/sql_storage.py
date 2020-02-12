import contextlib
import traceback
from typing import List

import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, select, Boolean, exc

from esofile_reader.constants import *
from esofile_reader.data.sql_data import SQLData
from esofile_reader.database_file import DatabaseFile
from esofile_reader.storage.base_storage import BaseStorage
from esofile_reader.storage.sql_functions import create_results_table, \
    create_datetime_table, merge_df_values, create_value_insert, \
    create_n_days_table, create_day_table
from esofile_reader.utils.mini_classes import ResultsFile
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.utils import profile


class SQLStorage(BaseStorage):
    FILE_TABLE = "result-files"
    SEPARATOR = "\t"

    def __init__(self, path=None):
        super().__init__()
        self.files = {}
        self.engine, self.metadata = self.set_up_db(path)

    @property
    def file_table(self):
        return self.metadata.tables[self.FILE_TABLE]

    @profile
    def set_up_db(self, path=None, echo=False):
        path = path if path else ":memory:"

        engine = create_engine(f"sqlite:///{path}", echo=echo)
        metadata = MetaData(bind=engine)
        metadata.reflect()

        if self.FILE_TABLE not in metadata.tables.keys():
            file = Table(
                self.FILE_TABLE,
                metadata,
                Column("id", Integer, autoincrement=True, primary_key=True),
                Column("file_path", String(120)),
                Column("file_name", String(50)),
                Column("file_created", DateTime),
                Column("totals", Boolean),
                Column("range_outputs_table", String(50)),
                Column("timestep_outputs_table", String(50)),
                Column("hourly_outputs_table", String(50)),
                Column("daily_outputs_table", String(50)),
                Column("monthly_outputs_table", String(50)),
                Column("annual_outputs_table", String(50)),
                Column("runperiod_outputs_table", String(50)),
                Column("timestep_dt_table", String(50)),
                Column("hourly_dt_table", String(50)),
                Column("daily_dt_table", String(50)),
                Column("monthly_dt_table", String(50)),
                Column("annual_dt_table", String(50)),
                Column("runperiod_dt_table", String(50)),
                Column("timestep_day_table", String(50)),
                Column("hourly_day_table", String(50)),
                Column("daily_day_table", String(50)),
                Column("monthly_n_days_table", String(50)),
                Column("annual_n_days_table", String(50)),
                Column("runperiod_n_days_table", String(50)),
            )

            with contextlib.suppress(exc.InvalidRequestError, exc.OperationalError):
                file.create()

        return engine, metadata

    @profile
    def store_file(self, results_file: ResultsFile, totals: bool = False) -> int:
        if not self.metadata or not self.engine:
            raise AttributeError(
                f"Cannot store file into database."
                f"\nIt's required to call '{self.__name__}.set_up_db(path)'"
                f" to create a database engine and metadata first."
            )

        f = self.metadata.tables[self.FILE_TABLE]
        ins = f.insert().values(
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            file_created=results_file.file_created,
            totals=totals,
        )

        # insert new file data
        with self.engine.connect() as conn:
            id_ = conn.execute(ins).inserted_primary_key[0]

            for interval in results_file.available_intervals:
                f_upd = {}

                outputs = results_file.data.tables[interval]
                # create result table
                results_table = create_results_table(self.metadata, id_, interval)
                f_upd[f"{interval}_outputs_table"] = results_table.name

                # store numeric values
                df = results_file.data.get_all_results(interval)
                sr = merge_df_values(df, self.SEPARATOR)

                ins = []
                for index, values in sr.iteritems():
                    ins.append(
                        {
                            "id": index[0],
                            "interval": index[1],
                            "key": index[2],
                            "variable": index[3],
                            "units": index[4],
                            "str_values": values,
                        }
                    )
                conn.execute(results_table.insert(), ins)

                # create index table
                if isinstance(outputs.index, pd.DatetimeIndex):
                    index_table = create_datetime_table(self.metadata, id_, interval)
                    conn.execute(
                        index_table.insert(), create_value_insert(outputs.index)
                    )
                    f_upd[f"{interval}_dt_table"] = index_table.name

                # store 'n days' data
                if N_DAYS_COLUMN in outputs.columns:
                    n_days_table = create_n_days_table(self.metadata, id_, interval)
                    conn.execute(
                        n_days_table.insert(),
                        create_value_insert(outputs[N_DAYS_COLUMN]),
                    )
                    f_upd[f"{interval}_n_days_table"] = n_days_table.name

                # store 'day of week' data
                if DAY_COLUMN in outputs.columns:
                    day_table = create_day_table(self.metadata, id_, interval)
                    conn.execute(
                        day_table.insert(), create_value_insert(outputs[DAY_COLUMN])
                    )
                    f_upd[f"{interval}_day_table"] = day_table.name

                conn.execute(f.update().where(f.c.id == id_).values(f_upd))

                db_file = DatabaseFile(
                    id_,
                    results_file.file_name,
                    SQLData(id_, self),
                    results_file.file_created,
                    totals=totals,
                    search_tree=results_file._search_tree,
                    file_path=results_file.file_path,
                )

        # store file in a class attribute
        self.files[id_] = db_file

        return id_

    @profile
    def delete_file(self, id_: int) -> None:
        files = self.metadata.tables[self.FILE_TABLE]

        with self.engine.connect() as conn:
            res = conn.execute(files.select().where(files.c.id == id_)).first()
            if res:
                # remove tables based on file reference
                for table_name in res:
                    try:
                        self.metadata.tables[table_name].drop()
                    except KeyError:
                        pass

                # remove result file
                conn.execute(files.delete().where(files.c.id == id_))

            else:
                raise KeyError(f"Cannot delete file id '{id_}'.")

        # reset metadata to reflect changes
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect()

        del self.files[id_]

    @profile
    def load_all_files(self) -> List[DatabaseFile]:
        files = self.metadata.tables[self.FILE_TABLE]

        with self.engine.connect() as conn:
            res = conn.execute(select([files.c.id]))
            ids = [r[0] for r in res]

            for id_ in reversed(ids):
                res = conn.execute(
                    select(
                        [
                            files.c.id,
                            files.c.file_name,
                            files.c.file_created,
                            files.c.file_path,
                            files.c.totals,
                        ]
                    ).where(files.c.id == id_)
                ).first()
            if res:
                data = SQLData(res[0], self)

                tree = Tree()
                tree.populate_tree(data.get_all_variables_dct())

                db_file = DatabaseFile(
                    res[0],
                    res[1],
                    data,
                    res[2],
                    file_path=res[3],
                    search_tree=tree,
                    totals=res[4],
                )

                self.files[id_] = db_file

            else:
                raise KeyError(
                    f"Cannot load file id '{id_}'.\n" f"{traceback.format_exc()}"
                )

        return ids

    def get_all_file_names(self):
        files = self.metadata.tables[self.FILE_TABLE]

        with self.engine.connect() as conn:
            res = conn.execute(select([files.c.file_name]))
            names = [r[0] for r in res]

        return names
