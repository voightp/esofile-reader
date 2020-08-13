import contextlib
from datetime import datetime
from typing import List

import pandas as pd
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    MetaData,
    create_engine,
    DateTime,
    select,
    exc,
)

from esofile_reader.base_file import BaseFile
from esofile_reader.constants import *
from esofile_reader.mini_classes import ResultsFileType
from esofile_reader.search_tree import Tree
from esofile_reader.storages.base_storage import BaseStorage
from esofile_reader.storages.sql_functions import (
    create_results_table,
    create_datetime_table,
    merge_df_values,
    create_value_insert,
    create_special_table,
)
from esofile_reader.tables.sql_tables import SQLTables


class SQLFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from processed ResultsFileType.

    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file_path: str
        A file path of the reference file.
    file_name: str
        File name of the reference file.
    sql_tables: SQLTables
        Processed SQL data instance.
    file_created: datetime
        A creation datetime of the reference file.
    search_tree: Tree
        Search tree instance.
    file_type: str
        Original file class name..

    Notes
    -----
    Reference file must be complete!

    """

    def __init__(
        self,
        id_: int,
        file_path: str,
        file_name: str,
        sql_tables: SQLTables,
        file_created: datetime,
        search_tree: Tree,
        file_type: str,
    ):
        super().__init__(file_path, file_name, file_created, sql_tables, search_tree, file_type)
        self.id_ = id_

    def rename(self, name: str) -> None:
        self.file_name = name
        self.tables.update_file_name(name)


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
                Column("file_type", String(50)),
                Column("numeric_tables", String),
                Column("datetime_tables", String),
                Column("special_tables", String),
            )

            with contextlib.suppress(exc.InvalidRequestError, exc.OperationalError):
                file.create()

        return engine, metadata

    def store_file(self, results_file: ResultsFileType) -> int:
        if not self.metadata or not self.engine:
            raise AttributeError(
                f"Cannot store file into database."
                f"\nIt's required to call '{self.__name__}.set_up_db(path)'"
                f" to create a database engine and metadata first."
            )

        file_table = self.metadata.tables[self.FILE_TABLE]
        ins = file_table.insert().values(
            file_path=str(results_file.file_path),
            file_name=results_file.file_name,
            file_created=results_file.file_created,
            file_type=results_file.file_type,
        )

        # insert new file data
        with self.engine.connect() as conn:
            id_ = conn.execute(ins).inserted_primary_key[0]
            file_input = {"numeric_tables": [], "datetime_tables": [], "special_tables": []}
            for table in results_file.table_names:
                df = results_file.tables[table]
                is_simple = results_file.is_header_simple(table)
                results_table = create_results_table(self.metadata, id_, table, is_simple)
                file_input["numeric_tables"].append(results_table.name)
                # store numeric values
                df_numeric = df.loc[:, df.columns.get_level_values(ID_LEVEL) != SPECIAL]
                df_special = df.loc[:, df.columns.get_level_values(ID_LEVEL) == SPECIAL]
                sr = merge_df_values(df_numeric, self.SEPARATOR)
                ins = []
                for index, values in sr.iteritems():
                    if is_simple:
                        ins.append(
                            {
                                ID_LEVEL: index[0],
                                TABLE_LEVEL: index[1],
                                KEY_LEVEL: index[2],
                                UNITS_LEVEL: index[3],
                                STR_VALUES: values,
                            }
                        )
                    else:
                        ins.append(
                            {
                                ID_LEVEL: index[0],
                                TABLE_LEVEL: index[1],
                                KEY_LEVEL: index[2],
                                TYPE_LEVEL: index[3],
                                UNITS_LEVEL: index[4],
                                STR_VALUES: values,
                            }
                        )
                conn.execute(results_table.insert(), ins)
                # create index table
                if isinstance(df.index, pd.DatetimeIndex):
                    index_table = create_datetime_table(self.metadata, id_, table)
                    conn.execute(index_table.insert(), create_value_insert(df.index))
                    file_input["datetime_tables"].append(index_table.name)
                if not df_special.empty:
                    key_index = df_special.columns.names.index(KEY_LEVEL)
                    for column in df_special:
                        sr = df_special[column]
                        column_type = Integer if pd.api.types.is_integer_dtype(sr) else String
                        special_table = create_special_table(
                            self.metadata, id_, table, column[key_index], column_type
                        )
                        conn.execute(special_table.insert(), create_value_insert(sr.tolist()))
                        file_input["special_tables"].append(special_table.name)
            # all table names are stored as a tab separated string
            file_input = {k: f"{self.SEPARATOR}".join(v) for k, v in file_input.items()}
            conn.execute(file_table.update().where(file_table.c.id == id_).values(file_input))
            db_file = SQLFile(
                id_,
                file_path=results_file.file_path,
                file_name=results_file.file_name,
                sql_tables=SQLTables(id_, self),
                file_created=results_file.file_created,
                search_tree=results_file.search_tree,
                file_type=results_file.file_type,
            )
            self.files[id_] = db_file
        return id_

    def delete_file(self, id_: int) -> None:
        file_table = self.metadata.tables[self.FILE_TABLE]
        with self.engine.connect() as conn:
            columns = [
                file_table.c.numeric_tables,
                file_table.c.special_tables,
                file_table.c.datetime_tables,
            ]
            res = conn.execute(select(columns).where(file_table.c.id == id_)).first()
            if res:
                # remove tables based on file reference
                for table_names in res:
                    for name in table_names.split(self.SEPARATOR):
                        self.metadata.tables[name].drop()

                # remove result file
                conn.execute(file_table.delete().where(file_table.c.id == id_))
            else:
                raise KeyError(f"File {id_} not found in database.")

        # reset metadata to reflect changes
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect()
        del self.files[id_]

    def load_all_files(self) -> List[SQLFile]:
        file_table = self.metadata.tables[self.FILE_TABLE]
        with self.engine.connect() as conn:
            res = conn.execute(select([file_table.c.id]))
            ids = [r[0] for r in res]
            for id_ in reversed(ids):
                res = conn.execute(
                    select(
                        [
                            file_table.c.id,
                            file_table.c.file_path,
                            file_table.c.file_name,
                            file_table.c.file_created,
                            file_table.c.file_type,
                        ]
                    ).where(file_table.c.id == id_)
                ).first()
            tables = SQLTables(res[0], self)
            tree = Tree()
            tree.populate_tree(tables.get_all_variables_dct())
            db_file = SQLFile(
                id_=res[0],
                file_path=res[1],
                file_name=res[2],
                sql_tables=tables,
                file_created=res[3],
                search_tree=tree,
                file_type=res[4],
            )
            self.files[id_] = db_file
        return ids

    def get_all_file_names(self):
        files = self.metadata.tables[self.FILE_TABLE]
        with self.engine.connect() as conn:
            res = conn.execute(select([files.c.file_name]))
            names = [r[0] for r in res]
        return names
