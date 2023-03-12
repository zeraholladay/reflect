from datetime import date, datetime
from typing import Dict, List

from pydantic import BaseModel, parse_obj_as
from sqlalchemy import Engine, MetaData, select
from sqlalchemy.orm import Session


class Tables(BaseModel):
    count: int
    tables: list[str]


class PageOfResults(BaseModel):
    count: int
    results: list[dict]
    next_url: str


def _format_next_url(page_number: int, per_page: int) -> str:
    page_number += 1
    return f"?page_number={page_number}&per_page={per_page}"


class Reflect(object):
    model = Dict

    def __init__(self, engine: Engine, **kwargs):
        self.metadata = MetaData(**kwargs)
        self.metadata.reflect(bind=engine)
        self.engine = engine

    def list_tables(self) -> Tables:
        tables = [table for table in self.metadata.tables]
        return Tables(count=len(tables), tables=tables)

    def format_object(self, obj) -> any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()  # ISO 8601
        else:
            return obj

    def to_dict(self, row, column_names=[]) -> dict:
        return {name: self.format_object(getattr(row, name)) for name in column_names}

    def parse_obj_as(self, db_results, column_names=[]):
        return parse_obj_as(
            List[self.model],
            [self.to_dict(row, column_names=column_names) for row in db_results],
        )

    def build_select(self, table: str, column_names=[]):
        return select(table)

    def paginate(
        self, table_name: str, page_number=0, per_page=100, column_names=[]
    ) -> PageOfResults:
        table = self.metadata.tables[table_name]

        column_names = (
            [column.name for column in table.columns]
            if not column_names
            else column_names
        )

        with Session(self.engine) as session:
            db_results = session.execute(
                self.build_select(table, column_names=[])
                .offset(page_number * per_page)
                .limit(per_page)
            ).all()

        results = self.parse_obj_as(db_results, column_names=column_names)

        # FIXME net_url should probably be null if there are no additional pages
        return PageOfResults(
            count=len(results),
            results=results,
            next_url=_format_next_url(page_number, per_page),
        )
