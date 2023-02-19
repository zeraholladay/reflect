from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.orm import Session
from pydantic import BaseModel

from sqlalchemy import create_engine


class Engine(object):
    @staticmethod
    def create_engine(url):
        return create_engine(url)


class Tables(BaseModel):
    count: int
    tables: list[str]


class PageOfResults(BaseModel):
    count: int
    rows: list[dict]
    next_url: str

    @staticmethod
    def format_next_url(page_number: int, per_page: int) -> str:
        page_number += 1
        return f"?page_number={page_number}&per_page={per_page}"


class Reflect(object):
    def __init__(self, engine: any, schema: any):
        self.metadata = MetaData(schema=schema)
        self.metadata.reflect(bind=engine)
        self.session = Session(engine, future=True)

    def list_tables(self) -> Tables:
        tables = [table for table in self.metadata.tables]
        return Tables(count=len(tables), tables=tables)

    def paginate(
        self, table_name: str, page_number: int, per_page: int
    ) -> PageOfResults:
        table = self.metadata.tables[table_name]
        statement = select(table).offset(page_number * per_page).limit(per_page)
        results = self.session.execute(statement).all()
        rows = [dict(row._mapping) for row in results]
        return PageOfResults(
            count=len(rows),
            rows=rows,
            next_url=PageOfResults.format_next_url(page_number, per_page),
        )
