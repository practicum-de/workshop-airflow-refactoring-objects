import contextlib
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from dwh.utils.postgres import PgConnect


class SessionBuilder:
    def __init__(self, db_connection_string: PgConnect):
        self.db_url = db_connection_string.sqlalchemy_url()

    @contextlib.contextmanager
    def session(self, expire_on_commit: bool = True) -> Generator[Session, None, None]:
        engine = create_engine(self.db_url)
        sm = sessionmaker(engine, expire_on_commit=expire_on_commit)

        with sm() as session:
            with session.begin():
                yield session
                session.commit()

        engine.dispose()
