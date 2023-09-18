from contextlib import contextmanager
from typing import Generator, Optional

import psycopg


class PgConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: Optional[str] = None) -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        ssl = f"sslmode={self.sslmode}" if self.sslmode else ""

        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            {ssl}
        """.format(
            host=self.host, port=self.port, db_name=self.db_name, user=self.user, pw=self.pw, ssl=ssl
        )

    def sqlalchemy_url(self) -> str:
        engine = "postgresql+psycopg2"

        conn_str = "{engine}://{login}:{password}@{host}:{port}/{schema}".format(
            engine=engine,
            login=self.user,
            password=self.pw,
            host=self.host,
            port=self.port,
            schema=self.db_name,
        )

        return conn_str

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = psycopg.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
