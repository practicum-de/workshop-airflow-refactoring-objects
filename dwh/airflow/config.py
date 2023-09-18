from airflow.hooks.base import BaseHook

from dwh.utils.postgres import ChConnect, PgConnect


class ConfigConst:
    TITANIC_RAW_REPOSITORY_PG_CONN = "TITANIC_RAW_REPOSITORY_PG"
    TITANIC_RAW_REPOSITORY_CH_CONN = "TITANIC_RAW_REPOSITORY_CH"


class ConnectionBuilder:
    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "disable"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host), str(conn.port), str(conn.schema), str(conn.login), str(conn.password), sslmode)

        return pg

    @staticmethod
    def ch_conn(conn_id: str) -> ChConnect:
        # conn = BaseHook.get_connection(conn_id)

        # ch = ChConnect(str(conn.host), str(conn.port), str(conn.schema), str(conn.login), str(conn.password))
        ch = ChConnect("host.docker.internal", "8123", "de", "default", "")

        return ch


class AppConfig:
    @staticmethod
    def titanic_raw_repository() -> PgConnect:
        return ConnectionBuilder.pg_conn(ConfigConst.TITANIC_RAW_REPOSITORY_PG_CONN)

    @staticmethod
    def titanic_ch_repository() -> ChConnect:
        return ConnectionBuilder.ch_conn(ConfigConst.TITANIC_RAW_REPOSITORY_CH_CONN)

    @staticmethod
    def titanic_api_url() -> str:
        return "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
