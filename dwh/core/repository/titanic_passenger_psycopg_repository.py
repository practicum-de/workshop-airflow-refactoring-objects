from typing import List

from dwh.core.domain.load_passengers_job import ITitanicPassengerRepository
from dwh.core.entities.passenger import Passenger
from dwh.utils.postgres import PgConnect


class TitanicPassengerPsycopgRepository(ITitanicPassengerRepository):
    def __init__(self, db_connection: PgConnect):
        self._db_connection = db_connection

    def save(self, passenger: Passenger):
        with self._db_connection.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.passengers_psycopg (
                        age,
                        fare,
                        name,
                        p_class,
                        parents_children_aboard,
                        gender,
                        siblings_spouses_aboard,
                        survived,
                        update_ts
                    )
                    VALUES (
                        %(age)s,
                        %(fare)s,
                        %(name)s,
                        %(p_class)s,
                        %(parents_children_aboard)s,
                        %(gender)s,
                        %(siblings_spouses_aboard)s,
                        %(survived)s,
                        NOW()
                    )
                    ON CONFLICT (name) DO NOTHING;

                """,
                    passenger.dict(),
                )

    def save_many(self, passengers: List[Passenger]):
        with self._db_connection.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.passengers_psycopg (
                        age,
                        fare,
                        name,
                        p_class,
                        parents_children_aboard,
                        gender,
                        siblings_spouses_aboard,
                        survived,
                        update_ts
                    )
                    VALUES (
                        %(age)s,
                        %(fare)s,
                        %(name)s,
                        %(p_class)s,
                        %(parents_children_aboard)s,
                        %(gender)s,
                        %(siblings_spouses_aboard)s,
                        %(survived)s,
                        NOW()
                    )
                    ON CONFLICT (name) DO NOTHING;

                """,
                    [p.model_dump() for p in passengers],
                )
