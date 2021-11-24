import os
from operator import itemgetter
from typing import List

from psycopg2.extras import execute_values

from common.hasura_exception import HasuraActionException
from recommendation.core.dim_vector import DimVector, NamedDimVector

script_dir = os.path.dirname(__file__)

with open(os.path.join(script_dir, "sql/collection_industries.sql")
          ) as _collection_industry_vector_query_file:
    _collection_industry_vector_query = _collection_industry_vector_query_file.read(
    )

with open(os.path.join(
        script_dir,
        "sql/ticker_categories.sql")) as _ticker_category_vector_query_file:
    _ticker_category_vector_query = _ticker_category_vector_query_file.read()

with open(os.path.join(
        script_dir,
        "sql/ticker_industries.sql")) as _ticker_industry_vector_query_file:
    _ticker_industry_vector_query = _ticker_industry_vector_query_file.read()

with open(os.path.join(script_dir, "sql/ticker_categories_industries.sql")
          ) as _ticker_categories_industries_query_file:
    _ticker_categories_industries_query = _ticker_categories_industries_query_file.read(
    )

with open(os.path.join(script_dir, "sql/profile_categories.sql")
          ) as _profile_category_vector_query_file:
    _profile_category_vector_query = _profile_category_vector_query_file.read()

with open(os.path.join(script_dir, "sql/profile_industries.sql")
          ) as _profile_industry_vector_query_file:
    _profile_industry_vector_query = _profile_industry_vector_query_file.read()

with open(os.path.join(
        script_dir,
        "sql/industry_frequencies.sql")) as _industry_frequencies_query_file:
    _industry_frequencies_query = _industry_frequencies_query_file.read()

with open(os.path.join(
        script_dir,
        "sql/collection_corpus_size.sql")) as _corpus_size_query_file:
    _corpus_size_query = _corpus_size_query_file.read()


def read_categories_risks(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        "SELECT id::varchar, risk_score from public.categories WHERE risk_score IS NOT NULL;"
    )
    return dict(cursor.fetchall())


def read_ticker_category_vectors(db_conn, symbols: List[str]) -> List[NamedDimVector]:
    return _get_ticker_vectors(db_conn, _ticker_category_vector_query, symbols)


def read_ticker_industry_vectors(db_conn, symbols: List[str]) -> List[NamedDimVector]:
    return _get_ticker_vectors(db_conn, _ticker_industry_vector_query, symbols)


def read_ticker_category_vector(db_conn, symbol: str) -> NamedDimVector:
    return _get_ticker_vectors(db_conn, _ticker_category_vector_query, [symbol])[0]


def read_ticker_industry_vector(db_conn, symbol: str) -> NamedDimVector:
    return _get_ticker_vectors(db_conn, _ticker_industry_vector_query, [symbol])[0]


def read_profile_category_vector(db_conn, profile_id):
    return _get_profile_vector(db_conn, _profile_category_vector_query, profile_id)


def read_profile_industry_vector(db_conn, profile_id):
    return _get_profile_vector(db_conn, _profile_industry_vector_query, profile_id)


def _get_profile_vector(db_conn, profile_vector_query, profile_id):
    vectors = _query_vectors(db_conn, profile_vector_query,
                             {"profile_id": profile_id})
    if not vectors:
        raise HasuraActionException(400, f"Profile {profile_id} not found")

    return vectors[0]


def _get_ticker_vectors(db_conn, ticker_vector_query, symbols) -> List[NamedDimVector]:
    vectors = _query_vectors(db_conn, ticker_vector_query, {"symbols": tuple(symbols)})
    if not vectors:
        raise HasuraActionException(400, f"None of symbols {symbols} were found")

    return vectors


def read_all_ticker_category_and_industry_vectors(db_conn) -> list[(DimVector, DimVector)]:
    cursor = db_conn.cursor()
    cursor.execute(_ticker_categories_industries_query)

    return [(NamedDimVector(row[0], row[1]), NamedDimVector(row[0], row[2]))
            for row in cursor.fetchall()]


def read_all_collection_industry_vectors(db_conn):
    return _query_vectors(db_conn, _collection_industry_vector_query)


def _query_vectors(db_conn, query, variables=None) -> List[NamedDimVector]:
    cursor = db_conn.cursor()
    cursor.execute(query, variables)

    vectors = []
    for row in cursor.fetchall():
        vectors.append(NamedDimVector(row[0], row[1]))

    return vectors


def read_industry_corpus_size(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(_corpus_size_query)
    corpus_size = cursor.fetchone()[0]
    return corpus_size


def read_industry_frequencies(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(_industry_frequencies_query)
    document_frequencies = dict(cursor.fetchall())
    return document_frequencies


def read_collection_tickers(db_conn, profile_id: str, collection_id: str) -> List[str]:
    with db_conn.cursor() as cursor:
        cursor.execute(
            """SELECT symbol FROM public.profile_ticker_collections 
            WHERE (profile_id=%(profile_id)s OR profile_id IS NULL) AND collection_id=%(collection_id)s""",
            {
                "profile_id": profile_id,
                "collection_id": collection_id
            })

        return list(map(itemgetter(0), cursor.fetchall()))


def is_collection_enabled(db_conn, profile_id: str, collection_id: str) -> bool:
    with db_conn.cursor() as cursor:
        cursor.execute(
            """SELECT enabled FROM public.profile_collections
            WHERE (profile_id=%(profile_id)s OR profile_id IS NULL) AND id=%(collection_id)s""",
            {
                "profile_id": profile_id,
                "collection_id": collection_id
            })

        row = cursor.fetchone()

    return row and row[0] == "1"


def update_personalized_collection(db_conn, profile_id, collection_id, ticker_list):
    with db_conn.cursor() as cursor:
        cursor.execute(
            """SELECT profile_id, collection_id FROM app.personalized_collection_sizes 
             WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s FOR UPDATE""",
            {
                "profile_id": profile_id,
                "collection_id": collection_id
            })

        if not cursor.fetchone() is None:
            cursor.execute(
                """DELETE FROM app.personalized_ticker_collections 
                WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s""",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id
                })

            cursor.execute(
                """UPDATE app.personalized_collection_sizes SET size = %(size)s
                WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s""",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id,
                    "size": len(ticker_list)
                })
        else:
            cursor.execute(
                "INSERT INTO app.personalized_collection_sizes(profile_id, collection_id, size) "
                "VALUES (%(profile_id)s, %(collection_id)s, %(size)s)", {
                    "profile_id": profile_id,
                    "collection_id": collection_id,
                    "size": len(ticker_list)
                })

        execute_values(
            cursor,
            "INSERT INTO app.personalized_ticker_collections(profile_id, collection_id, symbol) VALUES %s",
            [(profile_id, collection_id, symbol)
             for symbol in ticker_list])
