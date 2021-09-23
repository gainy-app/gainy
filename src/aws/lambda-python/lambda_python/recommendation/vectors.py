from math import sqrt
from typing import List


class DimVector:

    def __init__(self, coordinates):
        self.coordinates = coordinates if coordinates else {}

    @staticmethod
    def norm(vector):
        result = 0.0
        for coordinate in vector.coordinates.values():
            result += coordinate * coordinate

        return sqrt(result)

    @staticmethod
    def dot_product(first, second):
        result = 0.0
        for dimension in set(first.coordinates.keys()).union(second.coordinates.keys()):
            result += first.coordinates.get(dimension, 0.0) * second.coordinates.get(dimension, 0.0)

        return result

    def cosine_similarity(self, other):
        self_norm = DimVector.norm(self)
        other_norm = DimVector.norm(other)

        if self_norm == 0.0 or other_norm == 0.0:
            return 0.0

        return DimVector.dot_product(self, other) / self_norm / other_norm


def sort_vectors_by(vectors, similarity, asc=True):
    vectors.sort(key=similarity, reverse=not asc)
    return vectors


# TODO: find a better place
class NamedDimVector(DimVector):

    def __init__(self, name, coordinates):
        super().__init__(coordinates)
        self.name = name


def query_vectors(db_conn, query) -> List[NamedDimVector]:
    cursor = db_conn.cursor()
    cursor.execute(query)

    vectors = []
    for row in cursor.fetchall():
        vectors.append(NamedDimVector(row[0], row[1]))

    return vectors


