from math import sqrt


class SparseVector:

    def __init__(self, id, coordinates):
        self.id = id
        self.coordinates = coordinates

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
        self_norm = SparseVector.norm(self)
        other_norm = self.other_norm(other)

        if self_norm == 0.0 or other_norm == 0.0:
            return 0.0

        return SparseVector.dot_product(self, other) / self_norm / other_norm

    def other_norm(self, other):
        return SparseVector.norm(other)


def sort_vectors_by(vectors, similarity, asc=True):
    vectors.sort(key=similarity, reverse=not asc)
    return vectors
