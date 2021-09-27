from math import sqrt


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



