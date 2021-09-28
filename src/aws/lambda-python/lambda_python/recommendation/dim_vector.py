import numpy as np
from numpy import dot
from numpy.linalg import norm


class DimVector:

    def __init__(self, coordinates):
        if coordinates:
            dim_list = []
            value_list = []
            for (dim, value) in coordinates.items():
                dim_list.append(dim)
                value_list.append(value)

            self.dims = dim_list
            self.values = np.array(value_list)
        else:
            self.dims = []
            self.values = np.array([])

    @staticmethod
    def reshape(vector, new_dims):
        coordinates = dict([(new_dim, 0) for new_dim in new_dims])
        for (dim, value) in zip(vector.dims, vector.values):
            if dim in coordinates:
                coordinates[dim] = value

        return DimVector(coordinates)

    @staticmethod
    def norm(vector):
        return norm(vector.values)

    @staticmethod
    def dot_product(first, second, reshape=True):
        common_dims = list(set(first.dims).union(second.dims))

        if reshape:
            first_reshaped = DimVector.reshape(first, common_dims)
            second_reshaped = DimVector.reshape(second, common_dims)

            return dot(first_reshaped.values, second_reshaped.values)
        else:
            return dot(first.values, second.values)

    def cosine_similarity(self, other):
        self_norm = DimVector.norm(self)
        other_norm = DimVector.norm(other)

        if self_norm == 0.0 or other_norm == 0.0:
            return 0.0

        return DimVector.dot_product(self, other) / self_norm / other_norm



