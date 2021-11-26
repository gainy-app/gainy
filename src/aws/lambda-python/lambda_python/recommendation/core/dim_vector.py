import numpy as np
from numpy import dot
from numpy.linalg import norm


class DimVector:
    """
    DimVector is a wrapper of `numpy` array, which stores names of dimensions together with values.
    This implementation is useful to deal with "bag-of-words" model - that is how we typically deal
    with categories and industries.

    All mathematical operations (e.g. norm, cosine similarity) are performed against the underlying
     `numpy` vector.

    Alternatives considered:
    - https://xarray.pydata.org/ - can be useful in the future, but now is an overkill (as it requires pandas)
    - https://numpy.org/doc/stable/user/basics.rec.html - numpy structured arrays are differenet and can't be
    a direct replacement to this class
    - https://github.com/wagdav/dimarray (and https://github.com/perrette/dimarray) - custom implementations of
    similar data structure with wider functionality. Unfortunately, booth are outdated.
    """
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
    def norm(vector, order=2):
        return norm(vector.values, ord=order)

    @staticmethod
    def dot_product(first, second, reshape=True):
        common_dims = list(set(first.dims).union(second.dims))

        if reshape:
            first_reshaped = DimVector.reshape(first, common_dims)
            second_reshaped = DimVector.reshape(second, common_dims)

            return dot(first_reshaped.values, second_reshaped.values)
        else:
            return dot(first.values, second.values)

    def cosine_similarity(self, other, norm_order=2):
        self_norm = DimVector.norm(self, order=norm_order)
        other_norm = DimVector.norm(other, order=norm_order)

        if self_norm == 0.0 or other_norm == 0.0:
            return 0.0

        return DimVector.dot_product(self, other) / self_norm / other_norm


class NamedDimVector(DimVector):
    def __init__(self, name, coordinates):
        super().__init__(coordinates)
        self.name = name
