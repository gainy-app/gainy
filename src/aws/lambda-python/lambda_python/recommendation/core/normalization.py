from recommendation.core.dim_vector import DimVector


def double_normalization_k(vector: DimVector, k: float = 0.5) -> DimVector:
    """
    Maximum TF normalization (double normalization K) is a well-studied technique is to normalize
    the TF weights of all terms occurring in a document by the maximum tf in that document.

    See:
    1. IR book: https://nlp.stanford.edu/IR-book/html/htmledition/maximum-tf-normalization-1.html
    2. `double normalization K` in Wikipedia: https://en.wikipedia.org/wiki/Tf%E2%80%93idf

    :param vector: input DimVector
    :param k: normalization factor
    :return: DimVector with normalized coordinates
    """

    if len(vector.dims) == 0:
        return vector

    max_value = max(vector.values)
    if max_value == 0:
        return vector

    new_coordinates = {}
    for (dim, value) in zip(vector.dims, vector.values):
        if value > 0:
            new_coordinates[dim] = k + (1 - k) * value / max_value
        else:
            new_coordinates[dim] = 0.0

    return DimVector(new_coordinates)