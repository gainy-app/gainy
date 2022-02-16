from common.hasura_function import HasuraTrigger
from recommendation.compute import ComputeRecommendationsAndPersist


class SetRecommendations(HasuraTrigger):

    def __init__(self):
        super().__init__([
            "recommendations__profile_categories",
            "recommendations__profile_interests"
        ])

    def apply(self, db_conn, op, data):
        profile_id = self.get_profile_id(op, data)

        recommendations_func = ComputeRecommendationsAndPersist(db_conn, profile_id)
        recommendations_func.get_and_persist(db_conn, max_tries=3)

    def get_profile_id(self, op, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]
