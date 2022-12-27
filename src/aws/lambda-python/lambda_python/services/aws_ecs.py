import boto3
from gainy.utils import get_logger

logger = get_logger(__name__)


class ECS:

    def __init__(self):
        self.ecs_client = boto3.client("ecs")

    def describe_task_definition(self, id):
        return self.ecs_client.describe_task_definition(taskDefinition=id,
                                                        include=[
                                                            'TAGS',
                                                        ])
