from gainy.data_access.models import BaseModel, classproperty


class Profile(BaseModel):
    id = None
    email = None
    first_name = None
    last_name = None
    gender = None
    user_id = None
    avatar_url = None
    legal_address = None
    subscription_end_date = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "profiles"


class UploadedFile(BaseModel):
    id = None
    profile_id = None
    s3_bucket = None
    s3_key = None
    content_type = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "uploaded_files"
