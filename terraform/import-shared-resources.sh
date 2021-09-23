#!/bin/bash

terraform import module.firebase.google_project.project gainyapp && \
terraform import module.firebase.module.functions-refreshToken.google_storage_bucket.bucket gainyapp-refresh_token && \
terraform import module.firebase.module.functions-processSignUp.google_storage_bucket.bucket gainyapp-process_signup && \
terraform import module.firebase.module.functions-processSignUp.google_cloudfunctions_function.function process_signup && \
terraform import module.firebase.module.functions-refreshToken.google_cloudfunctions_function.function refresh_token