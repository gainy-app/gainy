locals {
  ecr_repo = var.repository_name

  meltano_build_args_force_build = {
    BASE_IMAGE_VERSION    = var.base_image_version
    GAINY_COMPUTE_VERSION = var.gainy_compute_version
    MELTANO_SOURCE_MD5    = data.archive_file.meltano_source.output_md5
  }
  meltano_build_args = merge(local.meltano_build_args_force_build, {
    BASE_IMAGE_REGISTRY_ADDRESS = var.base_image_registry_address
    CODEARTIFACT_PIPY_URL       = var.codeartifact_pipy_url
  })

  websockets_build_args_force_build = {
    WEBSOCKETS_SOURCE_MD5 = data.archive_file.websockets_source.output_md5
    GAINY_COMPUTE_VERSION = var.gainy_compute_version
  }
  websockets_build_args = merge(local.websockets_build_args_force_build, {
    CODEARTIFACT_PIPY_URL = var.codeartifact_pipy_url
  })

  meltano_root_dir           = abspath("${path.cwd}/../src/meltano")
  meltano_seed_root_dir      = abspath("${path.cwd}/../src/meltano/meltano/data")
  meltano_transform_root_dir = abspath("${path.cwd}/../src/meltano/meltano/transform")
  meltano_image_tag          = format("meltano-%s-%s-%s", var.env, var.base_image_version, md5(jsonencode(local.meltano_build_args_force_build)))
  meltano_ecr_image_name     = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.meltano_image_tag)

  hasura_root_dir       = abspath("${path.cwd}/../src/hasura")
  hasura_image_tag      = format("hasura-%s-%s-%s", var.env, var.base_image_version, data.archive_file.hasura_source.output_md5)
  hasura_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.hasura_image_tag)

  websockets_root_dir       = abspath("${path.cwd}/../src/websockets")
  websockets_image_tag      = format("websockets-%s-%s", var.env, md5(jsonencode(local.websockets_build_args_force_build)))
  websockets_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.websockets_image_tag)
}
