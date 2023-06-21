python scripts/generate_meltano_config.py
patch -u /venv/lib64/python3.9/site-packages/meltano/migrations/versions/6828cc5b1a4f_create_dedicated_state_table.py -i scripts/patches/6828cc5b1a4f_create_dedicated_state_table.patch
meltano upgrade database
