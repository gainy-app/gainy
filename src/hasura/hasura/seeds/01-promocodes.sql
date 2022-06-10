INSERT INTO app.promocodes (id, code, influencer_id, description, name, config, is_active)
VALUES (1, 'test', 1, 'test promocode', 'test promocode', '{"tariff_mapping": {"gainy_80_r_y1": "gainy_56_r_y1"}}', true);
ALTER SEQUENCE app.promocodes_id_seq RESTART WITH 2;
