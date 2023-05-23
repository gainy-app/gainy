drop index app.corporate_action_adjustments_profile_id_symbol_date_index;

alter table app.corporate_action_adjustments
    drop column date;
