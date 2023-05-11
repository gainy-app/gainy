alter table "app"."kyc_statuses"
    add column error_codes json;

with text_to_code_mapping(msg, code) as
         (
             values ('"No match found for address or invalid format in address"', 'ADDRESS_NOT_MATCH'),
                    ('"User is under sanction watchlist"', 'SANCTION_WATCHLIST'),
                    ('"No match found for address or invalid format in address."', 'ADDRESS_NOT_MATCH'),
                    ('"No match found for Firstname/Lastname or Invalid character found."', 'NAME_NOT_MATCH'),
                    ('"No document was found in the image, or there is a blank image"', 'NO_DOC_IN_IMAGE'),
                    ('"No match found for Social Security Number"', 'SSN_NOT_MATCH'),
                    ('"No match found for firstName / lastName or invalid characters found"', 'NAME_NOT_MATCH'),
                    ('"No match found for Date of Birth"', 'DOB_NOT_MATCH')
         ),
    kyc_status_codes as
        (
            select id, json_agg(code) as error_codes
            from (
                     select id, json_array_elements(error_messages)::text as msg
                     from app.kyc_statuses
                 ) t
                     left join text_to_code_mapping using (msg)
            group by id
     )
update app.kyc_statuses
set error_codes = kyc_status_codes.error_codes
from kyc_status_codes
where kyc_status_codes.id = kyc_statuses.id;
