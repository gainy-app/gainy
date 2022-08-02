-- Function: CDF of Normal Gaussian Distribution (mu=0,sigma=1, (so prestandardize input))
CREATE OR REPLACE FUNCTION pnorm(z double precision) RETURNS double precision AS
$$
SELECT CASE -- calculation by erf-function expansion to series (error= +/-2e-7, const 0 if <-7 AND 1 if >7)
           when $1 < -1e4 then 0.0
           when $1 > 1e4 then 1.0
           when $1 >= 0 then 1 - POWER(
                                         ((((((0.000005383 * $1 + 0.0000488906) * $1 + 0.0000380036) * $1 + 0.0032776263) *
                                            $1 + 0.0211410061) * $1 + 0.049867347) * $1 + 1), -16) / 2
           else 1 - pnorm(-$1)
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT;

-- Create a function that always returns the last non-NULL value:
CREATE OR REPLACE FUNCTION last_agg(anyelement, anyelement)
    RETURNS anyelement
    LANGUAGE sql
    IMMUTABLE STRICT PARALLEL SAFE AS
$$
SELECT $2;
$$;

-- Then wrap an aggregate around it:
CREATE OR REPLACE AGGREGATE LAST_VALUE_IGNORENULLS
	(
		SFUNC    = last_agg,
		BASETYPE = anyelement,
		STYPE    = anyelement
	);
    
-- Linear Interpolation function
CREATE OR REPLACE FUNCTION linear_interpolate(
    x_i DOUBLE PRECISION,
    x_0 DOUBLE PRECISION,
    y_0 DOUBLE PRECISION,
    x_1 DOUBLE PRECISION,
    y_1 DOUBLE precision
)
    RETURNS DOUBLE PRECISION AS
$$
SELECT (($5 - $3) / ($4 - $2)) * ($1 - $2) + $3;
$$ LANGUAGE SQL;

create or replace function is_date(s varchar) returns boolean as $$
begin
    if s is null then
        return false;
    end if;
    perform s::date;
    return true;
exception when others then
    return false;
end;
$$ language plpgsql;
