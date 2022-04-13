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
