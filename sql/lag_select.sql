\timing off
SET statement_timeout TO 1000;

-- WITH max_lag AS (SELECT min(t) AS max_lag FROM lag_test), now_time AS (SELECT now() AS now_time)
-- SELECT EXTRACT(EPOCH FROM (max(now_time) - min(max_lag)))*1000 from now_time, max_lag;

-- DO $$
-- DECLARE
--   max_lag TIMESTAMP;
-- BEGIN
--     SELECT min(t) INTO max_lag FROM lag_test;
--     RAISE NOTICE 'Lag: %', EXTRACT(EPOCH FROM (now() - max_lag))*1000;
-- END $$;

SELECT min(t) FROM lag_test;