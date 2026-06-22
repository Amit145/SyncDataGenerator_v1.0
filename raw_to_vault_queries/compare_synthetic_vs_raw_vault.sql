-- Synthetic vs raw-built vault comparison helpers.
-- Dialect: MySQL 8 compatible.
-- Assumption:
--   synthetic tables are loaded with prefix synthetic_
--   raw-built/silver vault tables are loaded with prefix silver_
--
-- Example physical table names:
--   synthetic_hub_policy
--   silver_hub_policy

-- -------------------------
-- Row count comparison
-- -------------------------

SELECT 'hub_policy' AS table_name,
       (SELECT COUNT(*) FROM synthetic_hub_policy) AS synthetic_count,
       (SELECT COUNT(*) FROM silver_hub_policy) AS silver_count;

SELECT 'sat_policy' AS table_name,
       (SELECT COUNT(*) FROM synthetic_sat_policy) AS synthetic_count,
       (SELECT COUNT(*) FROM silver_sat_policy) AS silver_count;

SELECT 'link_policy_customer' AS table_name,
       (SELECT COUNT(*) FROM synthetic_link_policy_customer) AS synthetic_count,
       (SELECT COUNT(*) FROM silver_link_policy_customer) AS silver_count;

-- -------------------------
-- Key set comparison examples
-- -------------------------

SELECT 'hub_policy missing_in_silver' AS check_name, s.policy_hash_key
FROM synthetic_hub_policy s
LEFT JOIN silver_hub_policy v
  ON v.policy_hash_key = s.policy_hash_key
WHERE v.policy_hash_key IS NULL;

SELECT 'hub_policy extra_in_silver' AS check_name, v.policy_hash_key
FROM silver_hub_policy v
LEFT JOIN synthetic_hub_policy s
  ON s.policy_hash_key = v.policy_hash_key
WHERE s.policy_hash_key IS NULL;

-- -------------------------
-- Full-row comparison examples
-- MySQL has no EXCEPT operator, so use NULL-safe equality.
-- -------------------------

SELECT 'sat_policy value_mismatch' AS check_name, s.policy_hash_key
FROM synthetic_sat_policy s
JOIN silver_sat_policy v
  ON v.policy_hash_key = s.policy_hash_key
WHERE NOT (
  s.load_date <=> v.load_date
  AND s.cover_option <=> v.cover_option
  AND s.declined_claims <=> v.declined_claims
  AND s.fraud_flag <=> v.fraud_flag
  AND s.gross_revenue <=> v.gross_revenue
  AND s.net_revenue <=> v.net_revenue
  AND s.number_of_active_claim <=> v.number_of_active_claim
  AND s.number_of_previous_claim <=> v.number_of_previous_claim
  AND s.policy_cycle <=> v.policy_cycle
  AND s.policy_end_date <=> v.policy_end_date
  AND s.policy_length <=> v.policy_length
  AND s.policy_number <=> v.policy_number
  AND s.policy_start_date <=> v.policy_start_date
  AND s.policy_status <=> v.policy_status
  AND s.renewal_amount_current_period <=> v.renewal_amount_current_period
  AND s.renewal_amount_next_period <=> v.renewal_amount_next_period
  AND s.renewal_date <=> v.renewal_date
  AND s.sales_channel <=> v.sales_channel
);

SELECT 'link_policy_channel value_mismatch' AS check_name, s.policy_channel_hash_key
FROM synthetic_link_policy_channel s
JOIN silver_link_policy_channel v
  ON v.policy_channel_hash_key = s.policy_channel_hash_key
WHERE NOT (
  s.load_date <=> v.load_date
  AND s.record_source <=> v.record_source
  AND s.channel_hash_key <=> v.channel_hash_key
  AND s.policy_hash_key <=> v.policy_hash_key
);

-- -------------------------
-- FK integrity examples
-- -------------------------

SELECT 'link_policy_customer missing_policy_parent' AS check_name, l.policy_hash_key
FROM silver_link_policy_customer l
LEFT JOIN silver_hub_policy h
  ON h.policy_hash_key = l.policy_hash_key
WHERE h.policy_hash_key IS NULL;

SELECT 'link_policy_customer missing_customer_parent' AS check_name, l.customer_hash_key
FROM silver_link_policy_customer l
LEFT JOIN silver_hub_customer h
  ON h.customer_hash_key = l.customer_hash_key
WHERE h.customer_hash_key IS NULL;

-- -------------------------
-- Policy date rule examples
-- -------------------------

SELECT 'policy_start_after_end' AS check_name, policy_hash_key, policy_start_date, policy_end_date
FROM silver_sat_policy
WHERE CAST(policy_start_date AS DATE) > CAST(policy_end_date AS DATE);

SELECT 'policy_renewal_outside_window' AS check_name, policy_hash_key, policy_end_date, renewal_date
FROM silver_sat_policy
WHERE renewal_date IS NOT NULL
  AND renewal_date <> ''
  AND (
    CAST(renewal_date AS DATE) < DATE_SUB(CAST(policy_end_date AS DATE), INTERVAL 60 DAY)
    OR CAST(renewal_date AS DATE) > DATE_ADD(CAST(policy_end_date AS DATE), INTERVAL 60 DAY)
  );
