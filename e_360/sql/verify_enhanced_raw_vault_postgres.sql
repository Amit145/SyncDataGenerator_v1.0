-- Verify enhanced source-split raw vault in PostgreSQL.

SELECT 'table_split' AS check_name,
       COUNT(*) FILTER (WHERE table_name LIKE 'hub\_%' ESCAPE '\') AS hubs,
       COUNT(*) FILTER (WHERE table_name LIKE 'link\_%' ESCAPE '\') AS links,
       COUNT(*) FILTER (WHERE table_name LIKE 'sat\_%' ESCAPE '\') AS sats,
       COUNT(*) AS total
FROM information_schema.tables
WHERE table_schema = 'public'
  AND (
      table_name LIKE 'hub\_%' ESCAPE '\'
      OR table_name LIKE 'link\_%' ESCAPE '\'
      OR table_name LIKE 'sat\_%' ESCAPE '\'
  );

SELECT 'required_counts' AS check_name, 'hub_person' AS table_name, COUNT(*) AS rows FROM hub_person
UNION ALL SELECT 'required_counts', 'hub_policy', COUNT(*) FROM hub_policy
UNION ALL SELECT 'required_counts', 'hub_insured_object', COUNT(*) FROM hub_insured_object
UNION ALL SELECT 'required_counts', 'sat_person_crm', COUNT(*) FROM sat_person_crm
UNION ALL SELECT 'required_counts', 'sat_person_sap', COUNT(*) FROM sat_person_sap
UNION ALL SELECT 'required_counts', 'sat_address_sap', COUNT(*) FROM sat_address_sap
UNION ALL SELECT 'required_counts', 'sat_product_sap', COUNT(*) FROM sat_product_sap
UNION ALL SELECT 'required_counts', 'sat_home_sap', COUNT(*) FROM sat_home_sap
UNION ALL SELECT 'required_counts', 'sat_motor_sap', COUNT(*) FROM sat_motor_sap
UNION ALL SELECT 'required_counts', 'link_person_address', COUNT(*) FROM link_person_address
UNION ALL SELECT 'required_counts', 'link_policy_product', COUNT(*) FROM link_policy_product
UNION ALL SELECT 'required_counts', 'link_policy_insured_object', COUNT(*) FROM link_policy_insured_object;

SELECT 'fk_link_person_address_person' AS check_name, COUNT(*) AS missing_rows
FROM link_person_address l
LEFT JOIN hub_person h ON h.person_hash_key = l.person_hash_key
WHERE l.person_hash_key IS NOT NULL
  AND h.person_hash_key IS NULL;

SELECT 'fk_link_person_address_address' AS check_name, COUNT(*) AS missing_rows
FROM link_person_address l
LEFT JOIN hub_address h ON h.address_hash_key = l.address_hash_key
WHERE l.address_hash_key IS NOT NULL
  AND h.address_hash_key IS NULL;

SELECT 'fk_link_policy_product_policy' AS check_name, COUNT(*) AS missing_rows
FROM link_policy_product l
LEFT JOIN hub_policy h ON h.policy_hash_key = l.policy_hash_key
WHERE l.policy_hash_key IS NOT NULL
  AND h.policy_hash_key IS NULL;

SELECT 'fk_link_policy_product_product' AS check_name, COUNT(*) AS missing_rows
FROM link_policy_product l
LEFT JOIN hub_product h ON h.product_hash_key = l.product_hash_key
WHERE l.product_hash_key IS NOT NULL
  AND h.product_hash_key IS NULL;

SELECT 'fk_link_policy_insured_object_policy' AS check_name, COUNT(*) AS missing_rows
FROM link_policy_insured_object l
LEFT JOIN hub_policy h ON h.policy_hash_key = l.policy_hash_key
WHERE l.policy_hash_key IS NOT NULL
  AND h.policy_hash_key IS NULL;

SELECT 'fk_link_policy_insured_object_object' AS check_name, COUNT(*) AS missing_rows
FROM link_policy_insured_object l
LEFT JOIN hub_insured_object h ON h.insured_object_hash_key = l.insured_object_hash_key
WHERE l.insured_object_hash_key IS NOT NULL
  AND h.insured_object_hash_key IS NULL;
