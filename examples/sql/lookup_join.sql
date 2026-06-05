-- Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
-- Flink SQL example: enrich a stream with TuGraph :Company properties via a dimension-table lookup.
-- Each probe key triggers a point MATCH (n:Company {company_id: $key}) in TuGraph; an LRU+TTL cache
-- reduces the query rate. The :Company label must already exist.

CREATE TABLE company_dim (
  company_id STRING,
  name       STRING,
  PRIMARY KEY (company_id) NOT ENFORCED
) WITH (
  'connector'             = 'tugraph',
  'uri'                   = 'bolt://127.0.0.1:7687',
  'username'              = 'admin',
  'password'              = '73@TuGraph',
  'element.type'          = 'vertex',
  'vertex.label'          = 'Company',
  'lookup.cache.max-rows' = '10000',   -- 0 disables the cache
  'lookup.cache.ttl'      = '10 min'
);

-- 'events' is your stream (e.g. Kafka) carrying a processing-time attribute 'proc_time'.
SELECT e.event_id, e.company_id, c.name
FROM events AS e
JOIN company_dim FOR SYSTEM_TIME AS OF e.proc_time AS c
  ON e.company_id = c.company_id;
