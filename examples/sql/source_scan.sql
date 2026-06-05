-- Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
-- Flink SQL example: read :Company vertices from TuGraph (bounded scan).
-- Projection and filter (=, <>, >, >=, <, <=, IN) are pushed down into TuGraph.

CREATE TABLE company_src (
  company_id  STRING,
  name        STRING,
  reg_capital DOUBLE
) WITH (
  'connector'          = 'tugraph',
  'uri'                = 'bolt://127.0.0.1:7687',
  'username'           = 'admin',
  'password'           = '73@TuGraph',
  'element.type'       = 'vertex',
  'vertex.label'       = 'Company',
  'vertex.primary-key' = 'company_id',   -- ORDER BY column for stable paging
  'scan.fetch-size'    = '1000'
);

-- Only company_id and name are RETURNed; the predicate becomes a Cypher WHERE.
SELECT company_id, name
FROM company_src
WHERE reg_capital > 1000000;
