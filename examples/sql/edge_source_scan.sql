-- Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
-- Flink SQL example: read :INVEST edges from TuGraph (bounded edge scan).
-- The src/dst key columns map to the endpoint vertices; other columns map to the edge.
-- Projection is pushed down. The labels must already exist in TuGraph.

CREATE TABLE invest_src (
  src_company STRING,
  dst_company STRING,
  ratio       DOUBLE
) WITH (
  'connector'      = 'tugraph',
  'uri'            = 'bolt://127.0.0.1:7687',
  'username'       = 'admin',
  'password'       = '73@TuGraph',
  'element.type'   = 'edge',
  'edge.label'     = 'INVEST',
  'edge.src.label' = 'Company',
  'edge.src.col'   = 'src_company',
  'edge.src.key'   = 'company_id',
  'edge.dst.label' = 'Company',
  'edge.dst.col'   = 'dst_company',
  'edge.dst.key'   = 'company_id'
);

SELECT src_company, dst_company, ratio FROM invest_src;
