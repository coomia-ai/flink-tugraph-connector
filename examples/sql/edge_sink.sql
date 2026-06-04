-- Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
-- Flink SQL example: write a stream of investment relationships into TuGraph as :INVEST edges.
--
-- The endpoint vertices (:Company) must exist first (see vertex_sink.sql). Missing endpoints are
-- skipped by default; set 'edge.on-missing-endpoint' = 'fail' to fail the job instead.

CREATE TABLE invest_edge (
  src_company STRING,
  dst_company STRING,
  ratio       DOUBLE
) WITH (
  'connector'                = 'tugraph',
  'uri'                      = 'bolt://127.0.0.1:7687',
  'username'                 = 'admin',
  'password'                 = '73@TuGraph',
  'graph'                    = 'default',
  'element.type'             = 'edge',
  'edge.label'               = 'INVEST',
  'edge.src.label'           = 'Company',
  'edge.src.col'             = 'src_company',
  'edge.src.key'             = 'company_id',
  'edge.dst.label'           = 'Company',
  'edge.dst.col'             = 'dst_company',
  'edge.dst.key'             = 'company_id',
  'edge.on-missing-endpoint' = 'skip',
  'sink.batch.size'          = '500'
);

INSERT INTO invest_edge VALUES
  ('c1', 'c2', 0.30),
  ('c2', 'c1', 0.15);
