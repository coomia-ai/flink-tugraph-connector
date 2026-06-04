-- Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
-- Flink SQL example: write a stream of companies into TuGraph as :Company vertices.
--
-- Usage (Flink SQL Client): ADD JAR the connector, paste the statements below, then run the INSERT.

CREATE TABLE company_vertex (
  company_id  STRING,
  name        STRING,
  reg_capital DOUBLE,
  PRIMARY KEY (company_id) NOT ENFORCED
) WITH (
  'connector'       = 'tugraph',
  'uri'             = 'bolt://127.0.0.1:7687',
  'username'        = 'admin',
  'password'        = '73@TuGraph',
  'graph'           = 'default',
  'element.type'    = 'vertex',
  'vertex.label'    = 'Company',
  'sink.batch.size' = '500'
);

-- In production, replace VALUES with a SELECT from a Kafka / CDC source table.
INSERT INTO company_vertex VALUES
  ('c1', 'Acme', 1000000.0),
  ('c2', 'Beta', 500000.0);
