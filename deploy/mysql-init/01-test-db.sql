-- Separate database for `go test -tags=integration` (the tests wipe their tables).
CREATE DATABASE IF NOT EXISTS crep_test;
GRANT ALL PRIVILEGES ON crep_test.* TO 'crep'@'%';
USE crep_test;
SOURCE /docker-entrypoint-initdb.d/00-schema.sql;
