-- Test COUNT(*) parsing
CREATE TABLE test (id INTEGER, name TEXT);
INSERT INTO test VALUES (1, 'Alice');
INSERT INTO test VALUES (2, 'Bob');
INSERT INTO test VALUES (3, 'Charlie');
SELECT COUNT(*) FROM test;
SELECT COUNT(*), MAX(id) FROM test;
