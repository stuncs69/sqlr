-- Test file for new SQLR features
-- Health check, functions, optimizations, and caching

-- Health check command
HEALTH

-- Create a test table
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);

-- Insert test data
INSERT INTO users VALUES (1, 'John Doe', 30);
INSERT INTO users VALUES (2, 'Jane Smith', 25);
INSERT INTO users VALUES (3, 'Bob Johnson', 35);
INSERT INTO users VALUES (-5, 'Charlie Brown', 28);

-- Test STRING FUNCTIONS

-- UPPER function
SELECT name, UPPER(name) FROM users;

-- LOWER function
SELECT name, LOWER(name) FROM users;

-- LENGTH function
SELECT name, LENGTH(name) FROM users;

-- SUBSTRING function (2-arg and 3-arg)
SELECT name, SUBSTRING(name, 1, 4) FROM users;
SELECT name, SUBSTR(name, 6) FROM users;

-- CONCAT function
SELECT CONCAT('User: ', name, ' (Age: ', age, ')') FROM users;

-- TRIM function
INSERT INTO users VALUES (4, '  Trimmed Name  ', 40);
SELECT name, TRIM(name) FROM users WHERE id = 4;

-- Test MATH FUNCTIONS

-- ABS function
SELECT id, ABS(id) FROM users;

-- RANDOM function
SELECT RANDOM();
SELECT RAND();

-- ROUND, CEIL, FLOOR (work with integers)
SELECT age, ROUND(age), CEIL(age), FLOOR(age) FROM users;

-- Test HEALTH CHECK again to see updated metrics
HEALTH

-- Test PERFORMANCE METRICS
-- Queries should now report execution time
SELECT COUNT(*) FROM users;

-- Complex query with functions
SELECT
    id,
    name,
    UPPER(name) as upper_name,
    LENGTH(name) as name_length,
    ABS(id) as abs_id,
    age
FROM users
WHERE age > 25
ORDER BY age DESC;

-- Final health check to see all metrics
HEALTH
