-- Test Core SQL Features

-- Create tables
CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary INTEGER);
CREATE TABLE departments (id INTEGER, name TEXT);

-- Insert test data
INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 75000);
INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 80000);
INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 60000);
INSERT INTO employees VALUES (4, 'David', 'Sales', 65000);
INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 90000);
INSERT INTO employees VALUES (6, 'Frank', 'HR', 55000);

INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Sales');
INSERT INTO departments VALUES (3, 'HR');

-- Test 1: ORDER BY with LIMIT
SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3;

-- Test 2: DISTINCT
SELECT DISTINCT department FROM employees;

-- Test 3: OFFSET with LIMIT
SELECT name FROM employees ORDER BY name ASC LIMIT 2 OFFSET 2;

-- Test 4: Aggregate functions with GROUP BY
SELECT department, COUNT(*), AVG(salary), MIN(salary), MAX(salary)
FROM employees
GROUP BY department;

-- Test 5: GROUP BY with HAVING
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 1;

-- Test 6: LEFT JOIN
SELECT e.name, d.name
FROM employees e
LEFT JOIN departments d ON e.department = d.name;

-- Test 7: Aggregate without GROUP BY (should work on entire table)
SELECT COUNT(*), SUM(salary), AVG(salary) FROM employees;

-- Test 8: DROP TABLE
DROP TABLE departments;

-- Verify table was dropped
SELECT * FROM SQLR_TABLES;
