-- Create commerce schema
CREATE SCHEMA IF NOT EXISTS commerce;

-- Set schema search path
SET search_path TO commerce;

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price REAL NOT NULL
);

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL
);

-- Ensure Debezium can capture full row changes
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE users REPLICA IDENTITY FULL;
