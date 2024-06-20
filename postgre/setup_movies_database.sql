-- Connect to your PostgreSQL server and execute these commands

-- Drop the database if it already exists
DROP DATABASE IF EXISTS Movies;

-- Create the database
CREATE DATABASE Movies;

-- Connect to the "Movies" database and execute these commands

-- Drop the table if it already exists
DROP TABLE IF EXISTS public."Movies";

-- Create the "Movies" table
CREATE TABLE IF NOT EXISTS public."Movies"
(
    id SERIAL PRIMARY KEY,
    budget VARCHAR(255),
    language VARCHAR(255),
    revenue VARCHAR(255),
    runtime VARCHAR(255),
    spoken_languages VARCHAR(255),
    title VARCHAR(255)
);

-- Set the owner of the table
ALTER TABLE IF EXISTS public."Movies"
    OWNER to postgres;

-- Populate the "Movies" table from the CSV file

COPY public."Movies" (budget, language, revenue, runtime, spoken_languages, title)
FROM '\neo4j\data-preprocess\data\movies_metadata.csv'
DELIMITER ','
CSV HEADER;
