DROP SCHEMA IF EXISTS coffee;
CREATE SCHEMA coffee;

CREATE TABLE coffee.review_cupping (
    id SERIAL PRIMARY KEY,
    acidity NUMERIC(4,2) NOT NULL,
    aftertaste NUMERIC(4,2) NOT NULL,
    aroma NUMERIC(4,2) NOT NULL,
    balance NUMERIC(4,2) NOT NULL,
    body NUMERIC(4,2) NOT NULL,
    cleanliness NUMERIC(4,2) NOT NULL,
    flavor NUMERIC(4,2) NOT NULL,
    sweetness NUMERIC(4,2) NOT NULL,
    cupper_points NUMERIC(4,2) NOT NULL,
    total_cup_point NUMERIC(5,2) NOT NULL,
    uniformity NUMERIC(4,2) NOT NULL
);

CREATE TABLE coffee.review_green (
    id SERIAL PRIMARY KEY,
    defects_1 INTEGER NOT NULL,
    defects_2 INTEGER NOT NULL,
    color VARCHAR(12),
    moisture NUMERIC(4,2) NOT NULL,
    quakers VARCHAR(2),
    processing_method VARCHAR
);

CREATE TABLE coffee.certificate (
    id SERIAL PRIMARY KEY,
    address VARCHAR(40) NOT NULL,
    body VARCHAR(100) NOT NULL,
    contact VARCHAR(40) NOT NULL
);

CREATE TABLE coffee.altitude_meters (
    id SERIAL PRIMARY KEY,
    high VARCHAR(9),
    low VARCHAR(9),
    mean VARCHAR(9)
);

CREATE TABLE coffee.altitude (
    id SERIAL PRIMARY KEY,
    altitude VARCHAR(41),
    unit VARCHAR(2) NOT NULL
);

CREATE TABLE coffee.farm (
    id SERIAL PRIMARY KEY,
    owner VARCHAR(50),
    owner_1 VARCHAR(50),
    company VARCHAR(78),
    country VARCHAR(28),
    name VARCHAR(73),
    ico VARCHAR(40),
    country_partner VARCHAR(85) NOT NULL,
    lot_number VARCHAR(71),
    mill VARCHAR(77),
    region VARCHAR(76),
    producer VARCHAR(100)
);

CREATE TABLE coffee.beans (
    id SERIAL PRIMARY KEY,
    bagweight VARCHAR(8) NOT NULL,
    number_of_bags INTEGER,
    species VARCHAR(7) NOT NULL,
    variety VARCHAR(21),
    expiration VARCHAR(20) NOT NULL,
    grading_date VARCHAR(20) NOT NULL,
    harvest_year VARCHAR(24)
);

