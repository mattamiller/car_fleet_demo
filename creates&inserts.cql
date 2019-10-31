--Create rentals table
CREATE TABLE IF NOT EXISTS keyspace1.rentals (
    start_fuel double,
    end_fuel double,
    vehicle_type text,
    vehicle_id text,
    start_location text,
    end_location text,
    account_email UUID,
    rental_id UUID,
    rental_start timestamp,
    rental_stop timestamp,
    hard_brake_instances int,
    PRIMARY KEY (rental_id, start_location,rental_start))
        WITH CLUSTERING ORDER BY (start_location DESC, rental_start DESC);
    

-- Create live_trip table
CREATE TABLE IF NOT EXISTS keyspace1.live_trip (
    speed double,
    hard_brake int, -- 0 or 1
    vehicle_id text,
    current_route text,
    trip_id UUID,
    timestamp timestamp,
    PRIMARY KEY (trip_id, timestamp))
        WITH CLUSTERING ORDER BY (timestamp DESC);


-- Create Table Accounts
DROP TABLE IF EXISTS keyspace1.accounts;
CREATE TABLE IF NOT EXISTS keyspace1.accounts (
    first_name text,
    last_name text,
    email text,
    PRIMARY KEY(email));

INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Matt','Miller','matt@testemail.org');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Foo','Barr','foo@aol.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Sam','Massy','sam@gmail.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Frank','Thompson','ft@hotmail.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('James','Newport','jpn@yahoo.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Quentin','Anderson','qanders@gmail.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Alvin','Bogdon','abogdon@outlook.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email) 
    VALUES('Nick','Smith','smithers@gmail.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Kim','McKinney','kimmc@yahoo.com');
INSERT INTO keyspace1.accounts (first_name, last_name, email)
    VALUES('Sarah','Johnson','sj@outlook.com');


-- Create table rental_locations
DROP TABLE IF EXISTS keyspace1.rental_locations;
CREATE TABLE IF NOT EXISTS keyspace1.rental_locations(
    city text,
    state text,
    airport_code text,
    PRIMARY KEY (airport_code)
);

INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('San Jose', 'CA', 'SJC');
INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('Los Angeles', 'CA', 'LAX');
INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('San Diego', 'CA', 'SAN');
INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('Reno', 'NV', 'RNO');
INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('Las Vegas', 'NV', 'LAS');
INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('Portland', 'OR', 'PDX');
INSERT INTO keyspace1.rental_locations (city, state, airport_code)
    VALUES('Seattle', 'WA', 'SEA');


-- create table for vehicles
DROP TABLE IF EXISTS keyspace1.vehicles;
CREATE TABLE IF NOT EXISTS keyspace1.vehicles(
    id text,
    make text,
    model text,
    year text,
    type text,
    PRIMARY KEY(id)
);

INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('ff2018lt-0001','Ford','F-150','2018','large truck');
INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('fr2020st-0001','Ford','Ranger','2020','small truck');
INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('ct2019ls-0001','Cheverolet','Tahoe','2019','large suv');
INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('hc2016ss-0001','Honda','CRV','2016', 'small suv');  
INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('cp2019v-0001','Chrysler','Pacifica','2019','van');   
INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('tc2020s-0001','Toyota','Corolla','2019','sedan');
INSERT INTO keyspace1.vehicles (id, make, model, year, type)
    VALUES ('kf2018c-0001','Kia','Forte','2018','compact');  





















