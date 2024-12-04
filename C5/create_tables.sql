DROP TABLE IF EXISTS public.staging_demographics;
DROP TABLE IF EXISTS public.staging_immigration;
DROP TABLE IF EXISTS public.human_migration;
DROP TABLE IF EXISTS public.cities;
DROP TABLE IF EXISTS public.states;
DROP TABLE IF EXISTS public.countries;
DROP TABLE IF EXISTS public.visa;
DROP TABLE IF EXISTS public.migrants;
DROP TABLE IF EXISTS public.demography;

CREATE TABLE IF NOT EXISTS public.cities (
    city_code varchar,
    city varchar,
    CONSTRAINT cities_pkey PRIMARY KEY (city_code)
);

CREATE TABLE IF NOT EXISTS public.states (
    state_code varchar,
    "state" varchar,
    CONSTRAINT states_pkey PRIMARY KEY (state_code)
);

CREATE TABLE IF NOT EXISTS public.countries (
    country_code int4,
    country varchar,
    CONSTRAINT countries_pkey PRIMARY KEY (country_code)
);

CREATE TABLE IF NOT EXISTS public.visa (
    visa_id varchar,
    visa_code int4,
    visa_type varchar,
    CONSTRAINT visa_pkey PRIMARY KEY (visa_id)
);

CREATE TABLE IF NOT EXISTS public.migrants (
    migrant_id varchar,
    citizen_country_name varchar,
    residence_country_name varchar,
    birth_year int4,
    gender varchar,
    admission_number int4,
    date_admitted_US varchar,
    date_added_to_i94file varchar,
    department_state_issue varchar,
    arrival_date date,
    departure_date date,
    CONSTRAINT migrants_pkey PRIMARY KEY (migrant_id)
);

CREATE TABLE IF NOT EXISTS public.demography (
    city_name varchar,
    state_code varchar,
    median_age float,
    male_population int4,
    female_population int4,
    total_population int4,
    number_of_veterans int4,
    foreign_born int4,
    CONSTRAINT demography_pkey PRIMARY KEY (city_name, state_code)
);

CREATE TABLE IF NOT EXISTS public.human_migration (
    human_migration_id varchar,
    cicid int4,
    "year" int4,
    "month" int4,
    citizen_country_code int4,
    residence_country_code int4,
    port_code varchar,
    arrival_date date,
    transportation_code int4,
    state_code varchar,
    departure_date date,
    visa_code int4,
    date_added_to_i94file varchar,
    department_state_issue varchar,
    airline_code varchar,
    median_age float,
    male_popultion int4,
    female_population int4,
    total_population int4,
    number_of_veterans int4,
    foreign_born int4,
    average_household_size float,
    city_individual_per_race int4,
    CONSTRAINT human_migration_pkey PRIMARY KEY (human_migration_id)
);



CREATE TABLE IF NOT EXISTS public.staging_demographics (
    city varchar,
    "state" varchar,
    median_age float,
    male_population int4,
    female_population int4,
    total_population int4,
    number_of_veterans int4,
    foreign_born int4,
    average_household_size float,
    state_code varchar,
    race varchar,
    city_individual_per_race int4
);

CREATE TABLE IF NOT EXISTS public.staging_immigration (
    cicid int4,
    "year" int4,
    "month" int4,
    citizen_country_code int4,
    residence_country_code int4,
    port_code varchar,
    arrival_date date,
    transportation_code int4,
    state_code varchar,
    departure_date date,
    i94bir int4,
    visa_code int4,
    summary_statistic int4,
    date_added_to_i94file varchar,
    department_state_issue varchar,
    arrival_flag varchar,
    departure_flag varchar,
    match_flag varchar,
    birth_year int4,
    date_admitted_US varchar,
    gender varchar,
    airline_code varchar,
    admission_number int4,
    flight_number_of_airline varchar,
    visa_type varchar,
    citizen_country_name varchar,
    residence_country_name varchar,
    city_name varchar,
    transportation varchar,
    state_name varchar,
    visa varchar
);