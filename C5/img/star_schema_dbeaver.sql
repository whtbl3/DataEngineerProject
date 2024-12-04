USE Immigration;

DROP TABLE IF EXISTS fact_immigrations;
DROP TABLE IF EXISTS dim_migrants;
DROP TABLE IF EXISTS dim_status;
DROP TABLE IF EXISTS dim_cities;
DROP TABLE IF EXISTS dim_states;
DROP TABLE IF EXISTS dim_visas;
DROP TABLE IF EXISTS dim_demographics;

CREATE TABLE dim_migrants (
	cicid BIGINT,
	citizen_country INT,
	residence_country INT,
	birth_year INT,
	gender CHAR(1),
	admission_number INT,
	age_of_respondent INT,
	date_admitted_US datetime,
	CONSTRAINT PK_migrants PRIMARY KEY (cicid)
);

CREATE TABLE dim_cities (
	port_code VARCHAR,
	city_name VARCHAR,
	CONSTRAINT PK_city PRIMARY KEY (port_code)
);

CREATE TABLE dim_states (
	state_code VARCHAR,
	"state" VARCHAR,
	CONSTRAINT PK_state PRIMARY KEY (state_code)
);

CREATE TABLE dim_visas (
	visa_id INT,
	visa_code INT,
	visa_type VARCHAR,
	visa_post VARCHAR
	CONSTRAINT PK_visa PRIMARY KEY (visa_id)
);

CREATE TABLE dim_demographics (
	city_name VARCHAR,
	state_code VARCHAR,
	median_age INT,
	male_population INT,
	female_population INT,
	total_population INT,
	number_veterans INT,
	foreign_born INT,
	avg_household_size FLOAT,
	respondent_race VARCHAR
	CONSTRAINT PK_demographic PRIMARY KEY (city_name, state_code)
);

CREATE TABLE fact_immigrations (
	immigration_id INT NOT NULL IDENTITY(1,1),
	cicid BIGINT,
	"year" INT,
	"month" INT,
	citizen_country_code INT,
	residence_country_code INT,
	port_code VARCHAR,
	arrival_date DATETIME,
	transportation_code INT,
	state_code VARCHAR,
	departure_date DATETIME,
	age_of_respondent INT,
	visa_code INT,
	date_added_to_i94file VARCHAR,
	department_state_issue VARCHAR,
	airline_code VARCHAR,
	visa_type VARCHAR,
	city_name VARCHAR

	CONSTRAINT PK_immigrations PRIMARY KEY (immigration_id),
	CONSTRAINT FK_immigrations_migrants FOREIGN KEY (cicid) REFERENCES dim_migrants (cicid),
	CONSTRAINT FK_immigrations_city FOREIGN KEY (port_code) REFERENCES dim_cities(port_code),
	CONSTRAINT FK_immigrations_state FOREIGN KEY (state_code) REFERENCES dim_states(state_code),
	CONSTRAINT FK_immigrations_visa FOREIGN KEY (visa_code) REFERENCES dim_visas(visa_id),
	CONSTRAINT FK_immigration_demographic FOREIGN KEY (city_name, state_code) REFERENCES dim_demographics(city_name, state_code)
);