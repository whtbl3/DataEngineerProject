class SqlQueries:
    human_migration_table_insert = ("""
        SELECT DISTINCT
                md5(imm.cicid || imm.state_code || imm.citizen_country_code) human_migration_id,
                imm.cicid,
                imm.year,
                imm.month,
                imm.citizen_country_code,
                imm.residence_country_code,
                imm.port_code,
                imm.arrival_date,
                imm.transportation_code,
                imm.state_code,
                imm.departure_date,
                imm.visa_code,
                imm.date_added_to_i94file,
                imm.department_state_issue,
                imm.airline_code,
                dem.median_age,
                dem.male_population,
                dem.female_population,
                dem.total_population,
                dem.number_of_veterans,
                dem.foreign_born,
                dem.average_household_size,
                dem.city_individual_per_race
            FROM staging_immigration imm 
            INNER JOIN staging_demographics dem
            ON imm.state_code = dem.state_code
                AND lower(imm.city_name) = lower(dem.city)
    """)
    
    cities_table_insert = ("""
        SELECT DISTINCT
            port_code AS city_code,
            city_name AS city
        FROM staging_immigration
        WHERE port_code IS NOT NULL
    """)
    
    states_table_insert = ("""
        SELECT DISTINCT
            state_code,
            state_name AS state
        FROM staging_immigration
        WHERE state_code IS NOT NULL
    """)
    
    countries_table_insert = ("""
        SELECT DISTINCT
            citizen_country_code AS country_code,
            citizen_country_name AS country
        FROM staging_immigration
        WHERE citizen_country_code IS NOT NULL
    """)
    
    visa_table_insert = ("""
        SELECT DISTINCT
            md5(visa_code || visa_type) AS visa_id,
            visa_code,
            visa_type
        FROM staging_immigration
    """)
    
    migrants_table_insert = ("""
        SELECT DISTINCT
            md5(cicid || admission_number) AS migrant_id,
            citizen_country_name,
            residence_country_name,
            birth_year,
            gender,
            admission_number,
            date_admitted_US,
            date_added_to_i94file,
            department_state_issue,
            arrival_date,
            departure_date
        FROM staging_immigration
    """)
    
    demography_table_insert = ("""
        SELECT DISTINCT
            city,
            state_code,
            median_age,
            male_population,
            female_population,
            total_population,
            number_of_veterans,
            foreign_born
        FROM staging_demographics
    """)
    