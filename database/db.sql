CREATE TABLE weather (
    id SERIAL PRIMARY KEY,
    dt TIMESTAMP NOT NULL,
    temperature FLOAT NOT NULL,
    humidity INT NOT NULL,
    description TEXT
);

CREATE TABLE traffic (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    distance TEXT NOT NULL,
    duration TEXT NOT NULL
);
