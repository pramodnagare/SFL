CREATE TABLE IF NOT EXISTS {} (
    id INTEGER NOT NULL,
    first_name VARCHAR (50) NOT NULL,
    last_name VARCHAR (50) NOT NULL,
    email VARCHAR (100) NOT NULL,
    domain VARCHAR (100) NOT NULL,
    gender VARCHAR (50) NOT NULL,
    ip_address VARCHAR (50) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT NOW()
);