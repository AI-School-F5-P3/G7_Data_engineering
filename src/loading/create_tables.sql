-- Tabla para datos personales
CREATE TABLE IF NOT EXISTS personal_data (
    passport VARCHAR(20) PRIMARY KEY,
    name VARCHAR(50),
    last_name VARCHAR(50),
    sex VARCHAR(10),
    telfnumber VARCHAR(20),
    email VARCHAR(50)
);

-- Tabla para datos de ubicación
CREATE TABLE IF NOT EXISTS location_data (
    passport VARCHAR(20) PRIMARY KEY,
    fullname VARCHAR(100),
    city VARCHAR(50),
    address VARCHAR(100)
);

-- Tabla para datos profesionales
CREATE TABLE IF NOT EXISTS professional_data (
    passport VARCHAR(20) PRIMARY KEY,
    fullname VARCHAR(100),
    company VARCHAR(100),
    company_address VARCHAR(100),
    company_telfnumber VARCHAR(20),
    company_email VARCHAR(50),
    job VARCHAR(50)
);

-- Tabla para datos bancarios
CREATE TABLE IF NOT EXISTS bank_data (
    passport VARCHAR(20) PRIMARY KEY,
    iban VARCHAR(34),
    salary VARCHAR(20)
);

-- Tabla para datos de red
CREATE TABLE IF NOT EXISTS net_data (
    passport VARCHAR(20) PRIMARY KEY,
    address VARCHAR(100),
    ipv4 VARCHAR(15)
);
