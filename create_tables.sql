CREATE TABLE public.symbols (
    id INT IDENTITY(0, 1) PRIMARY KEY,
    symbol VARCHAR(256) NOT NULL,
    description VARCHAR(256),
    exchange_name VARCHAR(256)
);

CREATE TABLE public.interest_rates (
    id INT IDENTITY(0, 1) PRIMARY KEY,
    "year" INT NOT NULL,
    "month" INT NOT NULL,
    "day" INT NOT NULL,
    effective_rates FLOAT NOT NULL,
    "timestamp" DATE NOT NULL
);

CREATE TABLE public.news (
    id INT IDENTITY(0, 1) PRIMARY KEY,
    "timestamp" DATE NOT NULL,
    news VARCHAR(4096)
);

CREATE TABLE public.time (
    id INT IDENTITY(0, 1) PRIMARY KEY,
    "timestamp" DATE NOT NULL,
    "day" INT NOT NULL,
    "month" INT NOT NULL,
    "year" INT NOT NULL
);

CREATE TABLE public.stock_analysis (
    id INT IDENTITY(0, 1) PRIMARY KEY,
    time_fk INT REFERENCES "time",
    interest_rates_fk INT REFERENCES interest_rates,
    symbol_fk INT REFERENCES symbols,
    news_fk INT REFERENCES news,
    top_performing_stock_diff FLOAT NOT NULL
)

CREATE TABLE public.staging_stocks (
    "timestamp" DATE NOT NULL,
    volume INT NOT NULL,
    "open" FLOAT,
    "close" FLOAT,
    "high" FLOAT,
    "low" FLOAT,
    adjclose FLOAT,
    symbol VARCHAR(256),
    diff FLOAT NOT NULL
)
