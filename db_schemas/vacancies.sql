CREATE TABLE IF NOT EXISTS vacancies
(
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    date_posted           TEXT,    -- Оригинальная дата, например "3 декабря"
    date_posted_timestamp DATE,    -- Дата в формате timestamp (2025-12-03)
    company_name          TEXT,
    company_rating        REAL,
    vacancy_title         TEXT,
    location              TEXT,
    employment_type       TEXT,
    remote_option         BOOLEAN,

    -- Поля для зарплаты
    salary_text           TEXT,
    salary_min            INTEGER,
    salary_max            INTEGER,
    salary_currency       TEXT,
    is_exact_salary       BOOLEAN, -- True = точная зарплата, False = похожие специалисты

    skills                TEXT,

    --Поля для GigaChat
    match_score           TEXT      DEFAULT NULL,
    is_relevant           BOOLEAN   DEFAULT NULL,
    missing_skills        TEXT      DEFAULT NULL,
    redundant_skills      TEXT      DEFAULT NULL,
    analysis              TEXT      DEFAULT NULL,
    recommendations       TEXT      DEFAULT NULL,

    scraped_date          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
