- docker-compose up -d
- docker-compose exec scylla cqlsh

```sql
CREATE KEYSPACE localisation WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE localisation;

CREATE TABLE translation_keys (
    name TEXT,
    entry_type TEXT,
    PRIMARY KEY (name)
);

CREATE TABLE translations (
    translation_key_name TEXT,
    value TEXT,
    locale_code TEXT,
    entry_type TEXT,
    entry_id TEXT,
    PRIMARY KEY (translation_key_name, locale_code)
);

CREATE MATERIALIZED VIEW translations_by_entry_type_locale_code AS
    SELECT * FROM translations
    WHERE entry_type IS NOT NULL AND locale_code IS NOT NULL 
    PRIMARY KEY ((entry_type, locale_code), translation_key_name);


CREATE TABLE countries (
    code TEXT PRIMARY KEY,
    title TEXT,
    title_local TEXT,
    default_locale TEXT,
    currency TEXT,
    is_disabled BOOLEAN,
);

CREATE TABLE languages (
    code TEXT PRIMARY KEY,
    title TEXT,
    title_local TEXT,
);

CREATE TABLE locales (
    code TEXT PRIMARY KEY,
    country_code TEXT,
    language_code TEXT,
    fallback TEXT,
);

CREATE MATERIALIZED VIEW locales_by_country_code AS
    SELECT * FROM locales
    WHERE country_code IS NOT NULL
    PRIMARY KEY (country_code, code);
```