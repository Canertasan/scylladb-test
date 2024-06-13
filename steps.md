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
    locale TEXT,
    entry_type TEXT,
    PRIMARY KEY (translation_key_name, locale)
);

CREATE MATERIALIZED VIEW translations_by_entry_type_locale AS
    SELECT * FROM translations
    WHERE entry_type IS NOT NULL AND locale IS NOT NULL 
    PRIMARY KEY ((entry_type, locale), translation_key_name);


//

CREATE TABLE translations (
    id UUID,
    key_id INT,
    value TEXT,
    entry_type TEXT,
    locale TEXT,
    PRIMARY KEY ((locale, id))
);

CREATE MATERIALIZED VIEW translations_by_entry_locale AS
    SELECT * FROM translations
    WHERE entry_type IS NOT NULL AND locale IS NOT NULL AND id IS NOT NULL
    PRIMARY KEY (entry_type, locale, id);
```