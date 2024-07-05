- docker-compose up -d
- docker-compose exec scylla cqlsh

```sql
CREATE TABLE translation_keys (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    entry_type VARCHAR(10)
);

CREATE TABLE translations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    translation_key_id INT,
    row_value TEXT,
    calc_value TEXT,
    locale_code VARCHAR(50),
    FOREIGN KEY (translation_key_id) REFERENCES translation_keys(id)
);

CREATE TABLE countries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    title VARCHAR(100) NOT NULL,
    title_local VARCHAR(100),
    default_locale VARCHAR(10),
    currency VARCHAR(50),
    is_disabled BOOLEAN NOT NULL
);

CREATE TABLE languages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    title VARCHAR(100) NOT NULL,
    title_local VARCHAR(100)
);

CREATE TABLE locales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    language_code VARCHAR(10) NOT NULL,
    fallback VARCHAR(10),
    FOREIGN KEY (country_code) REFERENCES countries(code),
    FOREIGN KEY (language_code) REFERENCES languages(code)
);
```
