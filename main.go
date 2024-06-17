package main

import (
	"encoding/json"
	"fmt"
	"log"
	"scylladb-test/models"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	// Connect to ScyllaDB
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "localisation"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Unable to connect to ScyllaDB:", err)
	}
	defer session.Close()

	// Seed data
	seedTranslationAndTranslationKey(session)
	seedLocaleData(session)

	b := queryTranslationsByEntryLocaleCode(session, "11", "en-NL")

	by, _ := json.MarshalIndent(b, "", "  ")
	fmt.Printf("%s", by)
}

func queryTranslationsByEntryLocaleCode(session *gocql.Session, entryType, locale_code string) []models.Bundle {
	var bundles []models.Bundle
	var bundle models.Bundle
	var translation models.MinimalTranslation

	t := time.Now()
	iter := session.Query(`SELECT translation_key_name, value FROM translations_by_entry_type_locale_code WHERE entry_type = ? AND locale_code = ?`,
		entryType, locale_code).Iter()

	for iter.Scan(&translation.TranslationKeyName, &translation.Value) {
		bundle = models.Bundle{
			EntryType:    entryType,
			LocaleCode:   locale_code,
			Translations: []models.MinimalTranslation{translation},
		}
		bundles = append(bundles, bundle)
	}
	fmt.Println(time.Since(t))

	if err := iter.Close(); err != nil {
		log.Println("Query error:", err)
	}

	return bundles
}

func seedLocaleData(session *gocql.Session) {
	baseLocale := generateBaseLocale()
	country := generateCountry()
	language := generateLanguage()
	locale := generateLocale(country, language)
	saveBaseLocale(session, baseLocale)
	saveCountry(session, country)
	saveLanguage(session, language)
	saveLocale(session, locale)
}

func generateBaseLocale() models.Locale {
	return models.Locale{
		Code: "en",
	}
}

func generateCountry() models.Country {
	return models.Country{
		Code:          "NL",
		Title:         "Netherlands",
		TitleLocal:    "Nederland",
		DefaultLocale: "en-NL",
		Currency:      "EUR",
		IsDisabled:    false,
	}
}

func generateLanguage() models.Language {
	return models.Language{
		Code:       "en",
		Title:      "English",
		TitleLocal: "English",
	}
}

func generateLocale(country models.Country, language models.Language) models.Locale {
	return models.Locale{
		CountryCode:  country.Code,
		LanguageCode: language.Code,
		Code:         country.DefaultLocale,
		Fallback:     "en",
	}
}

func saveBaseLocale(session *gocql.Session, baseLocale models.Locale) {
	if err := session.Query(`INSERT INTO locales (country_code, language_code, code, fallback) VALUES (?, ?, ?, ?)`,
		baseLocale.CountryCode, baseLocale.LanguageCode, baseLocale.Code, baseLocale.Fallback).Exec(); err != nil {
		log.Println("Insert error in locales table:", err)
	}
}

func saveCountry(session *gocql.Session, country models.Country) {
	if err := session.Query(`INSERT INTO countries (code, title, title_local, default_locale, currency, is_disabled) VALUES (?, ?, ?, ?, ?, ?)`,
		country.Code, country.Title, country.TitleLocal, country.DefaultLocale, country.Currency, country.IsDisabled).Exec(); err != nil {
		log.Println("Insert error in countries table:", err)
	}
}

func saveLanguage(session *gocql.Session, language models.Language) {
	if err := session.Query(`INSERT INTO languages (code, title, title_local) VALUES (?, ?, ?)`,
		language.Code, language.Title, language.TitleLocal).Exec(); err != nil {
		log.Println("Insert error in languages table:", err)
	}
}

func saveLocale(session *gocql.Session, locale models.Locale) {
	if err := session.Query(`INSERT INTO locales (country_code, language_code, code, fallback) VALUES (?, ?, ?, ?)`,
		locale.CountryCode, locale.LanguageCode, locale.Code, locale.Fallback).Exec(); err != nil {
		log.Println("Insert error in locales table:", err)
	}
}

func seedTranslationAndTranslationKey(session *gocql.Session) {
	tk := generateTranslationKey()
	t := generateTranslation(tk)
	saveTranslation(session, t)
	saveTranslationKey(session, tk)
}

func generateTranslationKey() models.TranslationKey {
	return models.TranslationKey{
		Name:      "TERMS_AND_CONDITIONS",
		EntryType: "11",
	}
}

func generateTranslation(tk models.TranslationKey) models.Translation {
	return models.Translation{
		TranslationKeyName: tk.Name,
		Value:              "Terms and conditions",
		LocaleCode:         "en-NL",
		EntryType:          tk.EntryType,
	}
}

func saveTranslation(session *gocql.Session, t models.Translation) {
	if err := session.Query(`INSERT INTO translations (translation_key_name, value, locale_code, entry_type, entry_id) VALUES (?, ?, ?, ?, ?)`,
		t.TranslationKeyName, t.Value, t.LocaleCode, t.EntryType, t.EntryID).Exec(); err != nil {
		log.Println("Insert error in translations table:", err)
	}
}

func saveTranslationKey(session *gocql.Session, tk models.TranslationKey) {
	if err := session.Query(`INSERT INTO translation_keys (name, entry_type) VALUES (?, ?)`,
		tk.Name, tk.EntryType).Exec(); err != nil {
		log.Println("Insert error in translation_keys table:", err)
	}
}
