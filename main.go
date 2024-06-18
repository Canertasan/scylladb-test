package main

import (
	"encoding/json"
	"fmt"
	"log"
	"scylladb-test/models"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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

	// Kafka Setup
	log.Println("Connecting to Kafka...")
	// Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9094"})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	log.Println("Kafka Producer created.")

	// Consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "example_group",
		"auto.offset.reset": "earliest", // Start reading from the earliest message
	})

	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	defer consumer.Close()
	log.Println("Kafka Consumer created.")

	// Subscribe
	topic := "update_child_translations"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	log.Printf("Consumer subscribed to topic %s", topic)

	// Seed data
	seedLocaleData(session)
	seedTranslationAndTranslationKey(session, p)

	b := queryTranslationsByEntryLocaleCode(session, "11", "en-NL")

	by, _ := json.MarshalIndent(b, "", "  ")
	fmt.Printf("%s", by)

	consumeKafkaMessages(consumer, p, session)
}

func queryTranslationsByEntryLocaleCode(session *gocql.Session, entryType, locale_code string) []models.Bundle {
	var bundles []models.Bundle
	var bundle models.Bundle
	var translation models.MinimalTranslation

	t := time.Now()
	iter := session.Query(`SELECT translation_key_name, calc_value FROM translations_by_entry_type_locale_code WHERE entry_type = ? AND locale_code = ?`,
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

func seedTranslationAndTranslationKey(session *gocql.Session, p *kafka.Producer) {
	tk := generateTranslationKey()
	createTranslationKey(session, tk)

	t := generateBaseTranslation(tk)
	updateTranslation(session, p, t)
}

func generateTranslationKey() models.TranslationKey {
	return models.TranslationKey{
		Name:      "CATALOG_RULES",
		EntryType: "11",
	}
}

func generateBaseTranslation(tk models.TranslationKey) models.Translation {
	return models.Translation{
		TranslationKeyName: tk.Name,
		RowValue:           "CATALOG_RULES BASE",
		CalcValue:          "CATALOG_RULES BASE",
		LocaleCode:         "en",
		EntryType:          tk.EntryType,
	}
}

func createTranslationKey(session *gocql.Session, tk models.TranslationKey) {
	if err := session.Query(`INSERT INTO translation_keys (name, entry_type) VALUES (?, ?)`,
		tk.Name, tk.EntryType).Exec(); err != nil {
		log.Println("Insert error in translation_keys table:", err)
	}
	createEmptyTranslationsForAllLocale(session, tk)
}

func createEmptyTranslationsForAllLocale(session *gocql.Session, tk models.TranslationKey) {
	var locales []models.Locale
	iter := session.Query(`SELECT country_code, language_code, code, fallback FROM locales`).Iter()

	for {
		var locale models.Locale
		if !iter.Scan(&locale.CountryCode, &locale.LanguageCode, &locale.Code, &locale.Fallback) {
			break
		}
		locales = append(locales, locale)
	}
	if err := iter.Close(); err != nil {
		log.Println("Query error:", err)
	}

	for _, locale := range locales {
		if err := session.Query(`INSERT INTO translations (entry_type, locale_code, translation_key_name, calc_value) VALUES (?, ?, ?, ?)`,
			tk.EntryType, locale.Code, tk.Name, "").Exec(); err != nil {
			log.Println("Insert error in translations table:", err)
		}
	}
}

func updateTranslation(session *gocql.Session, p *kafka.Producer, t models.Translation) {
	// Ensure calcValue matches rowValue if rowValue is not empty
	if t.RowValue != "" && t.CalcValue != t.RowValue {
		t.CalcValue = t.RowValue
	}

	// Attempt to update the translation in the database
	err := session.Query(`UPDATE translations SET row_value = ?, calc_value = ? WHERE translation_key_name = ? AND locale_code = ?`,
		t.RowValue, t.CalcValue, t.TranslationKeyName, t.LocaleCode).Exec()
	if err != nil {
		log.Printf("Update error in translations table: %v", err)
		return // Skip Kafka push if save failed
	}

	// Query to fetch fallback locale codes
	var fallbackCodes []string
	iter := session.Query(`SELECT code FROM locales_by_fallback WHERE fallback = ?`, t.LocaleCode).Iter()
	defer iter.Close() // Ensure iterator is closed

	var code string
	for iter.Scan(&code) {
		fallbackCodes = append(fallbackCodes, code)
	}
	if err = iter.Close(); err != nil {
		log.Printf("Query iterator error: %v", err)
		return
	}

	// If no fallback codes were found, return early
	if len(fallbackCodes) == 0 {
		return
	}

	log.Printf("Fallback codes: %v", fallbackCodes)

	// Select all translations with the same translation key name and any of the fallback locale codes
	iter = session.Query(`SELECT entry_type, locale_code, translation_key_name, row_value, calc_value FROM translations WHERE translation_key_name = ? AND locale_code IN ?`,
		t.TranslationKeyName, fallbackCodes).Iter()
	defer iter.Close() // Ensure iterator is closed

	var childTranslations []models.Translation
	var tempTranslation models.Translation
	for iter.Scan(&tempTranslation.EntryType, &tempTranslation.LocaleCode, &tempTranslation.TranslationKeyName, &tempTranslation.RowValue, &tempTranslation.CalcValue) {
		childTranslations = append(childTranslations, tempTranslation)
	}
	if err = iter.Close(); err != nil {
		log.Printf("Query error: %v", err)
	}

	log.Printf("Fallback translations: %v", childTranslations)

	// Push message to Kafka for each fallback code to update child translations
	for _, childTranslation := range childTranslations {
		if childTranslation.RowValue == "" && childTranslation.CalcValue != t.RowValue {
			log.Printf("Since row value is empty, updating child translation via kafka: %v", childTranslation)
			// update calculated value
			childTranslation.CalcValue = t.RowValue
			pushKafkaMessage(p, childTranslation)
		} else {
			if childTranslation.RowValue != "" {
				log.Printf("Skipping update child translation since row value is not empty")
			} else if childTranslation.CalcValue == t.CalcValue {
				log.Printf("Skipping update child translation since calc value is the same")
			}
		}
	}
}

func pushKafkaMessage(p *kafka.Producer, translation models.Translation) {
	topic := "update_child_translations"
	translationBytes, err := json.Marshal(translation)
	if err != nil {
		log.Println("Error marshaling translation:", err)
		return
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(translation.TranslationKeyName),
		Value:          translationBytes,
	}, nil)
	if err != nil {
		log.Println("Kafka push error:", err)
	}
}

func consumeKafkaMessages(consumer *kafka.Consumer, producer *kafka.Producer, session *gocql.Session) {
	// Consume messages
	log.Println("Starting to consume Kafka messages...")
	for {
		log.Println("Waiting for new Kafka message...")
		msg, err := consumer.ReadMessage(-1) // -1 for no timeout (blocks indefinitely)
		if err != nil {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		var translation models.Translation
		err = json.Unmarshal(msg.Value, &translation)

		if err != nil {
			log.Println("Error unmarshaling message:", err)
			continue
		}
		log.Printf("Received message: translation = %s", translation)

		updateTranslation(session, producer, translation)
		log.Printf("Processed message:  translation = %s", translation)
	}
}
