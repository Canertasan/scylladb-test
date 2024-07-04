package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"scylladb-test/models"
	"sync"
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

	// // Kafka Setup
	// log.Println("Connecting to Kafka...")
	// // Producer
	// p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9094"})
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer p.Close()
	// log.Println("Kafka Producer created.")

	// // Consumer
	// consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers": "localhost:9094",
	// 	"group.id":          "example_group",
	// 	"auto.offset.reset": "earliest", // Start reading from the earliest message
	// })

	// if err != nil {
	// 	log.Fatalf("Failed to create consumer: %s", err)
	// }

	// defer consumer.Close()
	// log.Println("Kafka Consumer created.")

	// // Subscribe
	// topic := "update_child_translations"
	// err = consumer.Subscribe(topic, nil)
	// if err != nil {
	// 	log.Fatalf("Failed to subscribe to topic: %s", err)
	// }

	// log.Printf("Consumer subscribed to topic %s", topic)

	// // Seed data
	// baseLocale := generateBaseLocale()
	// saveBaseLocale(session, baseLocale)
	// seedLocaleData(session)

	// Seed a large number of translations
	// seedMultipleTranslations(session, p, 10000) // 10k translation_keys and 60k translations

	entry_type := "12"
	locale_code := "en-GB"

	b, duration := queryTranslationsByEntryLocaleCode(session, entry_type, locale_code)

	// by, _ := json.MarshalIndent(b, "", "  ")
	// fmt.Printf("%s", by)
	fmt.Printf("Query execution time: %s\n", duration)
	fmt.Printf("Query parameters: entry_type = %s, locale_code = %s\n", entry_type, locale_code)
	fmt.Print("Number of bundles: ", len(b), "\n")

	// consumeKafkaMessages(consumer, p, session)
}

func queryTranslationsByEntryLocaleCode(session *gocql.Session, entryType, locale_code string) ([]models.Bundle, time.Duration) {
	var bundles []models.Bundle
	var bundle models.Bundle
	var translation models.MinimalTranslation

	start := time.Now()
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

	duration := time.Since(start) // Calculate the duration after query execution

	if err := iter.Close(); err != nil {
		log.Println("Query error:", err)
	}

	return bundles, duration
}

func seedLocaleData(session *gocql.Session) {
	// Define one language
	language := generateLanguage("en", "English", "English")
	saveLanguage(session, language)

	// Define multiple countries
	countries := []models.Country{
		generateCountry("NL", "Netherlands", "Nederland", "en-NL", "EUR", false),
		generateCountry("US", "United States", "United States", "en-US", "USD", false),
		generateCountry("GB", "United Kingdom", "United Kingdom", "en-GB", "GBP", false),
		generateCountry("CA", "Canada", "Canada", "en-CA", "CAD", false),
		generateCountry("AU", "Australia", "Australia", "en-AU", "AUD", false),
	}

	for _, country := range countries {
		saveCountry(session, country)

		// Generate and save the locale for each country-language combination
		locale := generateLocale(country, language)
		saveLocale(session, locale)
	}
}

func generateBaseLocale() models.Locale {
	return models.Locale{
		Code: "en",
	}
}

func generateCountry(code string, title string, titleLocale string, defaultLocale string, currency string, isDisabled bool) models.Country {
	return models.Country{
		Code:          code,
		Title:         title,
		TitleLocal:    titleLocale,
		DefaultLocale: defaultLocale,
		Currency:      currency,
		IsDisabled:    isDisabled,
	}
}

func generateLanguage(code string, title string, titleLocale string) models.Language {
	return models.Language{
		Code:       code,
		Title:      title,
		TitleLocal: titleLocale,
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

func seedMultipleTranslations(session *gocql.Session, p *kafka.Producer, count int) {
	// Start a timer to measure seeding duration
	start := time.Now()

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Set the number of workers (concurrent goroutines)
	numWorkers := 10
	wg.Add(numWorkers)

	// Channel to distribute work
	jobs := make(chan int, count)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				// Generate and insert a translation
				tk := generateTranslationKey(job)
				createTranslationKey(session, tk)
				t := generateBaseTranslation(tk, job)
				updateTranslation(session, p, t)
			}
		}()
	}

	// Enqueue jobs
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()

	// Print the duration of the seeding process
	log.Printf("Seeding completed in %s\n", time.Since(start))
}

func generateTranslationKey(jobID int) models.TranslationKey {
	return models.TranslationKey{
		Name:      fmt.Sprintf("TRANSLATION_KEY_%d", jobID),
		EntryType: "11", // or change it dynamically if needed
	}
}

func generateBaseTranslation(tk models.TranslationKey, jobID int) models.Translation {
	value := randomText(5000)
	return models.Translation{
		TranslationKeyName: tk.Name,
		RowValue:           value,
		CalcValue:          value,
		LocaleCode:         "en", // or use a dynamic locale code if needed
		EntryType:          tk.EntryType,
	}
}

// Function to generate random text of random length up to maxLength
func randomText(maxLength int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate a random length in the range [1, maxLength+1)
	length := seededRand.Intn(maxLength) + 1

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func createTranslationKey(session *gocql.Session, tk models.TranslationKey) {
	if err := session.Query(`INSERT INTO translation_keys (name, entry_type) VALUES (?, ?)`,
		tk.Name, tk.EntryType).Exec(); err != nil {
		log.Println("Insert error in translation_keys table:", err)
	}
}

func updateTranslation(session *gocql.Session, p *kafka.Producer, t models.Translation) {
	// Ensure calcValue matches rowValue if rowValue is not empty
	if t.RowValue != "" && t.CalcValue != t.RowValue {
		t.CalcValue = t.RowValue
		// raise an errors
	}

	// Attempt to update the translation in the database
	err := session.Query(`UPDATE translations SET row_value = ?, calc_value = ?, entry_type = ? WHERE translation_key_name = ? AND locale_code = ?`,
		t.RowValue, t.CalcValue, t.EntryType, t.TranslationKeyName, t.LocaleCode).Exec()
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

	// if no child translations found, create new translations
	if len(childTranslations) == 0 && len(fallbackCodes) > 0 {
		// create new translations
		for _, code := range fallbackCodes {
			// create new translation
			newTranslation := models.Translation{
				TranslationKeyName: t.TranslationKeyName,
				RowValue:           t.CalcValue,
				CalcValue:          t.CalcValue,
				LocaleCode:         code,
				EntryType:          t.EntryType,
			}
			// push to kafka
			pushKafkaMessage(p, newTranslation)
		}
	} else {
		// if child translations found, update them
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
