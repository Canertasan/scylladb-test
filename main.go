package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"scylladb-test/models"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Connect to MySQL
	dsn := "root:@tcp(127.0.0.1:3306)/localisation"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Unable to connect to MySQL:", err)
	}
	defer db.Close()

	// Verify connection to the database
	err = db.Ping()
	if err != nil {
		log.Fatal("Unable to ping the database:", err)
	}

	// Kafka Setup
	log.Println("Connecting to Kafka...")
	// Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	log.Println("Kafka Producer created.")

	// // Consumer
	// consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers": "localhost:9092",
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
	// saveLocale(db, baseLocale)
	// seedLocaleData(db)

	// Seed a large number of translations
	// seedMultipleTranslations(db, p, 10000) // 10k translation_keys and 60k translations

	bundles, duration := queryTranslationsByEntryLocaleCode(db, "11", "en-NL")

	bundlesJson, _ := json.MarshalIndent(bundles, "", "  ")
	fmt.Printf("%s", bundlesJson)
	fmt.Printf("Query execution time: %s\n", duration)

	// consumeKafkaMessages(consumer, p, db)
}

func queryTranslationsByEntryLocaleCode(db *sql.DB, entryType, localeCode string) ([]models.Bundle, time.Duration) {
	var bundles []models.Bundle
	var translation models.MinimalTranslation
	translationMap := make(map[string]*models.Bundle)

	start := time.Now()
	rows, err := db.Query(`
		SELECT tk.name, t.calc_value
		FROM translations t
		JOIN translation_keys tk ON t.translation_key_id = tk.id
		WHERE tk.entry_type = ? AND t.locale_code = ?
	`, entryType, localeCode)
	if err != nil {
		log.Fatal("Query error:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var keyName, value string
		if err := rows.Scan(&keyName, &value); err != nil {
			log.Fatal("Scan error:", err)
		}

		translation = models.MinimalTranslation{
			TranslationKeyName: keyName,
			Value:              value,
		}

		if bundle, exists := translationMap[entryType]; exists {
			bundle.Translations = append(bundle.Translations, translation)
		} else {
			bundle := &models.Bundle{
				EntryType:    entryType,
				LocaleCode:   localeCode,
				Translations: []models.MinimalTranslation{translation},
			}
			translationMap[entryType] = bundle
		}
	}
	duration := time.Since(start) // Calculate the duration after query execution

	if err := rows.Err(); err != nil {
		log.Fatal("Rows error:", err)
	}

	for _, bundle := range translationMap {
		bundles = append(bundles, *bundle)
	}

	return bundles, duration
}

func seedLocaleData(db *sql.DB) {
	// Define one language
	language := generateLanguage("en", "English", "English")
	saveLanguage(db, language)

	// Define multiple countries
	countries := []models.Country{
		generateCountry("NL", "Netherlands", "Nederland", "en-NL", "EUR", false),
		generateCountry("US", "United States", "United States", "en-US", "USD", false),
		generateCountry("GB", "United Kingdom", "United Kingdom", "en-GB", "GBP", false),
		generateCountry("CA", "Canada", "Canada", "en-CA", "CAD", false),
		generateCountry("AU", "Australia", "Australia", "en-AU", "AUD", false),
	}

	for _, country := range countries {
		saveCountry(db, country)

		// Generate and save the locale for each country-language combination
		locale := generateLocale(country, language)
		saveLocale(db, locale)
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

func saveCountry(db *sql.DB, country models.Country) {
	_, err := db.Exec(`INSERT INTO countries (code, title, title_local, default_locale, currency, is_disabled) VALUES (?, ?, ?, ?, ?, ?)`,
		country.Code, country.Title, country.TitleLocal, country.DefaultLocale, country.Currency, country.IsDisabled)
	if err != nil {
		log.Println("Insert error in countries table:", err)
	}
}

func saveLanguage(db *sql.DB, language models.Language) {
	_, err := db.Exec(`INSERT INTO languages (code, title, title_local) VALUES (?, ?, ?)`,
		language.Code, language.Title, language.TitleLocal)
	if err != nil {
		log.Println("Insert error in languages table:", err)
	}
}

func saveLocale(db *sql.DB, locale models.Locale) {
	_, err := db.Exec(`INSERT INTO locales (country_code, language_code, code, fallback) VALUES (?, ?, ?, ?)`,
		locale.CountryCode, locale.LanguageCode, locale.Code, locale.Fallback)
	if err != nil {
		log.Println("Insert error in locales table:", err)
	}
}

func seedMultipleTranslations(db *sql.DB, p *kafka.Producer, count int) {
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
				tk = createTranslationKey(db, tk)
				// tkMas, _ := json.MarshalIndent(tk, "", "  ")
				// fmt.Printf("%s", tkMas)
				t := generateBaseTranslation(tk, job)
				updateTranslation(db, p, t)
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
		TranslationKeyID: tk.ID,
		TranslationKey:   tk,
		RowValue:         value,
		CalcValue:        value,
		LocaleCode:       "en-NL", // or use a dynamic locale code if needed
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

func createTranslationKey(db *sql.DB, tk models.TranslationKey) models.TranslationKey {
	result, err := db.Exec(`INSERT INTO translation_keys (name, entry_type) VALUES (?, ?)`,
		tk.Name, tk.EntryType)
	if err != nil {
		log.Println("Insert error in translation_keys table:", err)
		return tk // Return the original tk on error
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		log.Println("Error getting last insert ID:", err)
		return tk // Return the original tk on error
	}

	tk.ID = uint(lastInsertID) // Convert lastInsertID to uint before assigning it to tk.ID
	return tk
}

func updateTranslation(db *sql.DB, p *kafka.Producer, t models.Translation) {
	// Ensure calcValue matches rowValue if rowValue is not empty
	if t.RowValue != "" && t.CalcValue != t.RowValue {
		t.CalcValue = t.RowValue
		// raise an errors
	}
	// tMas, _ := json.MarshalIndent(t, "", "  ")
	// fmt.Printf("%s", tMas)
	// Attempt to perform an upsert operation in the database
	_, err := db.Exec(`
	INSERT INTO translations (translation_key_id, locale_code, row_value, calc_value)
	VALUES (?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
	row_value = VALUES(row_value),
	calc_value = VALUES(calc_value)`,
		t.TranslationKeyID, t.LocaleCode, t.RowValue, t.CalcValue)
	if err != nil {
		log.Printf("Upsert error in translations table: %v", err)
		return // Skip Kafka push if save failed
	}

	// // Query to fetch fallback locale codes
	// var fallbackCodes []string
	// rows, err := db.Query(`SELECT code FROM locales WHERE fallback = ?`, t.LocaleCode)
	// if err != nil {
	// 	log.Fatal("Query error:", err)
	// }
	// defer rows.Close()

	// var code string
	// for rows.Next() {
	// 	if err := rows.Scan(&code); err != nil {
	// 		log.Fatal("Scan error:", err)
	// 	}
	// 	fallbackCodes = append(fallbackCodes, code)
	// }

	// if err := rows.Err(); err != nil {
	// 	log.Fatal("Rows error:", err)
	// }

	// // If no fallback codes were found, return early
	// if len(fallbackCodes) == 0 {
	// 	return
	// }

	// log.Printf("Fallback codes: %v", fallbackCodes)

	// // Select all translations with the same translation key name and any of the fallback locale codes
	// rows, err = db.Query(`SELECT locale_code, translation_key_ID, row_value, calc_value FROM translations WHERE translation_key_id = ? AND locale_code IN (?)`,
	// 	t.TranslationKey.ID, fallbackCodes)
	// if err != nil {
	// 	log.Fatal("Query error:", err)
	// }
	// defer rows.Close()

	// var childTranslations []models.Translation
	// var tempTranslation models.Translation
	// for rows.Next() {
	// 	if err := rows.Scan(&tempTranslation.LocaleCode, &tempTranslation.TranslationKey.Name, &tempTranslation.RowValue, &tempTranslation.CalcValue); err != nil {
	// 		log.Fatal("Scan error:", err)
	// 	}
	// 	childTranslations = append(childTranslations, tempTranslation)
	// }

	// if err := rows.Err(); err != nil {
	// 	log.Fatal("Rows error:", err)
	// }

	// log.Printf("Fallback translations: %v", childTranslations)

	// // if no child translations found, create new translations
	// if len(childTranslations) == 0 && len(fallbackCodes) > 0 {
	// 	// create new translations
	// 	for _, code := range fallbackCodes {
	// 		// create new translation
	// 		newTranslation := models.Translation{
	// 			TranslationKeyID: t.TranslationKeyID,
	// 			RowValue:         t.CalcValue,
	// 			CalcValue:        t.CalcValue,
	// 			LocaleCode:       code,
	// 		}
	// 		// push to kafka
	// 		pushKafkaMessage(p, newTranslation)
	// 	}
	// } else {
	// 	// if child translations found, update them
	// 	// Push message to Kafka for each fallback code to update child translations
	// 	for _, childTranslation := range childTranslations {
	// 		if childTranslation.RowValue == "" && childTranslation.CalcValue != t.RowValue {
	// 			log.Printf("Since row value is empty, updating child translation via kafka: %v", childTranslation)
	// 			// update calculated value
	// 			childTranslation.CalcValue = t.RowValue
	// 			pushKafkaMessage(p, childTranslation)
	// 		} else {
	// 			if childTranslation.RowValue != "" {
	// 				log.Printf("Skipping update child translation since row value is not empty")
	// 			} else if childTranslation.CalcValue == t.CalcValue {
	// 				log.Printf("Skipping update child translation since calc value is the same")
	// 			}
	// 		}
	// 	}
	// }
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
		Key:            []byte(translation.TranslationKey.Name),
		Value:          translationBytes,
	}, nil)
	if err != nil {
		log.Println("Kafka push error:", err)
	}
}

func consumeKafkaMessages(consumer *kafka.Consumer, producer *kafka.Producer, db *sql.DB) {
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

		updateTranslation(db, producer, translation)
		log.Printf("Processed message:  translation = %s", translation)
	}
}
