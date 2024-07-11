package main

import (
	"context"
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

	// Perform benchmarking
	const numWorkers = 60
	const ratePerSecond = 60 // 60 requests per second per worker
	var wg sync.WaitGroup

	jobs := make(chan int, numWorkers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize workers and rate limiter
	initializeWorkers(ctx, db, numWorkers, ratePerSecond, jobs, &wg)

	// Create and enqueue jobs for benchmarking
	enqueueJobs(jobs, numWorkers*5) // Adjust the number of jobs as needed

	wg.Wait()
	fmt.Println("All benchmarking jobs completed.")
}

// Query translations by entry type and locale code
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
		if err := rows.Scan(&translation.TranslationKeyName, &translation.Value); err != nil {
			return nil, 0
		}

		bundle := models.Bundle{
			EntryType:    entryType,
			LocaleCode:   localeCode,
			Translations: []models.MinimalTranslation{translation},
		}
		bundles = append(bundles, bundle)
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

// Worker function to process jobs
func worker(ctx context.Context, db *sql.DB, jobs <-chan int, wg *sync.WaitGroup, rateLimiter <-chan time.Time) {
	defer wg.Done()
	entryTypes := []string{"11", "12", "13", "14"}

	for job := range jobs {
		select {
		case <-ctx.Done():
			return
		case <-rateLimiter: // Wait for the rate limiter signal
			entryType := entryTypes[rand.Intn(len(entryTypes))]
			localeCode := "en-fr"

			// Query translations
			result, duration := queryTranslationsByEntryLocaleCode(db, entryType, localeCode)

			// Process result if necessary
			fmt.Printf("Job %d with entryType %s completed in %v with %d bundles\n", job, entryType, duration, len(result))
		}
	}
}

// Function to create and enqueue jobs
func enqueueJobs(jobs chan<- int, numJobs int) {
	for j := 0; j < numJobs; j++ {
		jobs <- j
	}
	close(jobs) // Close the jobs channel when done
}

// Function to initialize workers and the rate limiter
func initializeWorkers(ctx context.Context, db *sql.DB, numWorkers int, ratePerSecond int, jobs chan int, wg *sync.WaitGroup) {
	rateLimiter := time.Tick(time.Second / time.Duration(ratePerSecond))

	// Start up your workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, db, jobs, wg, rateLimiter)
	}
}

// Continue with your other functions...

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
	locales := getAllUniqueLocales(db)
	// Start workers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				// generate multiple entry_type
				entryTypes := []string{"11", "12", "13", "14"}
				// loop over entryType
				for _, entryType := range entryTypes {
					// Generate and insert a translation
					tk := generateTranslationKey(job, entryType)
					tk = createTranslationKey(db, tk)
					for _, locale := range locales {
						t := generateBaseTranslation(tk, locale.Code)
						updateTranslation(db, p, t)
					}
				}
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

func generateTranslationKey(jobID int, entryType string) models.TranslationKey {
	return models.TranslationKey{
		Name:      fmt.Sprintf("TRANSLATION_KEY_%s_%d", entryType, jobID),
		EntryType: entryType, // or change it dynamically if needed
	}
}

func generateBaseTranslation(tk models.TranslationKey, localeCode string) models.Translation {
	value := randomText(5000)
	return models.Translation{
		TranslationKeyID: tk.ID,
		TranslationKey:   tk,
		RowValue:         value,
		CalcValue:        value,
		LocaleCode:       localeCode, // or use a dynamic locale code if needed
	}
}

func getAllUniqueLocales(db *sql.DB) []models.Locale {
	var locales []models.Locale

	rows, err := db.Query(`SELECT country_code, language_code, code, fallback FROM locales`)
	if err != nil {
		log.Println("Query error:", err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var locale models.Locale
		if err := rows.Scan(&locale.CountryCode, &locale.LanguageCode, &locale.Code, &locale.Fallback); err != nil {
			log.Println("Scan error:", err)
			continue
		}
		locales = append(locales, locale)
	}

	if err := rows.Err(); err != nil {
		log.Println("Rows error:", err)
	}

	return locales
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
