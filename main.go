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

	tk := generateTranslationKey()
	t := generateTranslation(tk)
	saveTranslation(session, t)
	saveTranslationKey(session, tk)
	b := queryTranslationsByEntryLocale(session, "mobile", "es")
	// fmt.Printf("%#v", b)

	by, _ := json.MarshalIndent(b, "", "  ")
	fmt.Printf("%s", by)
}

func queryTranslationsByEntryLocale(session *gocql.Session, entryType, locale string) []models.Bundle {
	var bundles []models.Bundle
	var bundle models.Bundle
	var translation models.MinimalTranslation

	t := time.Now()
	iter := session.Query(`SELECT translation_key_name, value FROM translations_by_entry_type_locale WHERE entry_type = ? AND locale = ?`,
		entryType, locale).Iter()

	for iter.Scan(&translation.TranslationKeyName, &translation.Value) {
		bundle = models.Bundle{
			EntryType:    entryType,
			Locale:       locale,
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

func saveTranslationKey(session *gocql.Session, tk models.TranslationKey) {
	if err := session.Query(`INSERT INTO translation_keys (name, entry_type) VALUES (?, ?)`,
		tk.Name, tk.EntryType).Exec(); err != nil {
		log.Println("Insert error in translation_keys table:", err)
	}
}

func saveTranslation(session *gocql.Session, t models.Translation) {
	if err := session.Query(`INSERT INTO translations (translation_key_name, value, locale, entry_type) VALUES (?, ?, ?, ?)`,
		t.TranslationKeyName, t.Value, t.Locale, t.EntryType).Exec(); err != nil {
		log.Println("Insert error in translations table:", err)
	}
}

func generateTranslationKey() models.TranslationKey {
	return models.TranslationKey{
		Name:      "TermsAndConditions",
		EntryType: "mobile",
	}
}

func generateTranslation(tk models.TranslationKey) models.Translation {
	return models.Translation{
		TranslationKeyName: tk.Name,
		Value:              "Términos y condiciones",
		Locale:             "es",
		EntryType:          tk.EntryType,
	}
}

// import (
// 	"fmt"
// 	"log"

// 	"github.com/gocql/gocql"
// )

// func main() {
// 	// Connect to ScyllaDB
// 	cluster := gocql.NewCluster("127.0.0.1")
// 	cluster.Keyspace = "translations"
// 	cluster.Consistency = gocql.Quorum
// 	session, err := cluster.CreateSession()
// 	if err != nil {
// 		log.Fatal("Unable to connect to ScyllaDB:", err)
// 	}
// 	defer session.Close()

// 	// Seed data
// 	seedData(session)

// 	// Query data based on entry_type and locale
// 	entryType := "mobile"
// 	locale := "en"
// 	translations := queryTranslationsByEntryLocale(session, entryType, locale)
// 	for _, translation := range translations {
// 		fmt.Printf("ID: %s, KeyID: %d, Value: %s, EntryType: %s, Locale: %s\n",
// 			translation.id, translation.keyID, translation.value, translation.entryType, translation.locale)
// 	}
// }

// func seedData(session *gocql.Session) {
// 	translations := []struct {
// 		keyID     int
// 		value     string
// 		entryType string
// 		locale    string
// 	}{
// 		{1, "Hello", "mobile", "en"},
// 		{1, "Hola", "mobile", "es"},
// 		{2, "Goodbye", "web", "en"},
// 	}

// 	for _, t := range translations {
// 		id := gocql.TimeUUID()
// 		// Insert into translations table
// 		if err := session.Query(`INSERT INTO translations (id, key_id, value, entry_type, locale) VALUES (?, ?, ?, ?, ?)`,
// 			id, t.keyID, t.value, t.entryType, t.locale).Exec(); err != nil {
// 			log.Println("Insert error in translations table:", err)
// 		}

// 	}
// 	fmt.Println("Seeding complete")
// }

// type Translation struct {
// 	id        gocql.UUID
// 	keyID     int
// 	value     string
// 	entryType string
// 	locale    string
// }

// func queryTranslationsByEntryLocale(session *gocql.Session, entryType, locale string) []Translation {
// 	var translations []Translation
// 	var translation Translation

// 	iter := session.Query(`SELECT id, key_id, value, entry_type, locale FROM translations_by_entry_locale WHERE entry_type = ? AND locale = ?`,
// 		entryType, locale).Iter()

// 	for iter.Scan(&translation.id, &translation.keyID, &translation.value, &translation.entryType, &translation.locale) {
// 		t := translation
// 		translations = append(translations, t)
// 	}

// 	if err := iter.Close(); err != nil {
// 		log.Println("Query error:", err)
// 	}

// 	return translations
// }
