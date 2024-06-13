package models

type Translation struct {
	// ID        gocql.UUID
	TranslationKeyName string
	Value              string
	Locale             string
	EntryType          string
}

type MinimalTranslation struct {
	TranslationKeyName string
	Value              string
}

type Bundle struct {
	EntryType    string
	Locale       string
	Translations []MinimalTranslation
}
