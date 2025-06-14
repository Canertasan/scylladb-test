package models

type Translation struct {
	// ID        gocql.UUID
	TranslationKeyName string
	RowValue           string
	CalcValue          string
	LocaleCode         string
	EntryType          string
	EntryID            string
}

type MinimalTranslation struct {
	TranslationKeyName string
	Value              string
}

type Bundle struct {
	EntryType    string
	LocaleCode   string
	Translations []MinimalTranslation
}
