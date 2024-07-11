package models

type Translation struct {
	// ID        gocql.UUID
	ID               uint           `gorm:"primaryKey;autoIncrement"`
	TranslationKeyID uint           `gorm:"not null"`
	TranslationKey   TranslationKey `gorm:"foreignKey:TranslationKeyID;references:ID"`
	RowValue         string         `gorm:"type:text"`
	CalcValue        string         `gorm:"type:text"`
	LocaleCode       string         `gorm:"type:varchar(50)"`
}

type MinimalTranslation struct {
	TranslationKeyName string `json:"translation_key_name"`
	Value              string `json:"value"`
}

type Bundle struct {
	EntryType    string               `json:"entry_type"`
	LocaleCode   string               `json:"locale_code"`
	Translations []MinimalTranslation `json:"translations"`
}
