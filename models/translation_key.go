package models

// TranslationKey represents the translation_keys table.
type TranslationKey struct {
	ID        uint   `gorm:"primaryKey;autoIncrement"`
	Name      string `gorm:"type:varchar(100)"`
	EntryType string `gorm:"type:varchar(10)"`
}
