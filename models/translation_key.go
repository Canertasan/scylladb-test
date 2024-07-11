package models

type TranslationKey struct {
	// ID        gocql.UUID
	ID        uint   `gorm:"primaryKey;autoIncrement"`
	Name      string `gorm:"type:varchar(100)"`
	EntryType string `gorm:"type:varchar(10)"`
}
