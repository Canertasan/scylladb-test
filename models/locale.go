package models

// Locale represents the locales table.
type Locale struct {
	ID           uint     `gorm:"primaryKey;autoIncrement"`
	Code         string   `gorm:"type:varchar(10);unique;not null"`
	CountryCode  string   `gorm:"type:varchar(10);not null"`
	Country      Country  `gorm:"foreignKey:CountryCode;references:Code"`
	LanguageCode string   `gorm:"type:varchar(10);not null"`
	Language     Language `gorm:"foreignKey:LanguageCode;references:Code"`
	Fallback     string   `gorm:"type:varchar(10)"`
}
