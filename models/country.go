package models

// Country represents the countries table.
type Country struct {
	ID            uint   `gorm:"primaryKey;autoIncrement"`
	Code          string `gorm:"type:varchar(10);unique;not null"`
	Title         string `gorm:"type:varchar(100);not null"`
	TitleLocal    string `gorm:"type:varchar(100)"`
	DefaultLocale string `gorm:"type:varchar(10)"`
	Currency      string `gorm:"type:varchar(50)"`
	IsDisabled    bool   `gorm:"not null"`
}
