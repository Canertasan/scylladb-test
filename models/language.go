package models

type Language struct {
	ID         uint   `gorm:"primaryKey;autoIncrement"`
	Code       string `gorm:"type:varchar(10);unique;not null"`
	Title      string `gorm:"type:varchar(100);not null"`
	TitleLocal string `gorm:"type:varchar(100)"`
}
