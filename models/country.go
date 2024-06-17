package models

type Country struct {
	Code          string
	Title         string
	TitleLocal    string
	DefaultLocale string
	Currency      string
	IsDisabled    bool
}
