package parsers

type Parser interface {
	Parse(input string) string
}
