package parsers

type csvParser struct {
}

func NewCsvParser() csvParser {
	return csvParser{}
}

func (p csvParser) Parse(input string) string {
	return input
}
