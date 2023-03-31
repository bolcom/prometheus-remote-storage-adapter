package mysql

type Column struct {
	Column string `yaml:"column"`
	Type   string `yaml:"type"`
	Size   int    `yaml:"size"`
}

type Schema struct {
	SchemaName    string   `yaml:"schema_name"`
	SchemaColumns []Column `yaml:"schema_columns"`
}

type Schemas struct {
	Schemas []Schema `yaml:"schemas"`
}
