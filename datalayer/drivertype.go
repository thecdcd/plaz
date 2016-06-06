package datalayer

const (
	EXECUTOR_LOST_NAME = "execlost"
	SLAVE_LOST_NAME = "slavelost"
	OFFER_NAME = "offer"
)

type DataFields      map[string]interface{}
type DataTags        map[string]string

type DataConfig struct {
	address  string
	username string
	password string
	database string
}

type DataDriver interface {
	Connect() error

	StartBatch() (string, error)

	CreatePoint(string, DataTags, DataFields) DataFields

	RecordPoint(string, DataFields) error

	WriteBatch(string) error

	Close() error
}

func NewDataConfig(address string, database string, username string, password string) *DataConfig {
	return &DataConfig{
		database: database,
		username: username,
		password: password,
		address: address,
	}
}

