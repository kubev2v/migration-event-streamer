package clients

import (
	"database/sql"
	"fmt"

	_ "github.com/Kount/pq-timeouts"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type PgConnection struct {
	g2 *gorm.DB
	db *sql.DB
}

func (c *PgConnection) DB() *gorm.DB {
	return c.g2
}

func (c *PgConnection) Close() {
	c.db.Close()
}

type PostgresEnvConfig struct {
	Host     string `envconfig:"POSTGRES_HOST" default:"localhost"`
	Port     int    `envconfig:"POSTGRES_PORT" default:"5432"`
	DB       string `envconfig:"POSTGRES_DB" default:"planner"`
	Username string `envconfig:"POSTGRES_USERNAME" default:"admin"`
	Password string `envconfig:"POSTGRES_PASSWORD" default:"adminpass"`
}

func (p PostgresEnvConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		p.Host, p.Port, p.Username, p.Password, p.DB,
	)
}

func NewPgConnectionFromEnv() (*PgConnection, error) {
	config, err := getConfigFromEnv[PostgresEnvConfig]()
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres configuration: %w", err)
	}

	// Open connection to DB via standard library
	db, err := sql.Open("postgres", config.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf(
			"SQL failed to connect to database %s with connection string: %s: %w",
			config.DB,
			config.ConnectionString(),
			err,
		)
	}

	db.SetMaxOpenConns(10)
	// Connect GORM to use the same connection
	conf := &gorm.Config{
		PrepareStmt:            false,
		FullSaveAssociations:   false,
		SkipDefaultTransaction: true,
		Logger:                 logger.Default.LogMode(logger.Silent),
	}
	g2, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 db,
		PreferSimpleProtocol: true,
	}), conf)
	if err != nil {
		return nil, fmt.Errorf(
			"GORM failed to connect to database %s with connection string: %s: %w",
			config.DB,
			config.ConnectionString(),
			err,
		)
	}

	zap.S().Infof("connected to postgres: %s", config.ConnectionString())

	return &PgConnection{db: db, g2: g2}, nil
}
