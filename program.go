package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"time"

	_ "modernc.org/sqlite"
)

const DBPath string = "data/logoracle.db"

// const (
// 	EMERG byte = iota
// 	ALERT
// 	CRIT
// 	ERR
// 	WARNING
// 	NOTICE
// 	INFO
// 	DEBUG
// )

type JournaldLog map[string]json.RawMessage

type Event struct {
	Timestamp time.Time
	Host      string
	// Module      OSModule
	// Application Application
	Module      string
	Application string
	// Severity    byte
	Severity string
	Message  string
	BootID   string
}

// type OSModule struct {
// 	name string
// }
// type Application struct {
// 	name string
// }

type Store struct {
	db *sql.DB
}

type PipelineCtx struct {
	store    *Store
	provider string
}

type SQLQuery struct {
	Select  string
	From    string
	Where   string
	OrderBy string
	Limit   string
}

func main() {

	if err := runJournaldPipeline(); err != nil {
		log.Fatal(err)
	}

}

func runJournaldPipeline() error {
	db, err := OpenDatabase(DBPath)
	if err != nil {
		return err
	}

	defer func() {
		_ = db.Close()
	}()

	ctx := &PipelineCtx{
		store:    &Store{db: db},
		provider: "journald",
	}

	cursor, err := ctx.getIngestionState()
	if err != nil {
		return err
	}

	logs, err := extractJournaldLogs(cursor)
	if err != nil {
		return err
	}
	events := normalizeJournaldLogs(logs)
	if err := ctx.store.persistBatch(events); err != nil {
		return err
	}

	return nil
}

func (c *PipelineCtx) getIngestionState() (string, error) {
	cursorSlice, err := c.store.ReadTableRows[string](
		SQLQuery{
			Select:  "last_cursor",
			From:    "ingestion_state",
			Where:   "",
			OrderBy: "updated_at",
			Limit:   "1",
		},
		func(rows *sql.Rows) (string, error) {
			var cursor string

			if err := rows.Scan(&cursor); err != nil {
				return "", err
			}

			return cursor, nil
		},
	)
	if err != nil {
		return "", err
	}
	if len(cursorSlice) < 1 || cursorSlice[0] == "" {
		return "", err
	}

	return cursorSlice[0], nil
}

func extractJournaldLogs(cursor string) ([]JournaldLog, error) {
	var (
		cmd  *exec.Cmd
		logs []JournaldLog
	)

	switch cursor {
	case "":
		cmd = exec.Command("journalctl", "-b", "-o", "json", "--no-pager")
	default:
		cmd = exec.Command("journalctl", "--no-pager", "-o", "json")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	if err := json.NewDecoder(stdout).Decode(&logs); err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	// fmt.Printf("%s", stdout)
	return logs, nil
}

func normalizeJournaldLogs(logs []JournaldLog) []Event {
	var events []Event

	for _, log := range logs {
		timestamp, err := time.Parse("yyyy-mm-dd hh:mm:ss", log.GetString("_REALTIME_TIMESTAMP"))
		if err != nil {
			timestamp = time.Time{}
		}
		module := log.GetString("")
		application := log.GetString("")
		severity := log.GetString("")

		event := Event{
			Timestamp:   timestamp,
			Host:        log.GetString("_HOSTNAME"),
			Module:      module,
			Application: application,
			Severity:    severity,
			Message:     log.GetString("MESSAGE"),
			BootID:      log.GetString("_BOOT_ID"),
		}

		events = append(events, event)
	}

	return events
}

func (s *Store) persistBatch(events []Event) error {
	if err := insertEvents(s.db, events); err != nil {
		return err
	}

	return nil
}

func (l JournaldLog) GetString(key string) string {
	raw, exists := l[key]
	if !exists {
		return ""
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return value
}

func insertEvents(db *sql.DB, injects []Event) error {
	stmt, err := db.Prepare(`
			INSERT INTO events (
				timestamp,
				host,
				module_id,
				application_id,
				severity,
				message,
				boot_id,
			)
			VALUES (?, ?, ?, ?, ?, ?, ?)

			ON CONFLICT(id) DO UPDATE SET
				timestamp      = excluded.timestamp,
				host           = excluded.host,
				module_id      = excluded.module_id,
				application_id = excluded.application_id,
				severity       = excluded.severity,
				message        = excluded.message,
				boot_id        = excluded.boot_id,
		`)
	if err != nil {
		return err
	}

	defer func() {
		_ = stmt.Close()
	}()

	for _, inject := range injects {
		_, err := stmt.Exec(
			inject.Timestamp,
			inject.Host,
			nil,
			nil,
			inject.Severity,
			inject.Message,
			inject.BootID,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func createSchema(db *sql.DB) error {
	_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS ingestion_state (
				source      PRIMARY KEY TEXT,
				last_cursor TEXT,
				updated_at  TEXT
			);
						
		`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS applications (
				id   INTEGER PRIMARY KEY,
				name TEXT
			);
						
		`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS os_modules (
				id   INTEGER PRIMARY KEY,
				name TEXT
			);
						
		`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS events (
				id             INTEGER PRIMARY KEY,
				timestamp      TEXT,
				host           TEXT,
				module_id      INTEGER,
				application_id INTEGER,
				severity       INTEGER,
				message        TEXT,
				boot_id        TEXT,

				FOREIGN KEY (module_id)
					REFERENCES modules(id),
				FOREIGN KEY (application_id)
					REFERENCES application(id)
			);
						
		`)
	if err != nil {
		return err
	}

	return nil
}

// func insertApplications(db *sql.DB, injects []Application) error {
// 	stmt, err := db.Prepare(`
// 			INSERT INTO applications (
// 				name
// 			)
// 			VALUES (?)

// 			ON CONFLICT(id) DO UPDATE SET
// 				name = excluded.name
// 		`)
// 	if err != nil {
// 		return err
// 	}

// 	defer func() {
// 		_ = stmt.Close()
// 	}()

// 	for _, inject := range injects {
// 		_, err := stmt.Exec(
// 			inject.name,
// 		)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func insertOSModules(db *sql.DB, injects []OSModule) error {
// 	stmt, err := db.Prepare(`
// 			INSERT INTO os_modules (
// 				name
// 			)
// 			VALUES (?)

// 			ON CONFLICT(id) DO UPDATE SET
// 				name = excluded.name
// 		`)
// 	if err != nil {
// 		return err
// 	}

// 	defer func() {
// 		_ = stmt.Close()
// 	}()

// 	for _, inject := range injects {
// 		_, err := stmt.Exec(
// 			inject.name,
// 		)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func (s *Store) ReadTableRows[T any](query SQLQuery, decode func(*sql.Rows) (T, error)) ([]T, error) {
	queryString := fmt.Sprintf(`
			SELECT   %s
			FROM     %s
		`, query.Select, query.From)
	if query.Where != "" {
		queryString = fmt.Sprintf("%s WHERE %s", queryString, query.Where)
	}
	if query.OrderBy != "" {
		queryString = fmt.Sprintf("%s ORDER BY %s", queryString, query.OrderBy)
	}
	if query.Limit != "" {
		queryString = fmt.Sprintf("%s LIMIT %s", queryString, query.Limit)
	}

	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	var result []T
	for rows.Next() {
		value, err := decode(rows)
		if err != nil {
			return nil, err
		}

		result = append(result, value)
	}
	return result, nil
}

func OpenDatabase(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		func() {
			_ = db.Close()
		}()
		return nil, err
	}

	if err := db.Ping(); err != nil {
		func() {
			_ = db.Close()
		}()
		return nil, err
	}

	return db, nil
}
