package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "modernc.org/sqlite"
	"os/exec"
	"time"
)

func main() {
	runJournaldPipeline()
}

func runJournaldPipeline() error {
	stdout, err := exec.Command("journalctl", "-b", "-o", "json", "--no-pager").Output()
	if err != nil {
		return err
	}
	fmt.Printf("%s", stdout)
	return nil
}

const (
	HIGH int = iota + 1
	MEDIUM
	LOW
)

type JournaldLog map[string]json.RawMessage

func (l JournaldLog) GetString(key string) (string, bool) {
	raw, exists := l[key]
	if !exists {
		return "", false
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

type Event struct {
	Timestamp   time.Time
	Host        string
	Module      OSModule
	Application Application
	Severity    byte
	Message     string
	BootID      string
}

type OSModule struct {
	name string
}
type Application struct {
	name string
}

func InsertApplications(db *sql.DB, injects []Application) error {
	stmt, err := db.Prepare(`
			INSERT INTO applications (
				name
			)
			VALUES (?)

			ON CONFLICT(id) DO UPDATE SET
				name = excluded.name
		`)
	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, inject := range injects {
		_, err := stmt.Exec(
			inject.name,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertOSModules(db *sql.DB, injects []OSModule) error {
	stmt, err := db.Prepare(`
			INSERT INTO os_modules (
				name
			)
			VALUES (?)

			ON CONFLICT(id) DO UPDATE SET
				name = excluded.name
		`)
	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, inject := range injects {
		_, err := stmt.Exec(
			inject.name,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertEvent(db *sql.DB, injects []Event) error {

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

	defer stmt.Close()

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

func CreateSchema(db *sql.DB) error {
	_, err := db.Exec(`
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
