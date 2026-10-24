package main

import (
	// "fmt"
	"time"
)

func main() {

	// CLI argument parser
	//
	// decision tree and business logic command invocations

}

func runJournaldPipeline() {
	// extract_journald_logs()
}
func extractJournaldCmd() journaldLog {
	// exec_journald_command()
	//json.Unmarshal()
}

func execJournaldCmd(args ...string) string {

}

func normalizeJournaldLogs(logs []journaldLog) {

}

func write_to_db(work unit_of_work) {

}

type journaldLogRaw struct {
	_REALTIME_TIMESTAMP string
	_HOSTNAME           string
	_SYSTEMD_UNIT       string
	PRIORITY            byte
	MESSAGE             string
	_BOOT_ID            int
}

type journaldLog struct {
	Date        string
	host        string
	module      osModule
	application application
	prioroty    byte
	message     string
	boot        int
}

type osModule struct {
	name string
}
type application struct {
	name string
}

type unit_of_work struct {
}
