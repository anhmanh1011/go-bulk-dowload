package state

import "time"

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusInProgress JobStatus = "in_progress"
	StatusDone       JobStatus = "done"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	MsgID          int64
	ChatID         int64
	ChatAccessHash int64
	FileID         int64
	AccessHash     int64
	FileReference  []byte
	DCID           int
	Size           int64
	FileName       string
	MimeType       string
	Status         JobStatus
	Retries        int
	OutputPath     string
	ErrorMsg       string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Stats struct {
	Pending    int64
	InProgress int64
	Done       int64
	Failed     int64
	TotalSize  int64
	DoneSize   int64
}
