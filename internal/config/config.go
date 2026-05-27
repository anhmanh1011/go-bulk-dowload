package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Account       AccountConfig      `yaml:"account"`
	SourceChannel int64              `yaml:"source_channel"`
	TargetChannel int64              `yaml:"target_channel"`
	Fetcher       FetcherConfig      `yaml:"fetcher"`
	Splitter      SplitterConfig     `yaml:"splitter"`
	Processor     ProcessorConfig    `yaml:"processor"`
	Writer        WriterConfig       `yaml:"writer"`
	Uploader      UploaderConfig     `yaml:"uploader"`
	Backpressure  BackpressureConfig `yaml:"backpressure"`
	State         StateConfig        `yaml:"state"`
	Logging       LoggingConfig      `yaml:"logging"`
}

type AccountConfig struct {
	APIID       int    `yaml:"api_id"`
	APIHash     string `yaml:"api_hash"`
	SessionFile string `yaml:"session_file"`
}

type FetcherConfig struct {
	Sessions           int `yaml:"sessions"`
	ChunkSizeBytes     int `yaml:"chunk_size_bytes"`
	ChunkChannelCap    int `yaml:"chunk_channel_cap"`
	JobChannelCap      int `yaml:"job_channel_cap"`
	MaxRetriesPerChunk int `yaml:"max_retries_per_chunk"`
	MaxRetriesPerJob   int `yaml:"max_retries_per_job"`
}

type SplitterConfig struct {
	Workers        int `yaml:"workers"`
	LineChannelCap int `yaml:"line_channel_cap"`
}

type ProcessorConfig struct {
	Workers          int `yaml:"workers"`
	RecordChannelCap int `yaml:"record_channel_cap"`
}

type WriterConfig struct {
	OutputDir        string `yaml:"output_dir"`
	BatchSizeMB      int    `yaml:"batch_size_mb"`
	FlushIntervalSec int    `yaml:"flush_interval_sec"`
	OutputChannelCap int    `yaml:"output_channel_cap"`
}

type UploaderConfig struct {
	Sessions         int `yaml:"sessions"`
	ParallelParts    int `yaml:"parallel_parts"`
	UploadChannelCap int `yaml:"upload_channel_cap"`
}

type BackpressureConfig struct {
	MaxPendingOutputFiles int `yaml:"max_pending_output_files"`
}

type StateConfig struct {
	DBPath string `yaml:"db_path"`
}

type LoggingConfig struct {
	Level               string `yaml:"level"`
	Format              string `yaml:"format"`
	ProgressIntervalSec int    `yaml:"progress_interval_sec"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return &c, nil
}

func (c *Config) Validate() error {
	var errs []error
	if c.Account.APIID == 0 {
		errs = append(errs, errors.New("account.api_id must be set"))
	}
	if c.Account.APIHash == "" {
		errs = append(errs, errors.New("account.api_hash must be set"))
	}
	if c.Account.SessionFile == "" {
		errs = append(errs, errors.New("account.session_file must be set"))
	}
	if c.SourceChannel == 0 {
		errs = append(errs, errors.New("source_channel must be set"))
	}
	if c.TargetChannel == 0 {
		errs = append(errs, errors.New("target_channel must be set"))
	}
	if c.Fetcher.Sessions < 1 {
		errs = append(errs, errors.New("fetcher.sessions must be >= 1"))
	}
	if c.Fetcher.ChunkSizeBytes <= 0 {
		errs = append(errs, errors.New("fetcher.chunk_size_bytes must be > 0"))
	}
	if c.Uploader.Sessions < 1 {
		errs = append(errs, errors.New("uploader.sessions must be >= 1"))
	}
	if c.Writer.BatchSizeMB <= 0 {
		errs = append(errs, errors.New("writer.batch_size_mb must be > 0"))
	}
	if c.State.DBPath == "" {
		errs = append(errs, errors.New("state.db_path must be set"))
	}
	if c.Logging.Format != "json" && c.Logging.Format != "text" {
		errs = append(errs, fmt.Errorf("logging.format must be 'json' or 'text', got %q", c.Logging.Format))
	}
	return errors.Join(errs...)
}
