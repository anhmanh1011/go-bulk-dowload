package types

// Chunk is a 1MB raw-bytes block fetched from Telegram (output of Stage 1 Fetcher).
//
// Contract with Splitter (Stage 2):
//   - Seq=0 marks the first chunk of a source file → Splitter drops bytes before the first '\n'.
//   - IsLast=true marks the final chunk → Splitter drops bytes after the last '\n' and any carried remainder.
//   - Chunks for a single MsgID arrive in Seq order (Fetcher emits them sequentially).
type Chunk struct {
	MsgID  int64  // source message ID — propagated for tracking
	Seq    int    // sequence within source file (0-based); load-bearing for edge-drop logic
	Data   []byte // raw bytes, ≤ chunk_size_bytes
	IsLast bool   // last chunk of source file (load-bearing for tail-edge drop)
}

// Line is the output of Stage 2 (Splitter). Wraps raw line bytes with source MsgID
// so downstream Writer can track which sources contributed to each output file.
// Line.Data is a fresh allocation (not aliased to any chunk buffer) — safe to retain.
type Line struct {
	MsgID int64
	Data  []byte
}

// Record is the output of Stage 3 (Processor) — a parsed credential.
type Record struct {
	MsgID int64
	Email []byte
	Pass  []byte
}

// OutputFile is the output of Stage 4 (Writer) — a finalized batch file ready to upload.
type OutputFile struct {
	Path         string
	LineCount    int
	SizeBytes    int64
	BatchSeq     int
	SourceMsgIDs []int64 // dedup'd list of source MsgIDs contributing to this file
}
