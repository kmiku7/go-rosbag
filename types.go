package go_rosbag

import (
	"errors"
	"math"
)

type OpCodeType uint8

const (
	VersionV20 string = "#ROSBAG V2.0"

	FileHeaderLength uint32 = 4096

	IndexVersion      uint32 = 1
	ChunkIndexVersion uint32 = 1

	OpMsgDef     OpCodeType = 0x01
	OpMsgData    OpCodeType = 0x02
	OpFileHeader OpCodeType = 0x03
	OpIndexData  OpCodeType = 0x04
	OpChunk      OpCodeType = 0x05
	OpChunkInfo  OpCodeType = 0x06
	OpConnection OpCodeType = 0x07

	KeyOp string = "op"

	KeyTopic             string = "topic"
	KeyMessageType       string = "type"
	KeyMD5SUM            string = "md5sum"
	KeyMessageDefinition string = "message_definition"

	KeyIndexPosition   string = "index_pos"
	KeyConnectionID    string = "conn"
	KeyConnectionCount string = "conn_count"
	KeyChunkCount      string = "chunk_count"
	KeyChunkPosition   string = "chunk_pos"

	KeyTime      string = "time"
	KeyStartTime string = "start_time"
	KeyEndTime   string = "end_time"

	KeyCompressionMethod string = "compression"
	KeySize              string = "size"
	KeyVersion           string = "ver"
	KeyCount             string = "count"
)

var (
	ErrorVersionWrited    = errors.New("File version was writed.")
	ErrorVersionNotWrited = errors.New("File Version is not writed.")
	ErrorHeaderWrited     = errors.New("File header was writed.")
	ErrorHeaderNotWrited  = errors.New("File header is not writed.")
	ErrorChunkOpened      = errors.New("Chunk has opened.")
	ErrorChunkNotOpened   = errors.New("Chunk has not opened.")

	ErrorWriterFinished = errors.New("Writer was finished.")

	ErrorInvalidArgumentCount = errors.New("Invalid argument count.")
)

type RecordHeaderType map[string]interface{}

func NewRecordHeader(args ...interface{}) (header RecordHeaderType, err error) {
	if len(args)%2 != 0 {
		err = ErrorInvalidArgumentCount
		return
	}
	header = make(RecordHeaderType)
	for idx := 0; idx < len(args); idx += 2 {
		key := args[idx].(string)
		header[key] = args[idx+1]
	}
	return
}

type RosTopicClassType struct {
	Topic      string
	TypeName   string
	Md5sum     string
	Definition string
}

type IndexEntry200 struct {
	TimestampNs uint64
	Offset      uint32
}

type ChunkInfoType struct {
	Version          uint32
	ChunkPosition    uint64
	StartTimeNs      uint64
	EndTimeNs        uint64
	ConnectionCounts map[uint32]uint32
	ConnectionIndexs map[uint32][]IndexEntry200
}

func NewChunkInfo() *ChunkInfoType {
	return &ChunkInfoType{
		Version:          ChunkIndexVersion,
		ChunkPosition:    0,
		StartTimeNs:      math.MaxUint64,
		EndTimeNs:        0,
		ConnectionCounts: make(map[uint32]uint32),
		ConnectionIndexs: make(map[uint32][]IndexEntry200),
	}
}
