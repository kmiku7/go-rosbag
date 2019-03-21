package rosbag

import (
	"errors"
	"math"
)

const (
	VersionV20 string = "#ROSBAG V2.0"

	FileHeaderLength uint32 = 4096

	IndexVersion      uint32 = 1
	ChunkIndexVersion uint32 = 1

	OpMsgDef     uint8 = 0x01
	OpMsgData    uint8 = 0x02
	OpFileHeader uint8 = 0x03
	OpIndexData  uint8 = 0x04
	OpChunk      uint8 = 0x05
	OpChunkInfo  uint8 = 0x06
	OpConnection uint8 = 0x07

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
	ErrorVersionWrited    = errors.New("file version was writed")
	ErrorVersionNotWrited = errors.New("file Version is not writed")
	ErrorHeaderWrited     = errors.New("file header was writed")
	ErrorHeaderNotWrited  = errors.New("file header is not writed")
	ErrorChunkOpened      = errors.New("chunk has opened")
	ErrorChunkNotOpened   = errors.New("chunk has not opened")

	ErrorWriterFinished = errors.New("writer was finished")

	ErrorInvalidArgumentCount = errors.New("invalid argument count")
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
