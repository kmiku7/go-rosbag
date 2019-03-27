package rosbag

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/OneOfOne/xxhash"
)

type BagFileStreamingWriter struct {
	file              io.Writer
	compressionMethod CompressionMethodType
	compressor        Compressor

	versionWrited bool
	headerWrited  bool
	chunkOpened   bool
	writeFinished bool

	allChunkInfo []*ChunkInfoType

	chunkCompressedSize   uint32
	chunkUncompressedSize uint32
	chunkInfo             *ChunkInfoType

	connectionIdGenerator *IdGenerator
	allConnectionInfo     map[uint32]*RosTopicClassType

	hasher *xxhash.XXHash32
}

func NewBagFileStreamingWriter(
	file io.Writer, compressionMethod CompressionMethodType,
) (writer *BagFileStreamingWriter, err error) {

	if !(compressionMethod == CompressionNone || compressionMethod == CompressionLZ4) {
		return nil, ErrorInvalidCompressionMethod
	}
	compressor, err := NewCompressor(compressionMethod)
	if err != nil {
		return
	}
	writer = &BagFileStreamingWriter{
		file:              file,
		compressionMethod: compressionMethod,
		compressor:        compressor,

		versionWrited: false,
		headerWrited:  false,
		chunkOpened:   false,
		writeFinished: false,

		chunkCompressedSize:   0,
		chunkUncompressedSize: 0,
		chunkInfo:             nil,

		connectionIdGenerator: NewIdGenerator(),
		allConnectionInfo:     make(map[uint32]*RosTopicClassType),

		hasher: xxhash.New32(),
	}
	return
}

func (w *BagFileStreamingWriter) WriteVersion() (err error) {
	if w.versionWrited {
		err = ErrorVersionWrited
		return
	}
	w.versionWrited = true
	w.file.Write([]byte(VersionV20))
	w.file.Write([]byte{'\n'})
	return
}

func (w *BagFileStreamingWriter) writeHeader(
	indexPosition uint64, chunkCount uint32, connectionCount uint32,
) (err error) {

	header, err := NewRecordHeader(
		KeyOp, OpFileHeader,
		KeyIndexPosition, indexPosition,
		KeyConnectionCount, connectionCount,
		KeyChunkCount, chunkCount)
	if err != nil {
		return
	}

	_, err = writeRecord(w.file, header, nil, FileHeaderLength)
	if err != nil {
		return
	}
	return
}

func (w *BagFileStreamingWriter) WriteHeader(
	hintIndexPosition uint64, hintChunkCount uint32, hintConnectionCount uint32,
) error {

	if !w.versionWrited {
		return ErrorVersionNotWrited
	}
	if w.headerWrited {
		return ErrorHeaderWrited
	}
	w.headerWrited = true
	return w.writeHeader(hintIndexPosition, hintChunkCount, hintConnectionCount)
}

func (w *BagFileStreamingWriter) RewriteHeader(
	indexPosition uint64, chunkCount uint32, connectionCount uint32,
) error {

	if !w.versionWrited {
		return ErrorVersionNotWrited
	}
	return w.writeHeader(indexPosition, chunkCount, connectionCount)
}

func (w *BagFileStreamingWriter) writeChunkHeader(
	uncompressedSize uint32, compressedSize uint32,
) (err error) {

	header, err := NewRecordHeader(
		KeyOp, OpChunk,
		KeyCompressionMethod, []byte(w.compressionMethod),
		KeySize, uncompressedSize)
	if err != nil {
		return
	}
	_, err = writeHeader(w.file, header)
	if err != nil {
		return
	}
	err = binary.Write(w.file, binary.LittleEndian, compressedSize)
	return
}

func (w *BagFileStreamingWriter) OpenChunk(
	hintUncompressedSize uint32, hintCompressedSize uint32,
	hintOffset uint64,
) (err error) {
	if !w.headerWrited {
		return ErrorHeaderNotWrited
	}
	if w.chunkOpened {
		return ErrorChunkOpened
	}
	w.chunkOpened = true
	w.chunkCompressedSize = 0
	w.chunkUncompressedSize = 0
	w.chunkInfo = NewChunkInfo()
	w.chunkInfo.ChunkPosition = hintOffset

	err = w.writeChunkHeader(hintUncompressedSize, hintCompressedSize)

	if w.compressionMethod == CompressionLZ4 {
		_, err = w.file.Write([]byte{0x04, 0x22, 0x4d, 0x18, 0x64, 0x60, 0x85})
		if err != nil {
			panic(err)
		}
	}
	w.chunkCompressedSize += 7
	return
}

func (w *BagFileStreamingWriter) RewriteChunkHeader(
	uncompressedSize uint32, compressedSize uint32,
) error {
	return w.writeChunkHeader(uncompressedSize, compressedSize)
}

func (w *BagFileStreamingWriter) writeConnectionRecord(
	writer io.Writer,
	connectionId uint32, topicClass *RosTopicClassType,
) (writeLength int, err error) {
	headerHeader, err := NewRecordHeader(
		KeyOp, OpConnection,
		KeyTopic, []byte(topicClass.Topic),
		KeyConnectionID, connectionId,
	)
	if err != nil {
		return
	}
	var headerLength int
	var bodyLength int
	if headerLength, err = writeHeader(writer, headerHeader); err != nil {
		return
	}

	bodyHeader, err := NewRecordHeader(
		KeyTopic, []byte(topicClass.Topic),
		KeyMessageType, []byte(topicClass.TypeName),
		KeyMD5SUM, []byte(topicClass.Md5sum),
		KeyMessageDefinition, []byte(topicClass.Definition),
	)
	if err != nil {
		return
	}
	if bodyLength, err = writeHeader(writer, bodyHeader); err != nil {
		return
	}
	writeLength = headerLength + bodyLength
	return
}

func (w *BagFileStreamingWriter) updateChunkInfo(
	connectionId uint32,
	messageTimestampNs uint64,
	messageOffset uint32,
) {
	// TODO: assert is chronologically.
	w.chunkInfo.StartTimeNs = minUint64(
		w.chunkInfo.StartTimeNs,
		messageTimestampNs,
	)
	w.chunkInfo.EndTimeNs = maxUint64(
		w.chunkInfo.EndTimeNs,
		messageTimestampNs,
	)

	w.chunkInfo.ConnectionCounts[connectionId] += 1
	w.chunkInfo.ConnectionIndexs[connectionId] = append(
		w.chunkInfo.ConnectionIndexs[connectionId],
		IndexEntry200{
			TimestampNs: messageTimestampNs,
			Offset:      w.chunkCompressedSize,
		},
	)
}

func (w *BagFileStreamingWriter) updateChunkLength(writeLength int) {
	w.chunkCompressedSize += uint32(writeLength)
	w.chunkUncompressedSize += uint32(writeLength)
}

func (w *BagFileStreamingWriter) WriteMessage(
	timestampNs uint64, topicClass *RosTopicClassType, messageBody []byte,
) (err error) {
	if !w.chunkOpened {
		return ErrorChunkNotOpened
	}
	if w.compressionMethod != CompressionNone {
		return ErrorInvalidCompressionMethod
	}
	connectionId := w.connectionIdGenerator.GetUint32Id(topicClass.Topic)
	var writeLength int

	if _, has := w.allConnectionInfo[connectionId]; !has {
		tmpTopicClass := *topicClass
		w.allConnectionInfo[connectionId] = &tmpTopicClass
		writeLength, err = w.writeConnectionRecord(w.file, connectionId, &tmpTopicClass)
		if err != nil {
			return
		}
		w.updateChunkLength(writeLength)
	}

	w.updateChunkInfo(connectionId, timestampNs, w.chunkCompressedSize)
	// TODO: do not support compression now.
	msgHeader, err := NewRecordHeader(
		KeyOp, OpMsgData,
		KeyConnectionID, connectionId,
		KeyTime, packTimeInV200(timestampNs),
	)
	writeLength, err = writeRecord(w.file, msgHeader, messageBody, 0)
	if err != nil {
		return
	}
	w.updateChunkLength(writeLength)
	return
}

func (w *BagFileStreamingWriter) WriteRawMessage(
	timestampNs uint64, topicClass *RosTopicClassType,
	compressedMessageBody []byte, originalSize uint32, originalMessageBody []byte,
) (err error) {
	if !w.chunkOpened {
		return ErrorChunkNotOpened
	}
	if w.compressionMethod != CompressionLZ4 {
		return ErrorInvalidCompressionMethod
	}
	connectionId := w.connectionIdGenerator.GetUint32Id(topicClass.Topic)
	var writeLength int

	if _, has := w.allConnectionInfo[connectionId]; !has {
		tmpTopicClass := *topicClass
		w.allConnectionInfo[connectionId] = &tmpTopicClass
		var buffer bytes.Buffer
		writeLength, err = w.writeConnectionRecord(&buffer, connectionId, &tmpTopicClass)
		if err != nil {
			return
		}
		if writeLength != buffer.Len() {
			panic("buffer len not equal to connection function len.")
		}
		uncompressedSize := writeLength
		compressedSize := 4 + writeLength
		w.hasher.Write(buffer.Bytes())
		err = binary.Write(w.file, binary.LittleEndian, uint32(uncompressedSize)|0x80000000)
		if err != nil {
			panic(err)
		}
		_, err = w.file.Write(buffer.Bytes())
		if err != nil {
			panic(err)
		}

		// w.updateChunkLength(writeLength)
		w.chunkCompressedSize += uint32(compressedSize)
		w.chunkUncompressedSize += uint32(uncompressedSize)
	}

	w.updateChunkInfo(connectionId, timestampNs, w.chunkUncompressedSize)
	// TODO: do not support compression now.
	msgHeader, err := NewRecordHeader(
		KeyOp, OpMsgData,
		KeyConnectionID, connectionId,
		KeyTime, packTimeInV200(timestampNs),
	)
	var headerBuffer bytes.Buffer
	// writeLength, err = writeRecord(w.file, msgHeader, compressedMessageBody, 0)
	headerLen, err := writeHeader(&headerBuffer, msgHeader)
	if err != nil {
		return
	}
	if headerLen != headerBuffer.Len() {
		panic("buffer len not equal to message function len.")
	}

	// err = binary.Write(w.file, binary.LittleEndian, uint32(headerLen)|0x80000000)
	// if err != nil {
	// 	panic(err)
	// }
	// w.file.Write(buffer.Bytes())

	if originalSize == 0 {
		// uncompressed

		dataLen := uint32(len(compressedMessageBody))
		err = binary.Write(&headerBuffer, binary.LittleEndian, dataLen)
		if err != nil {
			panic(err)
		}

		err = binary.Write(w.file, binary.LittleEndian, uint32(headerBuffer.Len())|0x80000000)
		if err != nil {
			panic(err)
		}
		w.hasher.Write(headerBuffer.Bytes())
		_, err = w.file.Write(headerBuffer.Bytes())
		if err != nil {
			panic(err)
		}

		err = binary.Write(w.file, binary.LittleEndian, dataLen|0x80000000)
		if err != nil {
			panic(err)
		}
		w.hasher.Write(compressedMessageBody)
		_, err = w.file.Write(compressedMessageBody)
		if err != nil {
			panic(err)
		}

		uncompressedSize := uint32(headerBuffer.Len()) + dataLen
		compressedSize := uncompressedSize + 4 + 4

		// w.updateChunkLength(writeLength)
		w.chunkCompressedSize += uint32(compressedSize)
		w.chunkUncompressedSize += uint32(uncompressedSize)
	} else {
		// compressed

		dataLen := uint32(originalSize)
		err = binary.Write(&headerBuffer, binary.LittleEndian, dataLen)
		if err != nil {
			panic(err)
		}

		err = binary.Write(w.file, binary.LittleEndian, uint32(headerBuffer.Len())|0x80000000)
		if err != nil {
			panic(err)
		}
		w.hasher.Write(headerBuffer.Bytes())
		_, err = w.file.Write(headerBuffer.Bytes())
		if err != nil {
			panic(err)
		}

		err = binary.Write(w.file, binary.LittleEndian, uint32(len(compressedMessageBody[4:]))|0x80000000)
		if err != nil {
			panic(err)
		}
		w.hasher.Write(originalMessageBody)
		_, err = w.file.Write(compressedMessageBody[4:])
		if err != nil {
			panic(err)
		}

		uncompressedSize := uint32(headerBuffer.Len()) + dataLen
		compressedSize := uint32(headerBuffer.Len()) + 4 + uint32(len(compressedMessageBody))

		// w.updateChunkLength(writeLength)
		w.chunkCompressedSize += uint32(compressedSize)
		w.chunkUncompressedSize += uint32(uncompressedSize)
	}

	// w.updateChunkLength(writeLength)
	return
}

func (w *BagFileStreamingWriter) ChunkInfo() (*ChunkInfoType, uint32, uint32) {
	return w.chunkInfo, w.chunkCompressedSize, w.chunkUncompressedSize
}

func (w *BagFileStreamingWriter) CloseChunk(checksum uint32) (err error) {
	if !w.chunkOpened {
		return ErrorChunkNotOpened
	}

	if w.compressionMethod == CompressionLZ4 {
		_, err = w.file.Write([]byte{0, 0, 0, 0})
		if err != nil {
			panic(err)
		}
		_ = checksum
		err = binary.Write(w.file, binary.LittleEndian, w.hasher.Sum32())
		if err != nil {
			panic(err)
		}
		w.chunkUncompressedSize += 8
		w.chunkCompressedSize += 8
	}

	w.chunkOpened = false
	// Writes connection indexes.
	for connectionId, indexes := range w.chunkInfo.ConnectionIndexs {
		indexHeader, err := NewRecordHeader(
			KeyOp, OpIndexData,
			KeyConnectionID, connectionId,
			KeyVersion, IndexVersion,
			KeyCount, uint32(len(indexes)),
		)
		if err != nil {
			return err
		}
		var buffer bytes.Buffer
		for _, index := range indexes {
			err = binary.Write(&buffer, binary.LittleEndian, packTimeInV200(index.TimestampNs))
			if err != nil {
				panic(err)
			}
			err = binary.Write(&buffer, binary.LittleEndian, index.Offset)
			if err != nil {
				panic(err)
			}
		}
		_, err = writeRecord(w.file, indexHeader, buffer.Bytes(), 0)
		if err != nil {
			return err
		}
	}
	w.chunkInfo.ConnectionIndexs = nil
	w.allChunkInfo = append(w.allChunkInfo, w.chunkInfo)
	return
}

func (w *BagFileStreamingWriter) writeChunkInfo(
	chunkInfo *ChunkInfoType) (writeLength int, err error) {

	header, err := NewRecordHeader(
		KeyOp, OpChunkInfo,
		KeyVersion, chunkInfo.Version,
		KeyChunkPosition, chunkInfo.ChunkPosition,
		KeyStartTime, packTimeInV200(chunkInfo.StartTimeNs),
		KeyEndTime, packTimeInV200(chunkInfo.EndTimeNs),
		KeyCount, uint32(len(chunkInfo.ConnectionCounts)),
	)
	if err != nil {
		return
	}

	var buffer bytes.Buffer
	for connectionId, count := range chunkInfo.ConnectionCounts {
		binary.Write(&buffer, binary.LittleEndian, connectionId)
		binary.Write(&buffer, binary.LittleEndian, count)
	}

	writeLength, err = writeRecord(w.file, header, buffer.Bytes(), 0)
	return
}

func (w *BagFileStreamingWriter) FinishWrite() (err error) {
	if w.chunkOpened {
		return ErrorChunkOpened
	}
	if w.writeFinished {
		return ErrorWriterFinished
	}
	w.writeFinished = true
	for connectionId, topicClass := range w.allConnectionInfo {
		_, err = w.writeConnectionRecord(w.file, connectionId, topicClass)
		if err != nil {
			return
		}
	}

	for _, chunkInfo := range w.allChunkInfo {
		_, err = w.writeChunkInfo(chunkInfo)
		if err != nil {
			return
		}
	}
	return
}
