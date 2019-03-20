package go_rosbag

import (
	"bytes"
	"encoding/binary"
	"io"
)

func maxUint64(left, right uint64) uint64 {
	if left < right {
		return right
	}
	return left
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func packTimeInV200(timestampNs uint64) []byte {
	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, uint32(timestampNs/1000000000))
	binary.Write(&buffer, binary.LittleEndian, uint32(timestampNs%1000000000))
	return buffer.Bytes()
}

func writeSized(file io.Writer, data []byte) (n int, err error) {
	if err = binary.Write(file, binary.LittleEndian, uint32(len(data))); err != nil {
		return
	}
	if n, err = file.Write(data); err != nil {
		return
	}
	n = n + 4
	return
}

func writeHeader(file io.Writer, header RecordHeaderType) (n int, err error) {
	var buffer bytes.Buffer
	for key, value := range header {
		if err = binary.Write(
			&buffer, binary.LittleEndian, uint32(len(key)+1+binary.Size(value))); err != nil {
			return
		}
		if _, err = buffer.WriteString(key); err != nil {
			return
		}
		if err = buffer.WriteByte('='); err != nil {
			return
		}
		if err = binary.Write(&buffer, binary.LittleEndian, value); err != nil {
			return
		}
	}
	n, err = writeSized(file, buffer.Bytes())
	return
}

func writeRecord(
	file io.Writer, header RecordHeaderType,
	data []byte, paddedSize uint32,
) (n int, err error) {

	var headerLength int = 0
	var bodyLength int = 0
	if headerLength, err = writeHeader(file, header); err != nil {
		return
	}

	if paddedSize > 0 {
		if headerLength < int(paddedSize + 4) {
			data = bytes.Repeat([]byte{' '}, int(paddedSize+4)-headerLength)
		} else {
			data = nil
		}
	}
	bodyLength, err = writeSized(file, data)
	if err != nil {
		return
	}
	return headerLength + bodyLength, nil
}
