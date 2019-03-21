package rosbag

import (
	"errors"
)

var (
	ErrorInvalidCompressionMethod = errors.New("only support 'none' method")
)

type CompressionMethodType string

const (
	CompressionNone CompressionMethodType = "none"
	CompressionBZ2  CompressionMethodType = "bz2"
	CompressionLZ4  CompressionMethodType = "lz4"
)

type Compressor interface {
	Compress(originalData []byte) ([]byte, error)
	Decompress(compressedData []byte) ([]byte, error)
}

type PlainCompressor struct {
	name CompressionMethodType
}

func NewPlainCompressor() *PlainCompressor {
	return &PlainCompressor{
		name: CompressionNone,
	}
}

func (c *PlainCompressor) Compress(originalData []byte) ([]byte, error) {
	return originalData, nil
}

func (c *PlainCompressor) Decompress(compressedData []byte) ([]byte, error) {
	return compressedData, nil
}

func NewCompressor(method CompressionMethodType) (Compressor, error) {
	switch method {
	case CompressionNone:
		return NewPlainCompressor(), nil
	default:
		return nil, ErrorInvalidCompressionMethod
	}
}
