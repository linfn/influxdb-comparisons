package common

import (
	"bytes"
	"fmt"
	"io"
)

type SerializerKetaDB struct {
	buf []byte
}

func NewSerializerKetaDB() *SerializerKetaDB {
	return &SerializerKetaDB{
		buf: make([]byte, 0, 4096),
	}
}

// SerializePoint writes Point data to the given writer, conforming to the
// KetaDB JSON format.
//
// This function writes output that looks like:
// ...
func (s *SerializerKetaDB) SerializePoint(w io.Writer, p *Point) (err error) {
	timestamp := p.Timestamp.UTC().Unix()
	buf := s.buf[:0]

	buf = append(buf, "{"...)
	buf = append(buf, fmt.Sprintf("\"timestamp\":%d", timestamp*1000)...)
	buf = append(buf, fmt.Sprintf(",\"origin\":\"%s\"", p.MeasurementName)...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ",\""...)
		if bytes.Equal(p.TagKeys[i], []byte("hostname")) {
			buf = append(buf, "host"...)
		} else {
			buf = append(buf, p.TagKeys[i]...)
		}
		buf = append(buf, "\":\""...)
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, "\""...)
	}

	buf = append(buf, ",\"fields\":{"...)
	for i := 0; i < len(p.FieldKeys); i++ {
		if i > 0 {
			buf = append(buf, ","...)
		}
		buf = append(buf, "\""...)
		buf = append(buf, p.MeasurementName...)
		buf = append(buf, "_"...)
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, "\":"...)
		buf = fastFormatAppend(p.FieldValues[i], buf, false)
	}
	buf = append(buf, "}"...)

	buf = append(buf, "}\n"...)
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (s *SerializerKetaDB) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}
