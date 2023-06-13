package json

import (
	"bytes"
	"encoding/json"
)

func Marshal(v interface{}) ([]byte, error) {
	var d bytes.Buffer
	enc := json.NewEncoder(&d)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return d.Bytes(), nil
}
