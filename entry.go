package elastilog

import (
	"encoding/json"
	"time"
)

type Attributes map[string]interface{}

type Entry struct {
	Timestamp  time.Time  `json:"@timestamp"`
	Host       string     `json:"host"`
	Tags       []string   `json:"tags"`
	Log        string     `json:"log"`
	Attributes Attributes `json:"-"`
}

type EntryIndex struct {
	Index struct {
		Index string `json:"_index"`
		Type  string `json:"_type"`
	} `json:"index"`
}

func (e Entry) Index() EntryIndex {
	var ei EntryIndex
	ei.Index.Index = "logstash-" + e.Timestamp.Format("2006.01.02")
	ei.Index.Type = "logs"
	return ei
}

func (e Entry) BulkString() (string, error) {
	eBytes, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	if len(e.Attributes) > 0 {
		for k, attr := range e.Attributes {
			b, _ := json.Marshal(attr)
			e.Attributes[k] = string(b)
		}
		aBytes, err := json.Marshal(e.Attributes)
		if err != nil {
			return "", err
		}
		eBytes = append(append(eBytes[0:len(eBytes)-1], ','), append(aBytes[1:len(aBytes)-1], '}')...)
	}
	iBytes, err := json.Marshal(e.Index())
	if err != nil {
		return "", err
	}
	return string(iBytes) + "\n" + string(eBytes), nil
}
