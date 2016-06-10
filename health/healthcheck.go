package health

import (
	"net/http"
	"encoding/json"
	"reflect"
)

type Status struct {
	Scheduler bool        `json:"scheduler"`
	Api       bool        `json:"api"`
	Data      bool        `json:"data"`
}

const (
	contentType = "application/json; charset=utf-8"
	contentOptions = "nosniff"
)

var HealthStatus = &Status{false, false, false}

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", contentOptions)

	var statusCode int
	payload := map[string]interface{}{
		"status": HealthStatus,
	}

	errorFound := false
	v := reflect.ValueOf(HealthStatus).Elem()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Bool() == false {
			errorFound = true
			break
		}
	}

	if !errorFound {
		statusCode = 200
	} else {
		statusCode = 500
		payload["error"] = "One or more services are in an unhealthy state"
	}

	w.WriteHeader(statusCode)
	jsonPayload, _ := json.Marshal(payload)
	w.Write(jsonPayload)
}
