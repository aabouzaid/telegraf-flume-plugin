package flume

import (
	"encoding/json"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type Flume struct {
	Name    string   `toml:"name"`
	Servers []string `toml:"servers"`
	Filters Filters
}

type Filters struct {
	Source  []string `toml:"source"`
	Channel []string `toml:"channel"`
	Sink    []string `toml:"sink"`
}

type Metrics map[string]map[string]string

const (
	description  = `Read metrics exposed by Flume HTTP endpoint.`
	sampleConfig = `
  ## NOTE This plugin only reads numerical measurements, strings and booleans
  ## will be ignored.

  ## Name for the service being polled.  Will be appended to the name of the
  ## measurement e.g. flume_agents_metrics.
  ##
  name = "agents_metrics"
  ## URL of each server in the service's cluster
  servers = [
    "http://localhost:41414/metrics",
  ]

  ## Specific type (source, channel, sink) could be selected for each type,
  ## instead collecting all metrics as they come from flume.
  # [inputs.flume.filters]
  #  channel = [
  #    "EventPutSuccessCount",
  #    "EventPutAttemptCount"
  #  ]
`
)

const (
	source  = "SOURCE"
	channel = "CHANNEL"
	sink    = "SINK"
)

func (m *Metrics) getJson(flumeUrl string) error {

	// TODO: Better HTTP/s client. 
	_, err := url.Parse(flumeUrl)
	if err != nil {
		log.Printf("E! %s", err)
		return err
	}

	resp, err := http.Get(flumeUrl)
	if err != nil {
		log.Printf("E! %s", err)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("E! %s %s", flumeUrl, resp.Status)
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		log.Printf("E! %s", err)
		return err
	}

	return nil
}

func (f *Flume) gatherServer(
	acc telegraf.Accumulator,
	serverURL string,
) error {

	// Get Metrics from Flume server.
	var m Metrics
	m.getJson(serverURL)

	// Measurement.
	measurement := "flume"
	if f.Name != "" {
		measurement = measurement + "_" + f.Name
	}

	// Tags and fields.
	for keyName, _ := range m {

		// Fields.
		fields := make(map[string]interface{})
		typeName := strings.SplitN(keyName, ".", 2)[0]
		filtersMap := map[string][]string{
			source:  f.Filters.Source,
			channel: f.Filters.Channel,
			sink:    f.Filters.Sink,
		}
		for key, rawValue := range m[keyName] {
			var value interface{}
			if intValue, err := strconv.ParseInt(rawValue, 10, 0); err == nil {
				value = intValue
			} else if floatValue, err := strconv.ParseFloat(rawValue, 64); err == nil {
				value = floatValue
			}
			if value != nil {
				fields = filterFields(fields, filtersMap, typeName, key, value)
			}
		}

		// Tags.
		keyNameArr := strings.SplitN(keyName, ".", 2)
		tags := map[string]string{
			"type":   keyNameArr[0],
			"name":   keyNameArr[1],
			"server": serverURL,
		}

		acc.AddFields(measurement, fields, tags)
	}

	return nil
}

// Check if element in an array.
func inArray(arr []string, str string) bool {
	for _, elem := range arr {
		if elem == str {
			return true
		}
	}

	return false
}

// Filter metrics instead collecting all metrics as they come from flume.
func filterFields(
	fields map[string]interface{},
	filters map[string][]string,
	typeName string,
	key string,
	value interface{},
) map[string]interface{} {
	typeFiltersLen := len(filters[typeName])
	isTypeFiltered := inArray(filters[typeName], key)
	if (typeFiltersLen > 0 && isTypeFiltered) || typeFiltersLen == 0 {
		fields[key] = value
	}

	return fields
}

// SampleConfig returns sample configuration message.
func (f *Flume) SampleConfig() string {
	return sampleConfig
}

// Description returns description of Flume plugin.
func (f *Flume) Description() string {
	return description
}

// Gather reads stats from all configured servers accumulates stats.
func (f *Flume) Gather(acc telegraf.Accumulator) error {
	if len(f.Servers) == 0 {
		f.Servers = []string{"http://localhost:41414/metrics"}
	}

	var wg sync.WaitGroup
	for _, server := range f.Servers {
		wg.Add(1)
		go func(server string) {
			acc.AddError(f.gatherServer(acc, server))
			wg.Done()
		}(server)
	}
	wg.Wait()

	return nil
}

func init() {
	inputs.Add("flume", func() telegraf.Input { return &Flume{} })
}
