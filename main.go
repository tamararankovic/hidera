package main

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/hidera"
	"github.com/tamararankovic/hidera/peers"
)

var h *hidera.Hidera

func main() {
	time.Sleep(10 * time.Second)

	params := config.LoadParamsFromEnv()
	conf := config.LoadConfigFromEnv()
	peers, err := peers.NewPeers(conf)
	if err != nil {
		log.Fatalln(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	h = hidera.NewHidera(params, peers)

	go exportAll()

	r := http.NewServeMux()
	r.HandleFunc("POST /metrics", setMetricsHandler)
	log.Println("Metrics server listening on :9200/metrics")

	go func() {
		log.Fatal(http.ListenAndServe("0.0.0.0:9200", r))
	}()

	h.Run()

	<-quit

	log.Println("received shutdown signal...")
}

func exportAll() {
	for range time.NewTicker(time.Second).C {
		value := 0.0
		h.Lock.Lock()
		tree := h.FindBestTree()
		if tree != nil && tree.GlobalAgg != nil {
			value = tree.GlobalAgg.Value / float64(tree.GlobalAgg.Count)
		}
		h.Lock.Unlock()
		exportResult(float64(value), 0, time.Now().UnixNano())
		exportMsgCount()
	}
}

var writers map[string]*csv.Writer = map[string]*csv.Writer{}

func exportResult(value float64, reqTimestamp, rcvTimestamp int64) {
	filename := "/var/log/hidera/value.csv"
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	reqTsStr := strconv.Itoa(int(reqTimestamp))
	rcvTsStr := strconv.Itoa(int(rcvTimestamp))
	valStr := strconv.FormatFloat(value, 'f', -1, 64)
	err := writer.Write([]string{"x", reqTsStr, rcvTsStr, valStr})
	if err != nil {
		log.Println(err)
	}
}

func exportMsgCount() {
	filename := "/var/log/hidera/msg_count.csv"
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	tsStr := strconv.Itoa(int(time.Now().UnixNano()))
	peers.MessagesSentLock.Lock()
	sent := peers.MessagesSent
	peers.MessagesSentLock.Unlock()
	peers.MessagesRcvdLock.Lock()
	rcvd := peers.MessagesRcvd
	peers.MessagesRcvdLock.Unlock()
	sentStr := strconv.Itoa(sent)
	rcvdStr := strconv.Itoa(rcvd)
	err := writer.Write([]string{tsStr, sentStr, rcvdStr})
	if err != nil {
		log.Println(err)
	}
}

func setMetricsHandler(w http.ResponseWriter, r *http.Request) {
	newMetrics, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	lines := strings.Split(string(newMetrics), "\n")
	valStr := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "app_memory_usage_bytes") {
			valStr = strings.Split(line, " ")[1]
			break
		}
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		log.Println(err)
	} else {
		log.Println("new value", val)
		h.Value = val
	}
	w.WriteHeader(http.StatusOK)
}
