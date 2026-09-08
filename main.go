package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/mmcloughlin/geohash"
	"go.etcd.io/bbolt"
)

// --- ANSI CONSOLE COLORS & CONTROLS ---
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorBold   = "\033[1m"
	ClearScreen = "\033[H\033[2J"
	ClearLine   = "\033[K"
)

// --- CONFIG STRUCTURES ---
type ServerConfig struct {
	DBPath            string `json:"db_path"`
	Port              string `json:"port"`
	LogLevel          int    `json:"log_level"`
	MaxMemory         string `json:"max_memory"`
	CompressionMethod int    `json:"compression_method"`
}

type Config struct {
	Server ServerConfig `json:"server"`
	Maps   []string     `json:"maps"`
}

var appConfig *Config
var activeDB GeocoderDB
var logger *log.Logger

type GeocoderDB struct {
	db *bbolt.DB
	mu sync.RWMutex
}

// Map catalog to resolve region codes to download paths
var mapCatalog = map[string]string{
	"planet":            "",
	"europe":            "europe",
	"north-america":     "north-america",
	"south-america":     "south-america",
	"africa":            "africa",
	"asia":              "asia",
	"australia-oceania": "australia-oceania",

	// Europe
	"hu": "europe/hungary",
	"de": "europe/germany",
	"nl": "europe/netherlands",
	"at": "europe/austria",
	"ch": "europe/switzerland-liechtenstein",
	"fr": "europe/france-monacco",
	"it": "europe/italy",
	"es": "europe/spain",
	"uk": "europe/british-islands",
	"sk": "europe/slovakia",
	"ro": "europe/romania",
	"pl": "europe/poland",
	"cz": "europe/czech-republic",

	// North America
	"us": "north-america/usa",
	"ca": "north-america/canada",
	"mx": "north-america/mexico",
	"bm": "north-america/bermuda",
	"gl": "north-america/greenland",

	// South America
	"ar": "south-america/argentina",
	"bo": "south-america/bolivia",
	"br": "south-america/brazil",
	"cl": "south-america/chile",
	"co": "south-america/colombia",
	"ec": "south-america/ecuador",
	"py": "south-america/paraguay",
	"pe": "south-america/peru",
	"uy": "south-america/uruguay",

	// Africa (explicit countries listed)
	"bi": "africa/burundi",
	"ke": "africa/kenia",
	"mw": "africa/malawi",
	"mz": "africa/mozambique",
	"ng": "africa/nigeria",
	"rw": "africa/rwanda",
	"sh": "africa/saint-helena",
	"ss": "africa/south-sudan",
	"tz": "africa/tanzania",
	"ug": "africa/uganda",
	"zm": "africa/zambia",
	"zw": "africa/zimbabwe",

	// Asia
	"af": "asia/afghanistan",
	"am": "asia/armenia",
	"az": "asia/azerbaijan",
	"bd": "asia/bangladesh",
	"bt": "asia/bhutan",
	"bn": "asia/brunei",
	"kh": "asia/cambodia",
	"cn": "asia/china",
	"tl": "asia/east-timor",
	"in": "asia/india",
	"id": "asia/indonesia",
	"ir": "asia/iran",
	"iq": "asia/iraq",
	"il": "asia/isreal", // Note: Spelled 'isreal' on GraphHopper servers
	"jp": "asia/japan",
	"jo": "asia/jordan",
	"kp": "asia/korea",
	"kr": "asia/korea",
	"la": "asia/laos",
	"lb": "asia/lebanon",
	"my": "asia/malaysia",
	"mn": "asia/mongolia",
	"mm": "asia/myanmar",
	"np": "asia/nepal",
	"pk": "asia/pakistan",
	"ps": "asia/palestine",
	"ph": "asia/philippines",
	"sg": "asia/singapore",
	"sy": "asia/syria",
	"tw": "asia/taiwan",
	"th": "asia/thailand",
	"vn": "asia/vietnam",

	// Australia-Oceania
	"au": "australia-oceania/australia",
	"nz": "australia-oceania/new-zealand",
}

type BatchItem struct {
	Hash string
	Data []byte
}

type GeoJSONProperties struct {
	OsmType     string `json:"osm_type,omitempty"`
	OsmID       int64  `json:"osm_id,omitempty"`
	OsmKey      string `json:"osm_key,omitempty"`
	OsmValue    string `json:"osm_value,omitempty"`
	Type        string `json:"type,omitempty"`
	HouseNumber string `json:"housenumber,omitempty"`
	Street      string `json:"street,omitempty"`
	District    string `json:"district,omitempty"`
	City        string `json:"city,omitempty"`
	County      string `json:"county,omitempty"`
	State       string `json:"state,omitempty"`
	Country     string `json:"country,omitempty"`
	Postcode    string `json:"postcode,omitempty"`
	CountryCode string `json:"countrycode,omitempty"`
}

type GeoJSONFeature struct {
	Type       string            `json:"type"`
	Properties GeoJSONProperties `json:"properties"`
	Geometry   GeoJSONPoint      `json:"geometry"`
}

type GeoJSONPoint struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type structItem struct {
	Geometry struct {
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`
}

// --- COMPRESSION ---
func compressData(data []byte, method int) []byte {
	switch method {
	case 1:
		return snappy.Encode(nil, data)
	case 2:
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(data)
		w.Close()
		return b.Bytes()
	default:
		return data
	}
}

func decompressData(data []byte, method int) ([]byte, error) {
	switch method {
	case 1:
		return snappy.Decode(nil, data)
	case 2:
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	default:
		return data, nil
	}
}

// --- VISUAL HELPERS & PROGRESS BARS ---
func printBanner() {
	fmt.Print(ClearScreen) // Clears the console
	fmt.Println(ColorCyan + ColorBold + "==================================================")
	fmt.Println("                 ReversedGO                       ")
	fmt.Println("         Reverse Geocoder Server                  ")
	fmt.Println("   Coded by Adam \"ad1k4h\" Kecskes               ")
	fmt.Println("   (c) 2026 Budapest, Hungary                     ")
	fmt.Println("==================================================" + ColorReset)
}

func drawProgressBar(current, total int64, prefix string, startTime time.Time) {
	if total <= 0 {
		return
	}
	width := 30
	progress := float64(current) / float64(total)
	if progress > 1.0 {
		progress = 1.0
	}
	filled := int(progress * float64(width))
	bar := strings.Repeat("█", filled) + strings.Repeat("-", width-filled)

	elapsed := time.Since(startTime).Seconds()
	var etaStr string
	if progress > 0.01 && elapsed > 2 {
		estimatedTotal := elapsed / progress
		remaining := int(estimatedTotal - elapsed)
		mins := remaining / 60
		secs := remaining % 60
		etaStr = fmt.Sprintf(" | ETA: %02dm%02ds", mins, secs)
	} else {
		etaStr = " | ETA: Calculating..."
	}

	fmt.Printf("\r%s%s [%s] %.2f%%%s%s", ClearLine, prefix, bar, progress*100, etaStr, ColorReset)
	if current >= total {
		fmt.Println()
	}
}

// --- DOWNLOADING & PROGRESS TRACKING ---
type ProgressReader struct {
	io.Reader
	Total     int64
	Current   int64
	StartTime time.Time
	MapName   string
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	if pr.StartTime.IsZero() {
		pr.StartTime = time.Now()
	}

	n, err := pr.Reader.Read(p)
	pr.Current += int64(n)

	currentMB := float64(pr.Current) / (1024 * 1024)
	totalMB := float64(pr.Total) / (1024 * 1024)
	prefix := fmt.Sprintf("%s[Downloading %s] %.2f / %.2f MB", ColorYellow, pr.MapName, currentMB, totalMB)
	
	drawProgressBar(pr.Current, pr.Total, prefix, pr.StartTime)

	return n, err
}

type TrackingReader struct {
	io.Reader
	TotalRead *int64
}

func (tr *TrackingReader) Read(p []byte) (int, error) {
	n, err := tr.Reader.Read(p)
	atomic.AddInt64(tr.TotalRead, int64(n))
	return n, err
}

// --- MAP MANAGER LOGIC ---
func resolveMapURL(code string) (string, string) {
	path, exists := mapCatalog[code]
	if !exists {
		path = code
	}

	parts := strings.Split(path, "/")
	name := parts[len(parts)-1]
	if path == "" {
		name = "planet"
	}

	url := "https://download1.graphhopper.com/public/"
	if path != "" {
		url += path + "/"
	}
	fileName := fmt.Sprintf("photon-dump-%s-master-latest.jsonl.zst", name)
	url += fileName

	return url, fileName
}

func fetchRemoteMD5(url string) string {
	resp, err := http.Get(url)
	if err != nil || resp.StatusCode != 200 {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	parts := strings.Fields(string(body))
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func loadManifest() map[string]string {
	data, err := os.ReadFile("dumps/build_manifest.json")
	if err != nil {
		return make(map[string]string)
	}
	var m map[string]string
	json.Unmarshal(data, &m)
	return m
}

func saveManifest(m map[string]string) {
	os.MkdirAll("dumps", 0755)
	data, _ := json.MarshalIndent(m, "", "  ")
	os.WriteFile("dumps/build_manifest.json", data, 0644)
}

func checkAndDownloadUpdate(cfg *Config) {
	os.MkdirAll("dumps", 0755)
	os.MkdirAll("database", 0755)

	manifest := loadManifest()
	newManifest := make(map[string]string)
	needsRebuild := false
	var filesToProcess []string

	for _, mapCode := range cfg.Maps {
		url, fileName := resolveMapURL(mapCode)
		md5URL := url + ".md5"
		localZstPath := filepath.Join("dumps", fileName)
		localMD5File := localZstPath + ".md5"

		logger.Printf("%s[*] Checking map: %s%s", ColorCyan, mapCode, ColorReset)

		remoteMD5 := fetchRemoteMD5(md5URL)
		if remoteMD5 == "" {
			logger.Printf("%s[!] Error: Failed to fetch MD5 for %s. Skipping.%s", ColorRed, mapCode, ColorReset)
			continue
		}
		newManifest[mapCode] = remoteMD5

		localMD5Bytes, _ := os.ReadFile(localMD5File)
		localMD5 := strings.TrimSpace(string(localMD5Bytes))
		_, zstStatErr := os.Stat(localZstPath)

		if localMD5 != remoteMD5 || zstStatErr != nil {
			logger.Printf("%s[+] New update found for %s!%s", ColorGreen, mapCode, ColorReset)
			logger.Printf("%s[i] Downloading from: %s%s", ColorCyan, url, ColorReset)
			
			getResp, err := http.Get(url)
			if err != nil || getResp.StatusCode != 200 {
				logger.Printf("%s[!] Error downloading map %s.%s", ColorRed, mapCode, ColorReset)
				continue
			}

			out, _ := os.Create(localZstPath)
			pr := &ProgressReader{Reader: getResp.Body, Total: getResp.ContentLength, MapName: mapCode}
			io.Copy(out, pr)
			out.Close()
			getResp.Body.Close()
			fmt.Println() // New line after progress bar finishes

			os.WriteFile(localMD5File, []byte(remoteMD5), 0644)
			logger.Printf("%s[✓] Download finished for %s.%s", ColorGreen, mapCode, ColorReset)
		} else {
			logger.Printf("%s[✓] Map %s is up-to-date.%s", ColorGreen, mapCode, ColorReset)
		}

		filesToProcess = append(filesToProcess, localZstPath)
	}

	for k, v := range newManifest {
		if manifest[k] != v {
			needsRebuild = true
		}
	}
	for k, v := range manifest {
		if newManifest[k] != v {
			needsRebuild = true
		}
	}
	if _, err := os.Stat(cfg.Server.DBPath); err != nil {
		needsRebuild = true
	}

	if needsRebuild && len(filesToProcess) > 0 {
		logger.Println(ColorYellow + "[*] Changes detected. Building database..." + ColorReset)
		buildDBFromFiles(filesToProcess, cfg, newManifest)
	} else if !needsRebuild {
		logger.Println(ColorGreen + "[✓] All maps and database are current!" + ColorReset)
		ensureDBLoaded()
	}
}

// --- DATABASE BUILDER (MULTIFILE) ---
func buildDBFromFiles(zstPaths []string, cfg *Config, newManifest map[string]string) {
	tempDBPath := cfg.Server.DBPath + ".tmp"
	os.Remove(tempDBPath)
	os.MkdirAll(filepath.Dir(cfg.Server.DBPath), 0755)

	newDB, err := bbolt.Open(tempDBPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		logger.Printf("%s[!] Error opening database: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	newDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("features"))
		return err
	})

	var totalSize int64
	for _, p := range zstPaths {
		info, err := os.Stat(p)
		if err == nil {
			totalSize += info.Size()
		}
	}

	var totalRead int64
	batchCount := 0
	var batch []BatchItem
	startTime := time.Now()

	for _, zstPath := range zstPaths {
		logger.Printf("%s[*] Processing file: %s%s", ColorCyan, zstPath, ColorReset)
		file, err := os.Open(zstPath)
		if err != nil {
			logger.Printf("%s[!] Error opening file: %v%s", ColorRed, err, ColorReset)
			continue
		}

		tracker := &TrackingReader{Reader: file, TotalRead: &totalRead}
		decoder, _ := zstd.NewReader(tracker)
		scanner := bufio.NewScanner(decoder)
		buf := make([]byte, 0, 1024*1024)
		scanner.Buffer(buf, 10*1024*1024)

		for scanner.Scan() {
			line := scanner.Bytes()
			var rawLine map[string]interface{}
			if err := json.Unmarshal(line, &rawLine); err != nil {
				continue
			}

			typ, ok := rawLine["type"].(string)
			if !ok || typ != "Place" {
				continue
			}

			content, ok := rawLine["content"].([]interface{})
			if !ok {
				continue
			}

			for _, rawItem := range content {
				item, ok := rawItem.(map[string]interface{})
				if !ok {
					continue
				}

				var lon, lat float64
				if centroid, ok := item["centroid"].([]interface{}); ok && len(centroid) >= 2 {
					lon, _ = centroid[0].(float64)
					lat, _ = centroid[1].(float64)
				}
				if lat == 0 || lon == 0 {
					continue
				}

				extractField := func(baseKey string) interface{} {
					keysToCheck := []string{baseKey + ":en", baseKey + ":default", baseKey + ":hu", baseKey}
					for _, k := range keysToCheck {
						if val, exists := item[k]; exists && val != nil {
							return val
						}
						if props, ok := item["properties"].(map[string]interface{}); ok {
							if val, exists := props[k]; exists && val != nil {
								return val
							}
						}
						if addr, ok := item["address"].(map[string]interface{}); ok {
							if val, exists := addr[k]; exists && val != nil {
								return val
							}
						}
						if val, exists := rawLine[k]; exists && val != nil {
							return val
						}
						if props, ok := rawLine["properties"].(map[string]interface{}); ok {
							if val, exists := props[k]; exists && val != nil {
								return val
							}
						}
						if addr, ok := rawLine["address"].(map[string]interface{}); ok {
							if val, exists := addr[k]; exists && val != nil {
								return val
							}
						}
					}
					return nil
				}

				getString := func(v interface{}) string {
					if s, ok := v.(string); ok {
						return s
					}
					if m, ok := v.(map[string]interface{}); ok {
						if str, ok := m["en"].(string); ok {
							return str
						}
						if str, ok := m["default"].(string); ok {
							return str
						}
						if str, ok := m["hu"].(string); ok {
							return str
						}
						for _, val := range m {
							if str, ok := val.(string); ok {
								return str
							}
						}
					}
					return ""
				}

				props := GeoJSONProperties{}

				if v := extractField("object_type"); v != nil {
					props.OsmType = getString(v)
				} else if v := extractField("osm_type"); v != nil {
					props.OsmType = getString(v)
				}

				if v := extractField("object_id"); v != nil {
					if num, ok := v.(float64); ok {
						props.OsmID = int64(num)
					}
				} else if v := extractField("osm_id"); v != nil {
					if num, ok := v.(float64); ok {
						props.OsmID = int64(num)
					}
				}

				if v := extractField("osm_key"); v != nil {
					props.OsmKey = getString(v)
				}
				if v := extractField("osm_value"); v != nil {
					props.OsmValue = getString(v)
					props.Type = getString(v)
				}
				if props.Type == "" {
					if v := extractField("type"); v != nil {
						props.Type = getString(v)
					}
				}

				if v := extractField("housenumber"); v != nil {
					props.HouseNumber = getString(v)
				}
				if v := extractField("street"); v != nil {
					props.Street = getString(v)
				} else if v := extractField("name"); v != nil && props.HouseNumber != "" {
					props.Street = getString(v)
				}

				if v := extractField("district"); v != nil {
					props.District = getString(v)
				} else if v := extractField("suburb"); v != nil {
					props.District = getString(v)
				}

				if v := extractField("city"); v != nil {
					props.City = getString(v)
				}
				if v := extractField("county"); v != nil {
					props.County = getString(v)
				}
				if v := extractField("state"); v != nil {
					props.State = getString(v)
				}
				if v := extractField("country"); v != nil {
					props.Country = getString(v)
				}
				if v := extractField("postcode"); v != nil {
					props.Postcode = getString(v)
				}

				if v := extractField("countrycode"); v != nil {
					props.CountryCode = strings.ToUpper(getString(v))
				} else if v := extractField("country_code"); v != nil {
					props.CountryCode = strings.ToUpper(getString(v))
				}

				if props.Country == "" && props.CountryCode == "HU" {
					props.Country = "Hungary"
				}

				validCount := 0
				if props.HouseNumber != "" {
					validCount++
				}
				if props.Street != "" {
					validCount++
				}
				if props.City != "" {
					validCount++
				}
				if props.OsmID != 0 {
					validCount++
				}
				if validCount < 2 {
					continue
				}

				feature := GeoJSONFeature{
					Type:       "Feature",
					Properties: props,
					Geometry: GeoJSONPoint{
						Type:        "Point",
						Coordinates: []float64{lon, lat},
					},
				}

				itemBytes, _ := json.Marshal(feature)

				hash := geohash.EncodeWithPrecision(lat, lon, 8)
				uniqueStr := fmt.Sprintf("%d", props.OsmID)
				if props.OsmID == 0 {
					uniqueStr = fmt.Sprintf("%d", time.Now().UnixNano()%10000)
				}
				uniqueKey := hash + "_" + uniqueStr

				batch = append(batch, BatchItem{Hash: uniqueKey, Data: itemBytes})
				batchCount++

				if len(batch) >= 10000 {
					writeBatchToDB(newDB, batch, cfg.Server.CompressionMethod)
					batch = nil
				}

				if batchCount%50000 == 0 {
					currentRead := atomic.LoadInt64(&totalRead)
					prefix := fmt.Sprintf("%s[Building DB] Records: %d ", ColorPurple, batchCount)
					drawProgressBar(currentRead, totalSize, prefix, startTime)
				}
			}
		}
		decoder.Close()
		file.Close()

		logger.Printf("\n%s[i] Cleaning up dump file: %s%s", ColorYellow, zstPath, ColorReset)
		os.Remove(zstPath)
	}

	fmt.Println() 

	if len(batch) > 0 {
		writeBatchToDB(newDB, batch, cfg.Server.CompressionMethod)
	}

	logger.Printf("%s[*] Finalizing database (syncing data to disk, please wait)...%s", ColorYellow, ColorReset)
	
	err = newDB.Close()
	if err != nil {
		logger.Printf("%s[!] Error closing temporary database: %v%s", ColorRed, err, ColorReset)
	}

	logger.Printf("%s[*] Replacing old database with the newly built one...%s", ColorYellow, ColorReset)

	activeDB.mu.Lock()
	if activeDB.db != nil {
		activeDB.db.Close()
	}
	os.Rename(tempDBPath, cfg.Server.DBPath)

	reopenedDB, err := bbolt.Open(cfg.Server.DBPath, 0600, nil)
	if err == nil {
		activeDB.db = reopenedDB
	}
	activeDB.mu.Unlock()

	saveManifest(newManifest)
	logger.Printf("%s[✓] Database successfully built and replaced! Total records: %d%s\n", ColorGreen, batchCount, ColorReset)
}

func writeBatchToDB(db *bbolt.DB, batch []BatchItem, compressionMethod int) {
	db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("features"))
		for _, item := range batch {
			finalBytes := compressData(item.Data, compressionMethod)
			b.Put([]byte(item.Hash), finalBytes)
		}
		return nil
	})
}

// --- API AND SERVER ---
func ensureDBLoaded() {
	activeDB.mu.Lock()
	if activeDB.db == nil {
		db, err := bbolt.Open(appConfig.Server.DBPath, 0600, nil)
		if err == nil {
			activeDB.db = db
			logger.Println(ColorGreen + "[✓] Existing database successfully loaded into memory." + ColorReset)
		}
	}
	activeDB.mu.Unlock()
}

func reverseHandler(w http.ResponseWriter, r *http.Request) {
	latStr := r.URL.Query().Get("lat")
	lonStr := r.URL.Query().Get("lon")

	lat, errLat := strconv.ParseFloat(latStr, 64)
	lon, errLon := strconv.ParseFloat(lonStr, 64)

	if errLat != nil || errLon != nil {
		http.Error(w, `{"error": "Invalid lat or lon parameter"}`, http.StatusBadRequest)
		return
	}

	fullHash := geohash.EncodeWithPrecision(lat, lon, 8)
	searchPrefix := []byte(fullHash[:7])

	var bestFeature []byte
	var minDist = 999999.0

	activeDB.mu.RLock()
	if activeDB.db != nil {
		activeDB.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("features"))
			if b == nil {
				return nil
			}
			c := b.Cursor()

			for k, v := c.Seek(searchPrefix); k != nil && bytes.HasPrefix(k, searchPrefix); k, v = c.Next() {
				uncompressedJSON, err := decompressData(v, appConfig.Server.CompressionMethod)
				if err != nil {
					continue
				}

				var tempItem structItem
				if err := json.Unmarshal(uncompressedJSON, &tempItem); err == nil {
					if len(tempItem.Geometry.Coordinates) >= 2 {
						fLon := tempItem.Geometry.Coordinates[0]
						fLat := tempItem.Geometry.Coordinates[1]

						dLat := fLat - lat
						dLon := fLon - lon
						dist := (dLat * dLat) + (dLon * dLon)

						if dist < minDist {
							minDist = dist
							bestFeature = uncompressedJSON
						}
					}
				}
			}
			return nil
		})
	}
	activeDB.mu.RUnlock()

	if bestFeature == nil {
		http.Error(w, `{"type":"FeatureCollection","features":[]}`, http.StatusOK)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"type":"FeatureCollection","features":[`))
	w.Write(bestFeature)
	w.Write([]byte(`]}`))
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func logMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next(lrw, r)
		logger.Printf("API | %s | %s | Status: %d | Time: %v", r.RemoteAddr, r.URL.String(), lrw.statusCode, time.Since(start))
	}
}

func parseMemoryLimit(s string) int64 {
	if s == "" {
		return 0
	}
	if len(s) > 2 {
		unit := strings.ToUpper(s[len(s)-2:])
		valStr := s[:len(s)-2]
		var val int64
		if _, err := fmt.Sscanf(valStr, "%d", &val); err == nil {
			if unit == "GB" {
				return val * 1024 * 1024 * 1024
			} else if unit == "MB" {
				return val * 1024 * 1024
			}
		}
	}
	var val int64
	if _, err := fmt.Sscanf(s, "%d", &val); err == nil {
		return val
	}
	return 0
}

func setupLogger(level int) {
	switch level {
	case 0:
		logger = log.New(io.Discard, "", 0)
	case 1:
		logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	case 2:
		os.MkdirAll("database", 0755)
		file, err := os.OpenFile("database/reversed.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
		} else {
			multi := io.MultiWriter(os.Stdout, file)
			logger = log.New(multi, "", log.Ldate|log.Ltime)
		}
	default:
		logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
}

func loadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func main() {
	printBanner()

	cfg, err := loadConfig("config.json")
	if err != nil {
		fmt.Println(ColorRed + "CRITICAL: Failed to read config.json:" + ColorReset, err)
		os.Exit(1)
	}
	appConfig = cfg

	setupLogger(cfg.Server.LogLevel)
	logger.Println(ColorGreen + "=== Starting ReversedGO ===" + ColorReset)
	logger.Printf("Compression method: %d (0=None, 1=Snappy, 2=Gzip)", cfg.Server.CompressionMethod)

	bytesLimit := parseMemoryLimit(cfg.Server.MaxMemory)
	if bytesLimit > 0 {
		debug.SetMemoryLimit(bytesLimit)
		logger.Printf("Memory limit set to: %s (%d bytes)", cfg.Server.MaxMemory, bytesLimit)
	}
	debug.SetGCPercent(20)

	if _, err := os.Stat(cfg.Server.DBPath); err == nil {
		db, err := bbolt.Open(cfg.Server.DBPath, 0600, nil)
		if err == nil {
			activeDB.db = db
			logger.Println(ColorGreen + "[✓] Existing database successfully loaded on startup." + ColorReset)
		} else {
			logger.Printf(ColorRed+"[!] Error opening existing database: %v\n"+ColorReset, err)
		}
	} else {
		logger.Println(ColorYellow + "[i] No existing database found on disk. Search will not return results until the first build is complete." + ColorReset)
	}

	go checkAndDownloadUpdate(cfg)

	http.HandleFunc("/reverse", logMiddleware(reverseHandler))

	logger.Printf(ColorCyan+"[*] Server listening on port %s..."+ColorReset, cfg.Server.Port)
	if err := http.ListenAndServe(cfg.Server.Port, nil); err != nil {
		logger.Fatalf(ColorRed+"[!] Server error: %v"+ColorReset, err)
	}
}