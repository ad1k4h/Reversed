# 🌍 Reversed

**A high-performance, offline Reverse Geocoder server written in Go.**

Reversed is a standalone, incredibly fast, and memory-efficient reverse geocoding server. It uses OpenStreetMap data dumps (provided by Photon) to instantly convert GPS coordinates (latitude/longitude) into structured GeoJSON addresses. 

Built for real-time tracking systems, it features zero-downtime background updates, multi-region merging, and on-the-fly database compression.

---

## ✨ Features

- 🚀 **Lightning Fast:** Uses Geohash spatial indexing and BoltDB (`bbolt`) for sub-millisecond response times.
- 🗜 **Storage Optimized:** Strips unnecessary metadata and compresses records using `Snappy` or `Gzip`, reducing database size drastically.
- 🗺 **Multi-Region Support:** Define the countries or continents you need in the config. The server automatically downloads, unpacks, and merges them into a single unified database.
- 🔄 **Zero-Downtime Updates:** The server keeps answering API queries from the old database while compiling the new map data in the background. It swaps them instantly when finished.
- 💻 **Resource Friendly:** Includes a built-in memory limit manager. Easily runs on a basic VPS with 2GB RAM.
- 📊 **Beautiful Console:** Color-coded logs, progress bars, and ETA calculations right in your terminal.

---

## 🛠 Installation (Debian 13)

This guide provides a step-by-step installation for a fresh Debian 13 server.

### 1. Install System Dependencies
First, update your system and install Git and Go:
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git golang-go
```

### 2. Clone the Repository
Clone this repository to your server and enter the directory:
```bash
git clone https://github.com/ad1k4h/Reversed.git
cd Reversed
```

### 3. Initialize Go and Install Dependencies
Initialize the Go module and download the required external libraries:
```bash
go mod init reversed
go get github.com/golang/snappy
go get github.com/klauspost/compress/zstd
go get github.com/mmcloughlin/geohash
go get go.etcd.io/bbolt
go mod tidy
```

## ⚙️ Configuration

**Before running the server, edit the config.json file. You can configure server limits and specify which maps you want to download.**

```json
{
    "server": {
        "_comment_db_path": "Database file stored neatly inside the database folder.",
        "db_path": "database/geocoder.db",
        "port": ":2322",
        "log_level": 2,
        "max_memory": "4GB",
        "compression_method": 1
    },
    "maps": [
        "hu",
        "sk",
        "de"
    ]
}
```

Note: Available map codes include planet (world), europe, north-america, and 2-letter country codes like hu, de, at, us, etc.

## 🚀 Running the Server

You can run the server directly:

```bash
go run main.go
```

Or build it into a compiled executable for production:

```bash
go build -o reversed main.go
./reversed
```

## What happens on the first run?

- The server starts listening on the configured port immediately.
- It detects that no database exists and starts downloading the .jsonl.zst map dumps to the /dumps folder.
- Once downloaded, it parses, filters, compresses, and indexes the data into a BoltDB database (/database/geocoder.db).
- Future runs will boot instantly and serve queries from the database while checking for map updates via .md5 hashes in the background.

## 📡 API Usage

Once the database is built, you can query the API using standard HTTP GET requests.

Endpoint:

```http
GET http://YOUR_SERVER_IP:2322/reverse?lat=47.49789359211134&lon=19.040267297941828
```

## Response (Standard GeoJSON):

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "osm_type": "N",
        "osm_id": 736070388,
        "osm_key": "tourism",
        "osm_value": "artwork",
        "type": "artwork",
        "street": "Clark Ádám tér",
        "district": "Víziváros",
        "city": "Budapest",
        "county": "Budapest",
        "state": "Central Hungary",
        "country": "Hungary",
        "postcode": "1013",
        "countrycode": "HU"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [
          19.0402358,
          47.4978783
        ]
      }
    }
  ]
}
```

## ☕ Support & Donate

If you found this project helpful, saved hours of coding, or reduced your Google Maps API bills, please consider buying me a coffee or supporting my work!

💖 **[Support the project via PayPal](https://paypal.me/ad1k4h)**

---

## 📜 License & Credits

**Reversed** was developed by Adam "ad1k4h" Kecskes.  

Map data provided by [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors (ODbL). Dump infrastructure provided by [Photon / GraphHopper](https://photon.komoot.io/).