module github.com/go-ap/storage-badger

go 1.25

require (
	github.com/dgraph-io/badger/v4 v4.8.0
	github.com/go-ap/activitypub v0.0.0-20251028130710-8bc6217f6c8d
	github.com/go-ap/cache v0.0.0-20251028142135-e067d18ce6a1
	github.com/go-ap/errors v0.0.0-20250905102357-4480b47a00c4
	github.com/go-ap/filters v0.0.0-20251028142811-9b3305faa3cd
	github.com/go-ap/storage-conformance-suite v0.0.0-20251104073849-a2462e14030f
	github.com/google/go-cmp v0.7.0
	github.com/openshift/osin v1.0.2-0.20220317075346-0f4d38c6e53f
	golang.org/x/crypto v0.43.0
)

require (
	git.sr.ht/~mariusor/go-xsd-duration v0.0.0-20220703122237-02e73435a078 // indirect
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/bits-and-blooms/bitset v1.24.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ap/jsonld v0.0.0-20250905102310-8480b0fe24d9 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/flatbuffers v25.9.23+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gotesttools/gotestfmt/v2 v2.5.0 // indirect
	github.com/jdkato/prose v1.2.1 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/mariusor/qstring v0.0.0-20200204164351-5a99d46de39d // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/valyala/fastjson v1.6.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	golang.org/x/net v0.46.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/neurosnap/sentences.v1 v1.0.7 // indirect
)

replace go.opencensus.io => github.com/census-instrumentation/opencensus-go v0.23.0

tool github.com/gotesttools/gotestfmt/v2/cmd/gotestfmt
