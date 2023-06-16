all:
	go build -o bin/olmoci        ./cmd/olmoci
	go build -o bin/bundlebuild   ./cmd/bundlebuild
	go build -o bin/createcatalog ./cmd/createcatalog
