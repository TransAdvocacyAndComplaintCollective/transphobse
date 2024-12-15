CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -I/usr/include/jsoncpp -I/usr/include/boost -I/usr/include/libgumbo
LDFLAGS = -lcurl -lgumbo -lpthread

BIN_DIR = bin
TARGETS = $(BIN_DIR)/backed_url_queue $(BIN_DIR)/ovarit_domain_scrape

all: $(BIN_DIR) $(TARGETS)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/backed_url_queue: BackedURLQueue.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Output ELF binary: $@"
	@file $@

$(BIN_DIR)/ovarit_domain_scrape: ovarit_domain_scrape.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Output ELF binary: $@"
	@file $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f *.o $(BIN_DIR)/*
	rm -rf $(BIN_DIR)

.PHONY: all clean
