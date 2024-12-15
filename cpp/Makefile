# Directories
SRC_DIR = src
BUILD_DIR = build
BIN_DIR = bin
UTILS_DIR = src/utils
DATA_DIR = data
DB_DIR = db

# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -I/usr/include/jsoncpp -I/usr/include/libgumbo -I/usr/include/libxml2 -I$(SRC_DIR)
LDFLAGS = -lcurl -lsqlite3 -ljsoncpp -lgumbo -lxml2 -lpthread -lboost_locale  -lboost_system -lboost_url

# Source files
SRCS = $(wildcard $(SRC_DIR)/*.cpp) $(wildcard $(UTILS_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRCS))
DEP_FILES = $(OBJS:.o=.d)

# Main target
all: directories $(BIN_DIR)/crawler

# Create necessary directories
directories:
	mkdir -p $(BUILD_DIR)/utils $(BIN_DIR) $(DATA_DIR) $(DB_DIR)

# Link
$(BIN_DIR)/crawler: $(OBJS)
	$(CXX) $(OBJS) -o $@ $(LDFLAGS)

# Compile
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

$(BUILD_DIR)/utils/%.o: $(UTILS_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR) $(DATA_DIR) $(DB_DIR)

-include $(DEP_FILES)

.PHONY: all clean directories