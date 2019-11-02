
BUILD_TYPE=debug

CXX = g++
CXXFLAGS = -std=c++17

ifeq ($(BUILD_TYPE),debug)
CXXFLAGS += -g3
else
ifneq ($(BUILD_TYPE),release)
$(error input 'BUILD_TYPE' parameter can be either 'debug' or 'release')
endif
CXXFLAGS += -O3
endif

CXXFLAGS_UTEST = $(CXXFLAGS) -D_RAFT_UNIT_TEST_

SRCDIR = src
BINDIR = bin
OBJDIR = $(BINDIR)/$(BUILD_TYPE)/object
OBJ_EXE_SUB_DIR = $(OBJDIR)/release-version
OBJ_UTEST_SUB_DIR = $(OBJDIR)/unit-test-version


THIRD_PARTY_DIR=./third_party

INC = -I./src \
	 	-I./src/protocol \
	 	-I$(THIRD_PARTY_DIR)/glog/src \
		-I$(THIRD_PARTY_DIR)/gflags/build-dir/include \
		-I$(THIRD_PARTY_DIR)/boost_1_68_0 \
		-I$(THIRD_PARTY_DIR)/googletest/googletest/include\
		-I$(THIRD_PARTY_DIR)/grpc/include\

LIB = /usr/local/lib/libglog.a \
	  /usr/local/lib/libgflags.a \
	  /usr/local/lib/libgtest.a \
	  /usr/local/lib/libgflags_nothreads.a \
	  /usr/local/lib/libboost_filesystem.a \
	  /usr/local/lib/libboost_system.a \
	  /usr/local/lib/libboost_thread.a \
	  /usr/local/lib/libprotobuf.a \
	  /usr/local/lib/libgrpc++.a \
	  /usr/local/lib/libgrpc.a \
	  /usr/local/lib/libgrpc++_reflection.a \
	  /usr/local/lib/libtcmalloc.a \
	  -lz -ldl -lpthread \

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    LIB += -latomic
endif

PROTOS_PATH = ./src/protocol

vpath %.proto $(PROTOS_PATH)

PROTO_FLAG=$(BINDIR)/compile_proto
MAIN_PROGRAM=$(BINDIR)/$(BUILD_TYPE)/aurora
MAIN_TEST=$(BINDIR)/$(BUILD_TYPE)/aurora_test

-include prepare $(PROTO_FLAG)

.PHONY: all
all: system-check $(MAIN_PROGRAM) $(MAIN_TEST)

.PHONY: prepare
prepare:
	mkdir -p $(OBJ_EXE_SUB_DIR) $(OBJ_UTEST_SUB_DIR)

ALL_SRC_FILES=$(wildcard src/*.cc src/*/*.cc)
TPL_CC_FILES=%src/tools/lock_free_deque.cc %src/tools/lock_free_hash.cc \
		%src/tools/trivial_lock_double_list.cc %src/tools/lock_free_queue.cc  \
		%src/tools/lock_free_queue.cc  %src/tools/lock_free_single_list.cc \
		%src/common/request_base.cc  %src/client/client_framework.cc \
		%src/follower/follower_request.cc  %src/leader/connection_pool.cc  \
		%src/leader/client_pool.cc  %src/leader/leader_request.cc  \
		%src/service/ownership_delegator.cc  %src/candidate/candidate_request.cc  \
		%src/tools/data_structure_base.cc %src/tools/lock_free_unordered_single_list.cc \
		%src/tools/trivial_lock_list_base.cc %src/tools/lock_free_single_list.cc\
		%src/tools/trivial_lock_single_list.cc %src/tools/lock_free_hash_specific.cc\
		%src/common/react_group.cc \

COMPILE_SRC_FILES = $(filter-out $(TPL_CC_FILES), $(ALL_SRC_FILES) )

OBJ_EXE = $(patsubst %.cc, $(OBJ_EXE_SUB_DIR)/%.o, $(COMPILE_SRC_FILES))
OBJ_UTEST = $(patsubst %.cc, $(OBJ_UTEST_SUB_DIR)/%.o, $(COMPILE_SRC_FILES))

EXE_MAIN_OBJ=%/main.o
UTEST_MAIN_OBJ=%/gtest_main.o

EXE_OBJ = $(filter-out $(UTEST_MAIN_OBJ), $(OBJ_EXE) )
UTEST_OBJ = $(filter-out $(EXE_MAIN_OBJ), $(OBJ_UTEST) )

.PHONY:test
test:$(PROTO_FLAG)
	@echo "all:" $(ALL_SRC_FILES)
	@echo "src:" $(COMPILE_SRC_FILES)
	@echo "object-exe:" $(OBJ_EXE)
	@echo "object-utest:" $(OBJ_UTEST)

$(OBJ_EXE_SUB_DIR)/%.o: %.cc
	@mkdir -p $(OBJ_EXE_SUB_DIR)/$(dir $<)
	$(CXX) $(CXXFLAGS) $(INC) -c $< -o $@

$(OBJ_UTEST_SUB_DIR)/%.o: %.cc
	@mkdir -p $(OBJ_UTEST_SUB_DIR)/$(dir $<)
	$(CXX) $(CXXFLAGS_UTEST) $(INC) -c $< -o $@

$(MAIN_PROGRAM): $(EXE_OBJ)
	$(CXX) $(CXXFLAGS) $^ $(LIB) -o $@

$(MAIN_TEST): $(UTEST_OBJ)
	$(CXX) $(CXXFLAGS_UTEST) $^ $(LIB) -o $@

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

$(PROTO_FLAG): $(PROTOS_PATH)/raft.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=$(PROTOS_PATH) $<
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=$(PROTOS_PATH) --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<
	touch $@

.PHONY: clean
clean:
	rm -rf $(OBJDIR) $(PROTOS_PATH)/*.h $(PROTOS_PATH)/*.cc $(PROTO_FLAG) $(MAIN_PROGRAM) $(MAIN_TEST)


# The following is to test your system and ensure a smoother experience.
# They are by no means necessary to actually compile a grpc-enabled software.

PROTOC_CMD = which $(PROTOC)
PROTOC_CHECK_CMD = $(PROTOC) --version | grep -q libprotoc.3
PLUGIN_CHECK_CMD = which $(GRPC_CPP_PLUGIN)
HAS_PROTOC = $(shell $(PROTOC_CMD) > /dev/null && echo true || echo false)
ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC = $(shell $(PROTOC_CHECK_CMD) 2> /dev/null && echo true || echo false)
endif
HAS_PLUGIN = $(shell $(PLUGIN_CHECK_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_VALID_PROTOC),true)
ifeq ($(HAS_PLUGIN),true)
SYSTEM_OK = true
endif
endif

system-check:
ifneq ($(HAS_VALID_PROTOC),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.0.0 installed in your path."
	@echo "Please install Google protocol buffers 3.0.0 and its compiler."
	@echo "You can find it here:"
	@echo
	@echo "   https://github.com/google/protobuf/releases/tag/v3.0.0"
	@echo
	@echo "Here is what I get when trying to evaluate your version of protoc:"
	@echo
	-$(PROTOC) --version
	@echo
	@echo
endif
ifneq ($(HAS_PLUGIN),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have the grpc c++ protobuf plugin installed in your path."
	@echo "Please install grpc. You can find it here:"
	@echo
	@echo "   https://github.com/grpc/grpc"
	@echo
	@echo "Here is what I get when trying to detect if you have the plugin:"
	@echo
	-which $(GRPC_CPP_PLUGIN)
	@echo
	@echo
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif
