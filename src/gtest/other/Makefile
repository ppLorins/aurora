

CXX = g++
CXXFLAGS = -std=c++17 -O3

SRCDIR = ../../../src
BINDIR = bin
OBJDIR = $(BINDIR)/object

THIRD_PARTY_DIR=./third_party

INC = -I$(THIRD_PARTY_DIR)/grpc/include\

LIB = -L/usr/local/lib \
		-lprotobuf -pthread -lgrpc++ -lgrpc -lgrpc++_reflection\
		-lz -ldl \

PROTOS_PATH = $(SRCDIR)/protocol

vpath %.proto $(PROTOS_PATH)

PROTO_FLAG=$(BINDIR)/compile_proto

IDIOT_SVR=$(BINDIR)/idiot_svr
IDIOT_CLIENT=$(BINDIR)/idiot_client

PROTO_FILE_PREFIX=raft

-include prepare $(PROTO_FLAG)

.PHONY: all
all: prepare $(IDIOT_SVR) $(IDIOT_CLIENT)

.PHONY: prepare
prepare:
	mkdir -p $(OBJDIR) $(BINDIR) 

ALL_SRC_FILES=$(wildcard *.cc)

OBJ = $(patsubst %.cc, $(OBJDIR)/%.o, $(ALL_SRC_FILES))

SVR_CC_OBJ=%/idiot_server.o
CLIENT_CC_OBJ=%idiot_client.o

SVR_OBJ = $(filter-out $(CLIENT_CC_OBJ), $(OBJ) )
CLIENT_OBJ = $(filter-out $(SVR_CC_OBJ), $(OBJ) )

.PHONY:test
test:$(PROTO_FLAG)
	@echo "src:" $(ALL_SRC_FILES)

$(OBJDIR)/%.o: %.cc 
	@mkdir -p $(OBJDIR)/$(dir $<)
	$(CXX) $(CXXFLAGS) $(INC) -c $< -o $@

$(IDIOT_SVR): $(SVR_OBJ) 
	$(CXX) $(CXXFLAGS) $^ $(LIB) -o $@

$(IDIOT_CLIENT): $(CLIENT_OBJ) 
	$(CXX) $(CXXFLAGS) $^ $(LIB) -o $@

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

$(PROTO_FLAG): $(PROTOS_PATH)/$(PROTO_FILE_PREFIX).proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=./ $<
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=./ --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<
	mkdir -p `dirname $(PROTO_FLAG)`
	touch $@
	
.PHONY: clean
clean:
	rm -rf $(BINDIR) raft*.h raft*.cc


