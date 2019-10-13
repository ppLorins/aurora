

@echo off
SETLOCAL
CALL :myDosFunc raft.proto

@pause
EXIT /B %ERRORLEVEL%

:myDosFunc

@set grpc_cpp_plugin_bin="C:\Users\95\.babun\cygwin\home\arthur\git\grpc\build-dir\Debug\grpc_cpp_plugin.exe"
@"C:\Users\95\.babun\cygwin\home\arthur\git\protobuf\build-dir\Debug\protoc.exe" --grpc_out=. --plugin=protoc-gen-grpc=%grpc_cpp_plugin_bin%  %~1
@if not errorlevel 0 (
   @echo generate crm_trans fail
   @pause
)


@cd C:\Users\95\Documents\Visual Studio 2015\Projects\apollo\raft\src\protocol
@"C:\Users\95\.babun\cygwin\home\arthur\git\protobuf\build-dir\Debug\protoc.exe" --cpp_out=. %~1
@if not errorlevel 0 (
   @echo generate srpc fail
   @pause
)

@echo generate %~1  succ !

EXIT /B 0




