# build under windows:
##### 0. Rerquire :
  * visual studio >= 2015.
  * vs compiler >= 19.00.24215.1.

*Note: make sure modify the directory prefix to your own for each of the following example configure.*

##### 1. configure preprocessor
* Go to `Project properties->c/c++->Preprocessor->Proprocessor Definitions` with values:
  * _CRT_SECURE_NO_WARNINGS
  * _SCL_SECURE_NO_WARNINGS
  * _WIN32_WINNT=0x0A00
  * GLOG_NO_ABBREVIATED_SEVERITIES
  * GFLAGS_DLL_DEFINE_FLAG=
  * GTEST_HAS_TR1_TUPLE=0
  * _HAS_AUTO_PTR_ETC
* configure `Project properties->c/c++->All options->Additional Options` with `/std:c++latest`.

##### 2. configure Include Directories
Go to `Project properties->c/c++->General->Additional`, do it for each lib. Example of mine:

```
C:\Users\95\Documents\Visual Studio 2015\Projects\apollo\raft\src
D:\third_party\boost_1_64_0
C:\Users\95\.babun\cygwin\home\arthur\git\glog\src\windows
C:\Users\95\.babun\cygwin\home\arthur\git\gflags\cmake\build\include
C:\Users\95\.babun\cygwin\home\arthur\git\protobuf\src
C:\Users\95\.babun\cygwin\home\arthur\git\grpc\include
C:\Users\95\.babun\cygwin\home\arthur\git\googletest\googletest\include
```

##### 3. configure Library Directories
* Go to `Project properties->Linker->General->Additional Library Directories.` Example of mine:
```
C:\Users\95\.babun\cygwin\home\arthur\git\googletest\googletest\build\Debug
D:\third_party\boost_1_64_0\bin.v2\libs_summary
C:\Users\95\.babun\cygwin\home\arthur\git\glog\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\gflags\cmake\build\lib\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\protobuf\cmake\build\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\grpc\build-dir\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\grpc\build-dir\third_party\boringssl\crypto\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\grpc\build-dir\third_party\boringssl\ssl\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\grpc\build-dir\third_party\zlib\Debug
C:\Users\95\.babun\cygwin\home\arthur\git\grpc\third_party\cares\cares\build-dir\bin\Release
C:\Users\95\.babun\cygwin\home\arthur\git\gperftools\x64\Release-Patch
```

* Go to `Project properties->Linker->General->Input->Additional Dependencies.` Example of mine:
```
gtest.lib
grpc.lib
grpc++.lib
libprotobuf.lib
libprotobuf-lite.lib
gpr.lib
ws2_32.lib
gflags_static.lib
libglog_static.lib
shlwapi.lib
zlib.lib
ssl.lib
crypto.lib
cares.lib
address_sorting.lib
libtcmalloc_minimal.lib
```


