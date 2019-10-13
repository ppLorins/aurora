/*
*   <Aurora. A raft based distributed KV storage system.>
*   Copyright (C) <2019>  <arthur> <pplorins@gmail.com>

*   This program is free software: you can redistribute it and/or modify
*   it under the terms of the GNU General Public License as published by
*   the Free Software Foundation, either version 3 of the License, or
*   (at your option) any later version.

*   This program is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.

*   You should have received a copy of the GNU General Public License
*   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __AURORA_UTILITIES_H__
#define __AURORA_UTILITIES_H__

#include <stdint.h>
#include <list>
#include <set>
#include <string>
#include <chrono>
#include <cassert>
#include <ctime>
#include <random>

#include "common/log_identifier.h"
#include "boost/crc.hpp"

namespace RaftCore::Tools {

using ::RaftCore::Common::LogIdentifier;

typedef std::chrono::time_point<std::chrono::steady_clock>  TypeTimePoint;
typedef std::chrono::time_point<std::chrono::system_clock>  TypeSysTimePoint;

//Note: The suffix letter of 'x' is to avoid name conflict under darwin.
enum class LocalEndian{ UKNOWN_X, BIG_ENDIAN_X, LITTLE_ENDIAN_X };

inline bool LocalBigEndian() {

    static LocalEndian g_is_local_big_endian = LocalEndian::UKNOWN_X;

    if (g_is_local_big_endian != LocalEndian::UKNOWN_X)
        return (g_is_local_big_endian == LocalEndian::BIG_ENDIAN_X);

    uint32_t uTest = 0x12345678;

    unsigned char* pTest = (unsigned char*)&uTest;

    g_is_local_big_endian = LocalEndian::LITTLE_ENDIAN_X;
    if ((*pTest) == 0x12) {
        g_is_local_big_endian = LocalEndian::BIG_ENDIAN_X;
        return true;
    }

    return false;
}

template<typename _type>
inline void ConvertToBigEndian(_type input, _type *output) {

    //Note:"input" is a copied.
    assert(output != nullptr && output != &input);

    if (LocalBigEndian()) {
        *output = input;
        return;
    }

    unsigned char* pCur     = (unsigned char*)&input;
    unsigned char* pTarget  = (unsigned char*)output;

    int iter_cnt = sizeof(_type) - 1;
    for (int i = 0; i <= iter_cnt; ++i)
        pTarget[iter_cnt-i] = pCur[i];
}

template<typename _type>
inline void ConvertBigEndianToLocal(_type input, _type *output) {
    ConvertToBigEndian(input, output);
}

void GetLocalIPs(std::list<std::string>& ips);

uint32_t RoundUp(uint32_t num);

uint32_t GetMask(uint32_t num);

inline uint32_t CalculateCRC32(const void* data, unsigned int len) {
    boost::crc_32_type  crc_result;
    crc_result.process_bytes(data,len);
    return crc_result.checksum();
}

TypeTimePoint StartTimeing();

void EndTiming(const TypeTimePoint &tp_start, const char* operation_name, const LogIdentifier *p_cur_id = nullptr);

void StringSplit(const std::string &input, char delimiter, std::set<std::string> &output);

void StringSplit(const std::string &input, char delimiter, std::list<std::string> &output);

std::string TimePointToString(const TypeSysTimePoint &tp);

uint32_t GenerateRandom(uint32_t from, uint32_t to);

template<typename T>
inline uint32_t SizeOfX() noexcept {
    return sizeof(T);
}

template<>
inline uint32_t SizeOfX<void>() noexcept {
    return 0;
}

}

#endif

