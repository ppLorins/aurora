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

#include "glog/logging.h"
#include "boost/asio.hpp"

#include "tools/utilities.h"

#define _FOUR_BYTES_BITS_ (32)

namespace RaftCore::Tools {

void GetLocalIPs(std::list<std::string>& ips){

    boost::asio::io_service io_service;
    boost::asio::ip::tcp::resolver resolver(io_service);

    std::string h = boost::asio::ip::host_name();

    std::for_each(resolver.resolve({ h, "" }), {}, [&](const auto& re) {
        ips.emplace_back(re.endpoint().address().to_string());
    });

    //Forcibly add loop back address due to it may not appear under certain platforms.
    const static std::string _loop_back_addr = "127.0.0.1";
    if (std::find(ips.cbegin(), ips.cend(), _loop_back_addr) == ips.cend())
        ips.emplace_back(_loop_back_addr);
}

uint32_t RoundUp(uint32_t num) {

    const static uint32_t mask = 0x80000000;

    uint32_t tmp  = num;
    uint32_t ret  = 0x80000000;

    for (int i=0; i < _FOUR_BYTES_BITS_; ++i) {
        if (tmp & mask)
            break;
        tmp <<= 1;
        ret >>= 1;
    }

    int _shift = (tmp & ~mask)? 1 : 0 ;
    return ret << _shift;
}

uint32_t GetMask(uint32_t num) {

    uint32_t tmp  = num;
    uint32_t mask = 0x80000000;

    int _counter = 1;
    for (int i=0; i < _FOUR_BYTES_BITS_; ++i) {
        if (tmp & mask)
            break;
        tmp <<= 1;
        _counter++;
    }

    uint32_t ret  = 0x1;
    for (int i = 0; i < _FOUR_BYTES_BITS_ - _counter - 1; ++i) {
        ret <<= 1;
        ret++;
    }

    return ret;
}

TypeTimePoint StartTimeing() {
    return std::chrono::steady_clock::now();
}

uint64_t EndTiming(const TypeTimePoint &tp_start, const char* operation_name, const LogIdentifier *p_cur_id) {
    auto _now = std::chrono::steady_clock::now();
    std::chrono::microseconds _us = std::chrono::duration_cast<std::chrono::microseconds>(_now - tp_start);

    uint64_t _cost_us = _us.count();

    if (p_cur_id == nullptr)
        VLOG(88) << operation_name << " cost us:" << _cost_us;
    else
        VLOG(88) << operation_name << " cost us:" << _cost_us << " ,idx:" << *p_cur_id;

    return _cost_us;
}

void StringSplit(const std::string &input, char delimiter, std::set<std::string> &output) {
    std::list<std::string>  _output;
    StringSplit(input, delimiter, _output);
    for (const auto &_item : _output)
        output.emplace(_item);
}

void StringSplit(const std::string &input, char delimiter, std::list<std::string> &output) {

    std::size_t _pos = 0, _cur_pos=0;

    while ((_cur_pos = input.find(delimiter, _pos)) != std::string::npos) {
        std::size_t _len = _cur_pos - _pos ;
        if (_len > 0)
            output.emplace_back(input.substr(_pos, _len));

        _pos = _cur_pos;
        if (++_pos >= input.length())
            break;
    }

    if (_pos < input.length())
        output.emplace_back(input.substr(_pos));
}

std::string TimePointToString(const TypeSysTimePoint &tp){

    //Get seconds.
    char _buf[128];
    std::time_t _t = std::chrono::system_clock::to_time_t(tp);
    std::tm * _ptm = std::localtime(&_t);
    std::strftime(_buf, 32, "%Y.%m.%d %a, %H:%M:%S", _ptm);

    //Get milliseconds.
    char _result[128];
    std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
    std::snprintf(_result,sizeof(_result),"%s.%llu",_buf,ms.count() % 1000);

    return std::string(_result);
}

uint32_t GenerateRandom(uint32_t from, uint32_t to) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned long> dis(from,to);
    return dis(gen);
}

}



