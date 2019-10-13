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

#pragma once

#ifndef __AURORA_BIN_LOG_SINGLETON_H__
#define __AURORA_BIN_LOG_SINGLETON_H__

#include "binlog/binlog_operator.h"

namespace RaftCore::BinLog {

using ::RaftCore::BinLog::BinLogOperator;

class BinLogGlobal final{

public:

    static BinLogOperator      m_instance;

private:

    BinLogGlobal() = delete;

    virtual ~BinLogGlobal() = delete;

    BinLogGlobal(const BinLogGlobal&) = delete;

    BinLogGlobal& operator=(const BinLogGlobal&) = delete;

};

} //end namespace


#endif
