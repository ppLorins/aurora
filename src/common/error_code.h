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

#ifndef __AURORA_ERROR_CODE_H__
#define __AURORA_ERROR_CODE_H__

/*--------------Produce & consume operation return values--------------*/
#define  QUEUE_ERROR           (-1)
#define  QUEUE_SUCC            (0)
#define  QUEUE_FULL            (1)
#define  QUEUE_EMPTY           (2)


/*--------------storage error codes--------------*/
#define  SUCC               (0)
#define  LEFT_BEHIND        (-1)


#endif
