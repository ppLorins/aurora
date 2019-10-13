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

#include "service/ownership_delegator.h"

namespace RaftCore::Service {

template<typename T>
OwnershipDelegator<T>::OwnershipDelegator() {
    this->m_p_shp_delegator = new std::shared_ptr<T>();
}

template<typename T>
OwnershipDelegator<T>::~OwnershipDelegator() {
    delete this->m_p_shp_delegator;
}

template<typename T>
void OwnershipDelegator<T>::ResetOwnership(T *src) noexcept{
    this->m_p_shp_delegator->reset(src);
}

template<typename T>
void OwnershipDelegator<T>::ReleaseOwnership() noexcept{
    this->m_p_shp_delegator->reset();
}

template<typename T>
std::shared_ptr<T> OwnershipDelegator<T>::GetOwnership()noexcept {
    return *this->m_p_shp_delegator;
}

template<typename T>
void OwnershipDelegator<T>::CopyOwnership(std::shared_ptr<T> from)noexcept {
    *this->m_p_shp_delegator = from;
}

}
