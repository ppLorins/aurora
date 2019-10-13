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

#ifndef __GTEST_LOCK_FREE_HASH_H__
#define __GTEST_LOCK_FREE_HASH_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/tools/test_data_structure_base.h"
#include "tools/lock_free_hash.h"
#include "tools/lock_free_hash_specific.h"

using ::RaftCore::DataStructure::LockFreeHash;
using ::RaftCore::DataStructure::HashNodeAtomic;
using ::RaftCore::DataStructure::HashTypeBase;
using ::RaftCore::DataStructure::LockFreeHashAtomic;

template<typename T>
class DataStructureHash {

    public:

        template<typename ...Args>
        DataStructureHash(Args&&... args) : m_ds(std::forward<Args>(args)...) { }

        virtual ~DataStructureHash() {}

    protected:

        T      m_ds;

        virtual void Dump() = 0;
};


class TestHash final : public HashTypeBase<TestHash>{

    public:

        TestHash(int i) :m_i(i) {}

        virtual bool operator<(const TestHash& _other)const noexcept override {
            return m_i < _other.m_i;
        }

        virtual bool operator==(const TestHash& _other)const noexcept override {
            return m_i == _other.m_i;
        }

        virtual std::size_t Hash() const noexcept override {
            return this->m_i;
        }

        int m_i;
};

typedef std::shared_ptr<TestHash> TypePtrTestHash;

template<typename T>
class TestLockFreeHashBase : public TestBase, public DataStructureHash<T> {

    protected:

        TestLockFreeHashBase() :DataStructureHash<T>(500) {}

        virtual ~TestLockFreeHashBase() noexcept{}

    protected:

        virtual void SetUp() override {}

        virtual void TearDown() override {}

        virtual void Dump() override {
            std::list<std::shared_ptr<TestHash>> _output;
            this->m_ds.GetOrderedByKey(_output);

            for (auto &_item : _output)
                std::cout << _item->m_i << " ";
        }

    protected:

        int     m_sum = 10000;
};

class TestLockFreeHash : public TestLockFreeHashBase<LockFreeHash<TestHash,int>> {

    protected:

        TestLockFreeHash() {}

        virtual ~TestLockFreeHash() noexcept{}

        void GeneralOperation()noexcept {

            int val = 7;

            auto _tp = this->StartTimeing();

            for (int i = 0; i < this->m_sum;++i) {
                std::shared_ptr<TestHash> _shp_key(new TestHash(val+i));
                std::shared_ptr<int>      _shp_val(new int(val+i));
                this->m_ds.Insert(_shp_key,_shp_val);
            }

            //Do insert again ,should overwrite all the previously inserted values.
            for (int i = 0; i < this->m_sum;++i) {
                std::shared_ptr<TestHash> _shp_key(new TestHash(val+i));
                std::shared_ptr<int>      _shp_val(new int(val+i));
                this->m_ds.Insert(_shp_key,_shp_val);
            }

            ASSERT_TRUE(this->m_ds.Size() == this->m_sum);

            std::shared_ptr<int> _shp_val;
            this->m_ds.Read(TestHash(this->m_sum),_shp_val);
            ASSERT_TRUE(*_shp_val == this->m_sum);

            /*
            int _new_val = 17;
            std::shared_ptr<int> _shp_val_2(new int(_new_val));
            TestHash *_p_obj = new TestHash(this->m_sum);
            if (!this->m_ds.Upsert(_p_obj, _shp_val_2))
                delete _p_obj;

            _shp_val.reset();
            this->m_ds.Read(TestHash(this->m_sum),_shp_val);
            ASSERT_TRUE(*_shp_val == _new_val);
            */

            auto _traverse = [](const std::shared_ptr<TestHash> &k,const std::shared_ptr<int> &v)->bool {
                //std::cout << "k:" << k->Hash() << ",v:" << *v << std::endl;
                return true;
            };

            this->m_ds.Iterate(_traverse);

            auto _cond = [&](const TestHash &x) ->bool{
                return x.m_i >= val;
            };

            ASSERT_TRUE(this->m_ds.CheckCond(_cond));
            this->EndTiming(_tp, "CheckCond");

            std::list<std::shared_ptr<TestHash>> _output;
            this->m_ds.GetOrderedByKey(_output);

            LockFreeHash<TestHash,int>::ValueComparator _func = [](const std::shared_ptr<int> &left, const std::shared_ptr<int> &right) {
                return *left < *right;
            };

            std::map<std::shared_ptr<int>,TypePtrTestHash,decltype(_func)> _output_val(_func);

            this->m_ds.GetOrderedByValue(_output_val);

            ASSERT_EQ(_output_val.size(), this->m_sum);
            ASSERT_EQ(*_output_val.cbegin()->first, val);

            this->EndTiming(_tp, "GetOrder");

            _tp = this->StartTimeing();

            ASSERT_EQ(_output.size(), this->m_sum);
            ASSERT_EQ(_output.front()->m_i, val);

            ASSERT_TRUE(this->m_ds.Find(*_output.front()));
            ASSERT_FALSE(this->m_ds.Find(TestHash(6)));

            this->m_ds.Delete(*_output.front());

            this->EndTiming(_tp, "find & delete");

            _tp = this->StartTimeing();

            this->m_ds.GetOrderedByKey(_output);
            ASSERT_EQ(_output.size(), this->m_sum-1);
            this->EndTiming(_tp, "GetOrderedByKey");

            int _adder = 10;
            auto _modifier = [&](std::shared_ptr<TestHash> &x) ->void{
                x->m_i += _adder;
            };
            this->m_ds.Map(_modifier);
            this->m_ds.GetOrderedByKey(_output);
            ASSERT_EQ(_output.front()->m_i, val + _adder + 1);

            this->m_ds.Clear();
            this->m_ds.GetOrderedByKey(_output);
            ASSERT_EQ(_output.size(), 0);
        }

        void ConcurrentOperation() noexcept {

            //The program will consume more memory as _counter increasing due to its inner mechanism.
            int _counter = 10000;
            auto _insert = [&](int idx) {
                auto _tp = this->StartTimeing();
                bool _flg = true;
                for (int i = 0; i < _counter; ++i) {

                    int _val = idx * _counter + i;
                    std::shared_ptr<TestHash> _shp_key(new TestHash(_val));
                    std::shared_ptr<int>  _shp_val(new int(_val));
                    this->m_ds.Insert(_shp_key,_shp_val);
                    ASSERT_TRUE(this->m_ds.Find(*_shp_key));

                    this->m_ds.Read(TestHash(_val),_shp_val);
                    ASSERT_TRUE(*_shp_val==_val);

                    if (_flg) {
                        this->m_ds.Delete(*_shp_key);
                        ASSERT_FALSE(this->m_ds.Find(*_shp_key));
                    }
                    _flg = !_flg;
                }

                this->EndTiming(_tp, "one thread inserting");
            };

            this->LaunchMultipleThread(_insert);

            auto _tp = this->StartTimeing();

            std::list<std::shared_ptr<TestHash>> _output;
            this->m_ds.GetOrderedByKey(_output);

            this->EndTiming(_tp, "get ordered");

            ASSERT_EQ(_output.size(), this->m_cpu_cores * _counter/2 );
        }
};

class TestLockFreeHashAtomic : public TestLockFreeHashBase<LockFreeHashAtomic<TestHash,int>> {

    protected:

        TestLockFreeHashAtomic() {}

        virtual ~TestLockFreeHashAtomic() noexcept{}

};

TEST_F(TestLockFreeHash, GeneralOperation) {

    this->GeneralOperation();
}

TEST_F(TestLockFreeHash, ConcurrentOperation) {

    this->ConcurrentOperation();
}

TEST_F(TestLockFreeHash, Allocation) {
    for (int i = 0; i < 1000; ++i)
        LockFreeHash<TestHash, int> obj;
}

TEST_F(TestLockFreeHashAtomic, Specifics) {

    //this->GeneralOperation();

    int _new_val = 17;

    int* _p_val = new int(_new_val);

    TestHash *_p_obj = new TestHash(this->m_sum);
    if (!this->m_ds.Upsert(_p_obj, _p_val))
        delete _p_obj;

    std::shared_ptr<int> _shp_val;
    this->m_ds.Read(TestHash(this->m_sum),_shp_val);
    ASSERT_EQ(*_shp_val, _new_val);
}

#endif
