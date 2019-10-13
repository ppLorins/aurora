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

#ifndef __GTEST_TRIVIAL_SINGLE_LIST_H__
#define __GTEST_TRIVIAL_SINGLE_LIST_H__

#include <list>
#include <memory>
#include <chrono>
#include <random>

#include "gtest/tools/test_data_structure_base.h"
#include "tools/trivial_lock_single_list.h"

using ::RaftCore::DataStructure::TrivialLockSingleList;
using ::RaftCore::DataStructure::OrderedTypeBase;
using ::RaftCore::DataStructure::SingleListNode;

class TestSingletList final : public OrderedTypeBase<TestSingletList>{

    public:

        TestSingletList(uint32_t i) : m_i(i) {}

        virtual bool operator<(const TestSingletList& _other)const noexcept override {
            return m_i < _other.m_i;
        }

        virtual bool operator>(const TestSingletList& _other)const noexcept override {
            return m_i > _other.m_i;
        }

        virtual bool operator==(const TestSingletList& _other)const noexcept override {
            return m_i == _other.m_i;
        }

        uint32_t m_i;

};

class TestTrivialSingleLockList : public DataStructureBase<TrivialLockSingleList,TestSingletList> {

    public:

        TestTrivialSingleLockList(): DataStructureBase(
            std::shared_ptr<TestSingletList>(new TestSingletList(0x0)),
            std::shared_ptr<TestSingletList>(new TestSingletList(0xFFFFFFFF)) ) {}

    protected:

        virtual void Dump() override {

            auto _print = [](std::shared_ptr<TestSingletList> &_other) {
                std::cout << _other->m_i << " ";
                return true;
            };

            this->m_ds.Iterate(_print);
        }

        int CheckCutHeadByValue(int _max, bool check_delete = true) {
            SingleListNode<TestSingletList>* output_head = this->m_ds.CutHeadByValue(TestSingletList(_max));
            if (output_head == nullptr) {
                //VLOG(89) << "--------cut head empty---------";
                return 0;
            }

            int _head_size = 0;
            auto _cur = output_head;

            //std::cout << "checking cut by value:" << _max << std::endl;

            std::string _line = "";

            uint32_t _last = 0;
            bool _first = true;

            while (_cur) {
                if (check_delete)
                    CHECK(!_cur->IsDeleted()) << "deleted node found in the output list.";

                //bool _update_last = true;

                if (!_first) {
                    //CHECK(_last < _cur->m_val->m_i) << "check order fail:" << _last << "|" << _cur->m_val->m_i << ",until now values:" << _line;
                    if (_last >= _cur->m_val->m_i) {
                        VLOG(89) << "check order fail:" << _last << "|" << _cur->m_val->m_i
                                << ",until now values:" << _line;
                        std::cout << "error occur!" << std::endl;
                    }
                    //_update_last = false;
                }

                //if (_update_last)
                _last = _cur->m_val->m_i;

                _first = false;

                //std::cout << _cur->m_val->m_i << " ";
                _line += ("|" + std::to_string(_cur->m_val->m_i));

                _head_size++;
                _cur = _cur->m_atomic_next.load();
            }

            this->m_ds.ReleaseCutHead(output_head);

            VLOG(89) << "cuthead values: " << _line;

            return _head_size;
        }
};


TEST_F(TestTrivialSingleLockList, GeneralOperation) {

    std::shared_ptr<TestSingletList> _shp_1(new TestSingletList(3));
    SingleListNode<TestSingletList>* _node_1 = new SingleListNode<TestSingletList>(_shp_1);

    std::shared_ptr<TestSingletList> _shp_2(new TestSingletList(5));
    SingleListNode<TestSingletList>* _node_2 = new SingleListNode<TestSingletList>(_shp_2);

    _node_1->m_atomic_next.store(_node_2);

    SingleListNode<TestSingletList>::Apply(_node_1,[](SingleListNode<TestSingletList>* p_input) {
        std::cout << "element: " << p_input->m_val->m_i << std::endl;
    });

    //Insert 100 elements.
    int _total = 100;
    int _offset = 10;
    for (int i = 1; i <= _total;++i) {

        bool _over_half_way = false;
        if (i >= _total / 2) {
            _over_half_way = true;
        }

        int val = _over_half_way ? _total + _offset - (i - _total / 2) : i;

        std::shared_ptr<TestSingletList> _shp(new TestSingletList(val));
        SingleListNode<TestSingletList>* new_node = new SingleListNode<TestSingletList>(_shp);

        if (_over_half_way) {
            this->m_ds.Insert(new_node);
            continue;
        }

        this->m_ds.Insert(_shp);
    }
    this->Dump();
    ASSERT_EQ(this->m_ds.GetSize(), _total);

    //Delete 10 elements.
    int _delete_num = 10;
    for (int i = 1; i <= _delete_num;++i) {
        std::shared_ptr<TestSingletList> _shp(new TestSingletList(i));
        this->m_ds.Delete(_shp);
    }
    ASSERT_EQ(this->m_ds.GetSize(), _total - _delete_num);

    //Cut by value.
    int _less_than = 80;
    auto *output_head = this->m_ds.CutHeadByValue(TestSingletList(_less_than));
    int _head_size = 0;
    auto *_cur = output_head;
    std::cout << "checking cut by value:" << _less_than << std::endl;
    while (_cur) {
        CHECK(!_cur->IsDeleted()) << "deleted node found in the output list.";

        std::cout << _cur->m_val->m_i << " ";
        _head_size++;
        _cur = _cur->m_atomic_next.load();
    }
    this->m_ds.ReleaseCutHead(output_head);
    ASSERT_EQ(_head_size, _less_than - _delete_num - _offset);

    this->m_ds.Clear();
    ASSERT_EQ(this->m_ds.GetSize(),0);
}

TEST_F(TestTrivialSingleLockList, ConcurrentInsert) {

    int _max = 1000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    auto _insert = [&](int idx) {
        auto _tp = this->StartTimeing();

        for (int i = 1; i <= _max/2; ++i) {
            uint32_t insert_val = dis(gen);
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << std::endl;
            std::shared_ptr<TestSingletList> _shp(new TestSingletList(insert_val));
            this->m_ds.Insert(_shp);
        }
    };

    this->LaunchMultipleThread(_insert);

    ASSERT_LE(this->CheckCutHeadByValue(_max),_max);
}

TEST_F(TestTrivialSingleLockList, ConcurrentInsertDelete) {

    int _max = 1000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    auto _insert_delete = [&](int idx) {

        VLOG(89) << "unit test concurrent thread started";

        auto _tp = this->StartTimeing();

        bool _delete_flg = false;
        for (int i = 1; i <= _max/2; ++i) {
            uint32_t insert_val = (dis(gen) % (_max/2));
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << ",i:" << i << std::endl;
            std::shared_ptr<TestSingletList> _shp(new TestSingletList(insert_val));
            this->m_ds.Insert(_shp);

            if (_delete_flg) {
                //std::cout << "thread:" << std::this_thread::get_id() << " deleting " << insert_val << ",i:" << i << std::endl;
                this->m_ds.Delete(_shp);
            }

            //Reverse the flag.
            _delete_flg = !_delete_flg;
        }

        this->EndTiming(_tp, "one thread inserting");
    };

    this->LaunchMultipleThread(_insert_delete);

    ASSERT_LE(this->CheckCutHeadByValue(_max),_max/2);
}

TEST_F(TestTrivialSingleLockList, ConcurrentInsertCutHead) {

    int _max = 2000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    std::atomic<int>    _total_cut_size(0);

    auto _insert_cut = [&](int idx) {
        auto _tp = this->StartTimeing();

        VLOG(89) << "unite test thread spawned";

        int _counter = 0;

        int _start = _max*idx + 1;
        int _end = _start + _max - 1;

        VLOG(89) << "start:" << _start << ",end:" << _end << std::endl;

        for (int i = _end; i >= _start; --i) {
            uint32_t insert_val = dis(gen);
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << ",i:" << i << std::endl;

            //VLOG(89) << "inserting " << i;

            std::shared_ptr<TestSingletList> _shp(new TestSingletList(i));
            this->m_ds.Insert(_shp);

            //VLOG(89) << "inserted " << i;

            _counter++;
            if (_counter >= 10) {
                int _size = this->CheckCutHeadByValue(_end,false);

                //VLOG(89) << "interval cut head,size:" << _size;

                _counter = 0;
                _total_cut_size.fetch_add(_size);
            }
        }

        this->EndTiming(_tp, "one thread inserting");
    };

    int _thread_num = this->m_cpu_cores;
    //int _thread_num = 6;
    this->LaunchMultipleThread(_insert_cut, _thread_num);

    ASSERT_EQ(_total_cut_size.load(), _max * _thread_num);

    std::cout << "finial cutHead size:" <<this->CheckCutHeadByValue(_max,false) << std::endl;
}

TEST_F(TestTrivialSingleLockList, ConcurrentInsertDeleteCutHead) {

    int _max = 2000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    std::atomic<int>    _total_cut_size(0);

    auto _insert_delete_cut = [&](int idx) {
        auto _tp = this->StartTimeing();

        bool _delete_flg = false;
        int _counter = 0;
        for (int i = 1; i <= _max/2; ++i) {
            uint32_t insert_val = dis(gen);
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << ",i:" << i << std::endl;
            std::shared_ptr<TestSingletList> _shp(new TestSingletList(insert_val));
            this->m_ds.Insert(_shp);

            if (_delete_flg) {
                //std::cout << "thread:" << std::this_thread::get_id() << " deleting " << insert_val << ",i:" << i << std::endl;
                this->m_ds.Delete(_shp);
            }
            //Reverse the flag.
            _delete_flg = !_delete_flg;

            _counter++;
            if (_counter >= 20) {
                int _size = this->CheckCutHeadByValue(_max, false);
                //std::cout << "interval cut head,size:" << _size << std::endl;
                _counter = 0;
                _total_cut_size.fetch_add(_size);
            }
        }

        this->EndTiming(_tp, "one thread inserting");
    };

    this->LaunchMultipleThread(_insert_delete_cut);

    std::cout << "finial cutHead size:" << this->CheckCutHeadByValue(_max, false)
        << ",total CutHead size:" << _total_cut_size.load() << std::endl;
}

#endif
