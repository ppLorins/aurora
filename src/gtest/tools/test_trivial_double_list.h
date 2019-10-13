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

#ifndef __GTEST_TRIVIAL_DOUBLE_LIST_H__
#define __GTEST_TRIVIAL_DOUBLE_LIST_H__

#include <list>
#include <memory>
#include <chrono>
#include <random>

#include "gtest/tools/test_data_structure_base.h"
#include "tools/trivial_lock_double_list.h"

using ::RaftCore::DataStructure::TrivialLockDoubleList;
using ::RaftCore::DataStructure::OrderedTypeBase;
using ::RaftCore::DataStructure::DoubleListNode;

class TestDoubletList final : public OrderedTypeBase<TestDoubletList>{

    public:

        TestDoubletList(uint32_t i) : m_i(i) {}

        virtual bool operator<(const TestDoubletList& _other)const noexcept override {
            return this->m_i < _other.m_i;
        }

        virtual bool operator>(const TestDoubletList& _other)const noexcept override {
            return this->m_i > _other.m_i;
        }

        virtual bool operator==(const TestDoubletList& _other)const noexcept override {
            return this->m_i == _other.m_i;
        }

        uint32_t m_i;

};

class TestTrivialLockDoubleList : public DataStructureBase<TrivialLockDoubleList,TestDoubletList> {

    public:

        TestTrivialLockDoubleList(): DataStructureBase(
            std::shared_ptr<TestDoubletList>(new TestDoubletList(0x0)),
            std::shared_ptr<TestDoubletList>(new TestDoubletList(0xFFFFFFFF)) ) {}

    protected:

        virtual void Dump() override {

            auto _print = [](const TestDoubletList &_other) {
                std::cout << _other.m_i << " ";
                return true;
            };

            this->m_ds.Iterate(_print);
        }

        int CheckCutHeadByValue(int _max,bool check_delete=true) {
            DoubleListNode<TestDoubletList>* output_head = this->m_ds.CutHeadByValue(TestDoubletList(_max));
            if (output_head == nullptr) {
                //VLOG(89) << "--------cut head empty---------";
                return 0;
            }

            int _head_size = 0;
            auto _cur = output_head;

            //std::cout << "checking cut by value:" << _max << std::endl;

            std::string _line = "";

            uint32_t _last = 0;
            while (_cur) {
                if (check_delete)
                    CHECK(!_cur->IsDeleted()) << "deleted node found in the output list.";
                CHECK(_last < _cur->m_val->m_i) << "check order fail:" << _last
                    << "|" << _cur->m_val->m_i << ",until now values:" << _line;
                _last = _cur->m_val->m_i;

                //std::cout << _cur->m_val->m_i << " ";
                _line += ("|" + std::to_string(_cur->m_val->m_i));

                _head_size++;
                _cur = _cur->m_atomic_next.load();
            }
            //this->m_ds.ReleaseCutHead(output_head);

            //VLOG(89) << "cuthead values: " << _line;

            return _head_size;
        }

        int CheckCutHeadAdjacent(bool check_delete=true) {
            auto _adjacent_judger = [](const TestDoubletList &a,const TestDoubletList &b)->bool { return b.m_i == a.m_i + 1; };

            DoubleListNode<TestDoubletList>* output_head = this->m_ds.CutHead(_adjacent_judger);
            if (output_head == nullptr) {
                std::cout << "--------cut head empty---------" << std::endl;
                return 0;
            }

            int _head_size = 0;
            auto _cur = output_head;
            //std::cout << std::this_thread::get_id() << " checking cut by adjacent ";
            uint32_t _last = 0;
            std::string _line = "";
            while (_cur) {
                if (check_delete)
                    CHECK(!_cur->IsDeleted()) << "deleted node found in the output list.";
                if (_last > 0)
                    CHECK(_last+1 == _cur->m_val->m_i) << "check adjacent fail:" << _last
                                << "|" << _cur->m_val->m_i << ",total:" << _line;
                _last = _cur->m_val->m_i;
                //std::cout << _cur->m_val->m_i << " ";

                _line += ("|" + std::to_string(_cur->m_val->m_i));

                _head_size++;
                _cur = _cur->m_atomic_next.load();
            }
            //std::cout << std::endl;
            this->m_ds.ReleaseCutHead(output_head);

            return _head_size;
        }
};


TEST_F(TestTrivialLockDoubleList, GeneralOperation) {

    std::shared_ptr<TestDoubletList> _shp_1(new TestDoubletList(3));
    DoubleListNode<TestDoubletList>* _node_1 = new DoubleListNode<TestDoubletList>(_shp_1);

    std::shared_ptr<TestDoubletList> _shp_2(new TestDoubletList(5));
    DoubleListNode<TestDoubletList>* _node_2 = new DoubleListNode<TestDoubletList>(_shp_2);

    _node_1->m_atomic_next.store(_node_2);
    _node_2->m_atomic_pre.store(_node_1);

    DoubleListNode<TestDoubletList>::Apply(_node_1,[](DoubleListNode<TestDoubletList>* p_input) {
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

        //int _avg = (_total + _total / 2) / 2;
        //int val = _over_half_way ? _avg*2-i + _offset : i ;
        int val = _over_half_way ? _total + _offset - (i - _total / 2) : i;

        std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(val));
        DoubleListNode<TestDoubletList>* new_node = new DoubleListNode<TestDoubletList>(_shp);

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
        std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(i));
        this->m_ds.Delete(_shp);
    }
    ASSERT_EQ(this->m_ds.GetSize(), _total - _delete_num);

    //CutHead
    auto _adjacent = [](const TestDoubletList &left, const TestDoubletList &right)->bool {
        return left.m_i + 1 == right.m_i;
    };
    int _adjacent_size = 0;
    DoubleListNode<TestDoubletList>* output_head = this->m_ds.CutHead(_adjacent);
    auto _cur = output_head;
    std::cout << "checking adjacent..." << std::endl;
    while (_cur) {
        CHECK(!_cur->IsDeleted()) << "deleted node found in the output list.";

        std::cout << _cur->m_val->m_i << " ";
        _adjacent_size++;
        _cur = _cur->m_atomic_next.load();
    }
    this->m_ds.ReleaseCutHead(output_head);
    ASSERT_EQ(_adjacent_size, _total / 2 - _delete_num - 1);

    //Cut by value.
    int _less_than = 80;
    output_head = this->m_ds.CutHeadByValue(TestDoubletList(_less_than));
    int _head_size = 0;
    _cur = output_head;
    std::cout << "checking cut by value:" << _less_than << std::endl;
    while (_cur) {
        CHECK(!_cur->IsDeleted()) << "deleted node found in the output list.";

        std::cout << _cur->m_val->m_i << " ";
        _head_size++;
        _cur = _cur->m_atomic_next.load();
    }
    this->m_ds.ReleaseCutHead(output_head);
    ASSERT_EQ(_head_size,_less_than - _total/2 - _offset + 1);

    this->m_ds.DeleteAll();
    ASSERT_EQ(this->m_ds.GetSize(),0);

    this->m_ds.Clear();
    ASSERT_EQ(this->m_ds.GetSize(),0);
}

TEST_F(TestTrivialLockDoubleList, ConcurrentInsert) {

    int _max = 1000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    auto _insert = [&](int idx) {
        auto _tp = this->StartTimeing();

        for (int i = 1; i <= _max/2; ++i) {
            uint32_t insert_val = dis(gen);
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << std::endl;
            std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(insert_val));
            this->m_ds.Insert(_shp);
        }

        this->EndTiming(_tp, "one thread inserting");
    };


    this->LaunchMultipleThread(_insert);

    ASSERT_LE(this->CheckCutHeadByValue(_max),_max);
}

TEST_F(TestTrivialLockDoubleList, ConcurrentInsertDelete) {

    int _max = 1000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    auto _insert_delete = [&](int idx) {
        auto _tp = this->StartTimeing();

        bool _delete_flg = false;
        for (int i = 1; i <= _max/2; ++i) {
            uint32_t insert_val = (dis(gen) % (_max/2));
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << ",i:" << i << std::endl;
            std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(insert_val));
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

TEST_F(TestTrivialLockDoubleList, ConcurrentInsertCutHead) {

    int _max = 10;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    std::atomic<int>    _total_cut_size(0);

    auto _insert_cut = [&](int idx) {
        auto _tp = this->StartTimeing();

        int _counter = 0;

        int _start = _max*idx + 1;
        int _end = _start + _max - 1;

        //std::cout << "start:" << _start << ",end:" << _end << std::endl;

        for (int i = _end; i >= _start; --i) {
            uint32_t insert_val = dis(gen);
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << ",i:" << i << std::endl;

            //VLOG(89) << "inserting " << i;

            std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(i));
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

TEST_F(TestTrivialLockDoubleList, ConcurrentInsertDeleteCutHead) {

    int _max = 2000;
    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, _max);

    auto _insert_delete_cut = [&](int idx) {
        auto _tp = this->StartTimeing();

        bool _delete_flg = false;
        int _counter = 0;
        for (int i = 1; i <= _max/2; ++i) {
            uint32_t insert_val = dis(gen);
            //std::cout << "thread:" << std::this_thread::get_id() << " inserting " << insert_val << ",i:" << i << std::endl;
            std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(insert_val));
            this->m_ds.Insert(_shp);

            if (_delete_flg) {
                //std::cout << "thread:" << std::this_thread::get_id() << " deleting " << insert_val << ",i:" << i << std::endl;
                this->m_ds.Delete(_shp);
            }
            //Reverse the flag.
            _delete_flg = !_delete_flg;

            _counter++;
            if (_counter >= 20) {
                int _size = this->CheckCutHeadByValue(_max,false);
                //std::cout << "interval cut head,size:" << _size << std::endl;
                _counter = 0;
            }

        }

        this->EndTiming(_tp, "one thread inserting");
    };


    this->LaunchMultipleThread(_insert_delete_cut);

    std::cout << "finial cutHead size:" <<this->CheckCutHeadByValue(_max,false) << std::endl;
}

TEST_F(TestTrivialLockDoubleList, Adjacent) {

    std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(1));
    this->m_ds.Insert(_shp);
    this->m_ds.Delete(_shp);
    this->m_ds.Insert(_shp);

    ASSERT_EQ(this->CheckCutHeadAdjacent(), 1);
    ASSERT_EQ(this->CheckCutHeadAdjacent(), 0);

    //Do it again.
    this->m_ds.Insert(_shp);
    this->m_ds.Delete(_shp);
    this->m_ds.Insert(_shp);

    std::shared_ptr<TestDoubletList> _shp_2(new TestDoubletList(3));

    this->m_ds.Insert(_shp_2);
    this->m_ds.Delete(_shp_2);

    this->m_ds.Insert(_shp_2);
    this->m_ds.Delete(_shp_2);

    this->m_ds.Insert(_shp_2);

    ASSERT_EQ(this->CheckCutHeadAdjacent(), 1);
    ASSERT_EQ(this->CheckCutHeadAdjacent(), 1);
}

TEST_F(TestTrivialLockDoubleList, ConcurrentInsertCutHeadAdjacent) {

    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(1, 10);

    auto _insert_cut_adjacent = [&](int idx) {
        auto _tp = this->StartTimeing();

        int _run_times = 8000;
        for (int i = 1; i <= _run_times; ++i) {
            std::shared_ptr<TestDoubletList> _shp(new TestDoubletList(i));
            this->m_ds.Insert(_shp);

            //std::cout << std::this_thread::get_id() << " inserting " << i << std::endl;

            uint32_t _ran_val = dis(gen);
            if (_ran_val & 1) {
                int _size = this->CheckCutHeadAdjacent();
                //std::cout << "interval cut head,size:" << _size << std::endl;
            }
        }

        this->EndTiming(_tp, "one thread inserting");
    };


    this->LaunchMultipleThread(_insert_cut_adjacent);

    std::cout << "finial cutHead size:" <<this->CheckCutHeadAdjacent() << std::endl;
}

#endif
