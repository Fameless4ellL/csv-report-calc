/**
 * \file test_pool.cpp
 * \brief Unit-тесты для pool
 */

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <numeric>
#include <vector>

#include "thread_pool.hpp"

using csv_median::thread_pool;

TEST_CASE("pool - submit and get result", "[pool]") {
    thread_pool pool{ 2 };

    auto future = pool.submit([] { return 42; });

    CHECK(future.get() == 42);
}

TEST_CASE("pool - parallel tasks execute concurrently", "[pool]") {
    thread_pool pool{ 4 };

    std::atomic<int> counter{ 0 };
    std::vector<std::future<void>> futures;

    for (int i = 0; i < 100; ++i) {
        futures.push_back(pool.submit([&counter] {
            counter.fetch_add(1, std::memory_order_relaxed);
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    CHECK(counter.load() == 100);
}

TEST_CASE("pool - thread count is correct", "[pool]") {
    thread_pool pool{ 4 };
    CHECK(pool.thread_count() == 4);
}

TEST_CASE("pool - single thread pool works", "[pool]") {
    thread_pool pool{ 1 };

    std::vector<int> order;
    std::mutex mtx;

    std::vector<std::future<void>> futures;
    for (int i = 0; i < 5; ++i) {
        futures.push_back(pool.submit([&order, &mtx, i] {
            std::lock_guard lock{ mtx };
            order.push_back(i);
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    CHECK(order.size() == 5);
}

TEST_CASE("pool - tasks with return values", "[pool]") {
    thread_pool pool{ 4 };

    std::vector<std::future<int>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(pool.submit([i] { return i * i; }));
    }

    int sum = 0;
    for (auto& f : futures) {
        sum += f.get();
    }

    // 0+1+4+9+16+25+36+49+64+81 = 285
    CHECK(sum == 285);
}