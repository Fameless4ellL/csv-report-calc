/**
 * \file pool.hpp
 * \brief Пул потоков с очередью задач
 */

#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

namespace csv_median {

    /**
     * \brief Пул потоков с очередью задач фиксированного размера
     *
     * Потоки создаются один раз при конструировании
     */
    class thread_pool {
    public:
        /**
         * \brief Создать пул с заданным числом потоков
         * \param thread_count_ число рабочих потоков.
         *        По умолчанию — hardware_concurrency().
         */
        explicit thread_pool(
            std::size_t thread_count_ =
            std::thread::hardware_concurrency()) noexcept;

        ~thread_pool() noexcept;
        thread_pool(const thread_pool&) = delete;
        thread_pool& operator=(const thread_pool&) = delete;
        thread_pool(thread_pool&&) = delete;
        thread_pool& operator=(thread_pool&&) = delete;

        template<class F>
        [[nodiscard]] auto submit(F&& task_)
            -> std::future<std::invoke_result_t<F>>;

        [[nodiscard]] std::size_t thread_count() const noexcept;

    private:
        void worker_loop() noexcept;

        std::vector<std::thread>          _workers;
        std::queue<std::function<void()>> _tasks;
        std::mutex                        _mutex;
        std::condition_variable           _cv;
        bool                              _stop{ false };
    };

    inline thread_pool::thread_pool(std::size_t thread_count_) noexcept {
        const std::size_t count = std::max(std::size_t{ 1 }, thread_count_);
        _workers.reserve(count);

        for (std::size_t i = 0; i < count; ++i) {
            _workers.emplace_back([this] { worker_loop(); });
        }
    }

    inline thread_pool::~thread_pool() noexcept {
        {
            std::unique_lock lock{ _mutex };
            _stop = true;
        }
        _cv.notify_all();

        for (auto& worker : _workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    inline void thread_pool::worker_loop() noexcept {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock{ _mutex };

                _cv.wait(lock, [this] {
                    return _stop || !_tasks.empty();
                    });

                if (_stop && _tasks.empty()) {
                    return;
                }

                task = std::move(_tasks.front());
                _tasks.pop();
            }
            task();
        }
    }

    template<class F>
    auto thread_pool::submit(F&& task_) -> std::future<std::invoke_result_t<F>> {
        using result_t = std::invoke_result_t<F>;

        auto pkg = std::make_shared<std::packaged_task<result_t()>>(
            std::forward<F>(task_)
        );
        auto future = pkg->get_future();

        {
            std::unique_lock lock{ _mutex };
            if (_stop) {
                throw std::runtime_error{ "thread_pool: submit after shutdown" };
            }
            // для единообразного хранения
            _tasks.emplace([pkg] { (*pkg)(); });
        }
        _cv.notify_one();
        return future;
    }

    inline std::size_t thread_pool::thread_count() const noexcept {
        return _workers.size();
    }

}