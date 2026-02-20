/**
 * \file median.hpp
 * \brief Инкрементальный расчёт медианы на основе min-max heap
 */

#pragma once

#include <cstdint>
#include <functional>
#include <queue>
#include <stdexcept>

namespace csv_median {

    /**
     * \brief Калькулятор инкрементальной медианы
     *
     * Поддерживает добавление значений по одному и
     * возвращает актуальную медиану после каждого добавления.
     * Фиксирует факт изменения медианы для оптимизации записи.
     */
    class calculator {
    public:
        calculator() noexcept = default;

        /**
         * \brief Добавить новое значение цены
         * \param price_ новое значение для учёта в медиане
         */
        void add(double price_) noexcept;

        /**
         * \brief Получить текущую медиану
         * \return медиана всех добавленных значений
         * \warning Вызов до добавления хотя бы одного значения — UB
         */
        [[nodiscard]] double median() const noexcept;

        /**
         * \brief Проверить, изменилась ли медиана после последнего вызова add()
         * \return true если медиана изменилась
         */
        [[nodiscard]] bool is_changed() const noexcept;

        /**
         * \brief Количество добавленных значений
         */
        [[nodiscard]] std::size_t count() const noexcept;

        /**
         * \brief Есть ли хотя бы одно значение
         */
        [[nodiscard]] bool has_values() const noexcept;

    private:
        // Нижняя половина значений (максимум наверху)
        std::priority_queue<double> _lower;

        // Верхняя половина значений (минимум наверху)
        std::priority_queue<double, std::vector<double>, std::greater<double>>
            _upper;

        double      _last_median{ 0.0 };
        bool        _changed{ false };

        /**
         * \brief Балансировка куч: разница размеров не должна превышать 1.
         * Инвариант: _lower.size() >= _upper.size()
         */
        void balance() noexcept;

        /**
         * \brief Вычислить медиану из текущего состояния куч
         */
        [[nodiscard]] double compute_median() const noexcept;
    };

    // ──────────────────────────────────────────────
    // Реализация (inline, т.к. header-only)
    // ──────────────────────────────────────────────

    inline void calculator::add(double price_) noexcept {
        // Направляем значение в нужную кучу
        if (_lower.empty() || price_ <= _lower.top()) {
            _lower.push(price_);
        }
        else {
            _upper.push(price_);
        }

        balance();

        const double new_median = compute_median();
        _changed = (new_median != _last_median);
        _last_median = new_median;
    }

    inline double calculator::median() const noexcept {
        return _last_median;
    }

    inline bool calculator::is_changed() const noexcept {
        return _changed;
    }

    inline std::size_t calculator::count() const noexcept {
        return _lower.size() + _upper.size();
    }

    inline bool calculator::has_values() const noexcept {
        return !_lower.empty();
    }

    inline void calculator::balance() noexcept {
        // Инвариант: _lower.size() == _upper.size() или _lower.size() == _upper.size() + 1
        if (_lower.size() > _upper.size() + 1) {
            _upper.push(_lower.top());
            _lower.pop();
        }
        else if (_upper.size() > _lower.size()) {
            _lower.push(_upper.top());
            _upper.pop();
        }
    }

    inline double calculator::compute_median() const noexcept {
        if (_lower.size() == _upper.size()) {
            // Чётное количество — среднее двух центральных
            return (_lower.top() + _upper.top()) / 2.0;
        }
        // Нечётное — верхний элемент нижней кучи
        return _lower.top();
    }

}