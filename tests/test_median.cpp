/**
 * \file test_median_calculator.cpp
 * \brief Unit-тесты для median_calculator
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "median.hpp"

using csv_median::calculator;
using Catch::Approx;

TEST_CASE("median — базовое поведение", "[median]") {

    SECTION("начальное состояние") {
        calculator calc;
        CHECK_FALSE(calc.has_values());
        CHECK(calc.count() == 0);
    }

    SECTION("одно значение") {
        calculator calc;
        calc.add(100.0);

        CHECK(calc.has_values());
        CHECK(calc.count() == 1);
        CHECK(calc.median() == Approx(100.0));
        CHECK(calc.is_changed()); // первое добавление всегда меняет медиану
    }

    SECTION("два значения — среднее арифметическое") {
        calculator calc;
        calc.add(100.0);
        calc.add(102.0);

        CHECK(calc.median() == Approx(101.0));
        CHECK(calc.is_changed());
    }

    SECTION("три значения — среднее") {
        calculator calc;
        calc.add(100.0);
        calc.add(102.0);
        calc.add(99.0);

        // [99, 100, 102] → медиана 100
        CHECK(calc.median() == Approx(100.0));
        CHECK(calc.is_changed());
    }
}

TEST_CASE("median — case 1", "[median]") {
    calculator calc;

    // Шаг 1: price=68480.10, медиана=68480.10
    calc.add(68480.10);
    CHECK(calc.median() == Approx(68480.10));
    CHECK(calc.is_changed());

    // Шаг 2: price=68480.00, медиана=68480.05
    calc.add(68480.00);
    CHECK(calc.median() == Approx(68480.05));
    CHECK(calc.is_changed());

    // Шаг 3: price=68480.10, медиана=68480.10
    calc.add(68480.10);
    CHECK(calc.median() == Approx(68480.10));
    CHECK(calc.is_changed());

    // Шаг 4: price=68480.10, медиана НЕ изменилась
    calc.add(68480.10);
    CHECK(calc.median() == Approx(68480.10));
    CHECK_FALSE(calc.is_changed());
}

TEST_CASE("median — case 2", "[median]") {
    calculator calc;

    // receive_ts=1000, price=100.0 → медиана 100.0
    calc.add(100.0);
    CHECK(calc.median() == Approx(100.0));
    CHECK(calc.count() == 1);

    // receive_ts=2000, price=101.0 → [100, 101] → 100.5
    calc.add(101.0);
    CHECK(calc.median() == Approx(100.5));

    // receive_ts=2000, price=102.0 → [100, 101, 102] → 101.0
    calc.add(102.0);
    CHECK(calc.median() == Approx(101.0));

    // receive_ts=3000, price=103.0 → [100, 101, 102, 103] → 101.5
    calc.add(103.0);
    CHECK(calc.median() == Approx(101.5));
}

TEST_CASE("median — is_changed корректно сбрасывается", "[median]") {
    calculator calc;

    calc.add(5.0);
    CHECK(calc.is_changed());

    calc.add(5.0); // медиана не изменится: [5, 5] → 5.0
    CHECK_FALSE(calc.is_changed());

    calc.add(5.0); // [5, 5, 5] → 5.0, не изменилась
    CHECK_FALSE(calc.is_changed());

    calc.add(10.0); // [5, 5, 5, 10] → (5+5)/2 = 5.0, не изменилась
    CHECK_FALSE(calc.is_changed());

    calc.add(10.0); // [5, 5, 5, 10, 10] → 5.0, не изменилась
    CHECK_FALSE(calc.is_changed());

    calc.add(10.0); // [5, 5, 5, 10, 10, 10] → (5+10)/2 = 7.5, изменилась
    CHECK(calc.is_changed());
    CHECK(calc.median() == Approx(7.5));
}
