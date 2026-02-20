/**
 * \file main.cpp
 * \brief Точка входа: оркестрирует чтение, расчёт и запись результатов
 */

#include <cstdlib>
#include <format>

#include <spdlog/spdlog.h>

#include "parser.hpp"
#include "reader.hpp"
#include "median.hpp"
#include "writer.hpp"

int main(int argc, const char* argv[]) noexcept {
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    spdlog::info("Start");

    // ── 1. Конфигурация ──────────────────────────────
    csv_median::config_parser cfg_parser;
    auto [config, config_err] = cfg_parser.parse(argc, argv);

    if (config_err) {
        spdlog::error("cfg error: {}", config_err.message());
        return EXIT_FAILURE;
    }

    spdlog::info("input dir:  {}", config.input_dir.string());
    spdlog::info("output dir: {}", config.output_dir.string());

    csv_median::csv_reader reader;
    auto [records, read_err] = reader.load(config.input_dir,
                                           config.filename_masks);

    if (read_err) {
        spdlog::error("read err: {}", read_err.message());
        return EXIT_FAILURE;
    }

    if (records.empty()) {
        spdlog::warn("empty data");
        return EXIT_SUCCESS;
    }

    csv_median::result_writer writer;
    if (const auto err = writer.open(config.output_dir)) {
        spdlog::error("err open file from output dir: {}", err.message());
        return EXIT_FAILURE;
    }

    csv_median::calculator calc;

    for (const auto& rec : records) {
        calc.add(rec.price);

        if (calc.is_changed()) {
            if (const auto err = writer.write(rec.receive_ts, calc.median())) {
                spdlog::error("error write: {}", err.message());
                return EXIT_FAILURE;
            }
        }
    }

    // ── 5. Итоги ─────────────────────────────────────
    spdlog::info("records size:         {}", records.size());
    spdlog::info("how many current median is changed: {}", writer.written_count());
    spdlog::info("saved on: {}",
                 (config.output_dir / "median_result.csv").string());
    spdlog::info("closing");

    return EXIT_SUCCESS;
}
