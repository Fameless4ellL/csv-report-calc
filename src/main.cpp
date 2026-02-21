/**
 * \file main.cpp
 */

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <format>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "parser.hpp"
#include "reader.hpp"
#include "median.hpp"
#include "writer.hpp"
#include "pool.hpp"

namespace {
    inline constexpr std::size_t k_min_threads = 1;
    inline constexpr std::size_t k_log_max_size = 10 * 1024 * 1024; // 10 MB
    inline constexpr std::size_t k_log_max_files = 3;
    volatile std::sig_atomic_t g_shutdown{ 0 };

    void setup_logger() noexcept {
        try {
            auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                "logs/app.log", k_log_max_size, k_log_max_files
            );

            auto logger = std::make_shared<spdlog::logger>(
                "main",
                spdlog::sinks_init_list{ console_sink, file_sink }
            );

            spdlog::set_default_logger(logger);
            spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
        }
        catch (const std::exception& e) {
            spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
            spdlog::warn("can't make log system: {}", e.what());
        }
    }

    void setup_signals() noexcept {
        // Graceful shutdown
        std::signal(SIGINT, [](int) { g_shutdown = 1; });
        std::signal(SIGTERM, [](int) { g_shutdown = 1; });
    }

}

int main(int argc, const char* argv[]) noexcept {
    setup_logger();
    setup_signals();

    spdlog::info("Start");

    // ── 1. Конфигурация ──────────────────────────────
    csv_median::config_parser parser;
    auto [config, config_err] = parser.parse(argc, argv);

    if (config_err) {
        spdlog::error("cfg error: {}", config_err.message());
        return EXIT_FAILURE;
    }

    spdlog::info("input dir:  {}", config.input_dir.string());
    spdlog::info("output dir: {}", config.output_dir.string());

    const std::size_t thread_count = std::max(
        k_min_threads,
        static_cast<std::size_t>(std::thread::hardware_concurrency())
    );

    csv_median::thread_pool pool{ thread_count };
    spdlog::info("Thread pool: {}", pool.thread_count());

    csv_median::result_writer writer;
    if (const auto err = writer.open(config.output_dir)) {
        spdlog::error("Ошибка открытия выходного файла: {}", err.message());
        return EXIT_FAILURE;
    }

    csv_median::calculator calc;
    csv_median::csv_reader reader{ pool };
    std::size_t written = 0;

    const auto on_record = [&](const csv_median::csv_record& rec) {
        if (g_shutdown) [[unlikely]] {
            return;
        }

        calc.add(rec.price);

        if (calc.is_changed()) {
            if (const auto err = writer.write(rec.receive_ts, calc.median())) {
                spdlog::error("error writer: {}", err.message());
                g_shutdown = 1;
                return;
            }
            ++written;
        }
        };

    const auto process_err = reader.process(
        config.input_dir,
        config.filename_masks,
        on_record
    );

    if (process_err) {
        spdlog::error("error during work: {}", process_err.message());
        return EXIT_FAILURE;
    }

    if (g_shutdown) {
        spdlog::warn("stopped by system signal");
    }

    // ── 5. Итоги ─────────────────────────────────────
    spdlog::info("median: {}", written);
    spdlog::info("records: {}",
        (config.output_dir / "median_result.csv").string());
    spdlog::info("closing");

    return g_shutdown ? EXIT_FAILURE : EXIT_SUCCESS;
}