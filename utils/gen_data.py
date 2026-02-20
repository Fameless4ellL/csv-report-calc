#!/usr/bin/env python3

import random
import sys
import os
from pathlib import Path

TARGET_MB       = 100
TRADE_SHARE     = 0.5  
BASE_PRICE      = 68_000.0
PRICE_VOLATILITY = 0.0005
START_TS        = 1_716_810_808_000_000
TS_STEP_MIN     = 100
TS_STEP_MAX     = 5_000

OUTPUT_DIR = Path(__file__).parent / "input"


def generate_trade(output_path: Path, target_bytes: int) -> int:
    """Генерирует файл торговых сделок."""
    sides      = ["bid", "ask"]
    written    = 0
    ts         = START_TS
    price      = BASE_PRICE

    print(f"Генерация {output_path.name}...")

    with open(output_path, "w", buffering=1 << 20) as f:
        f.write("receive_ts;exchange_ts;price;quantity;side\n")

        while written < target_bytes:
            price *= 1.0 + random.uniform(-PRICE_VOLATILITY, PRICE_VOLATILITY)
            price  = round(price, 8)

            ts         += random.randint(TS_STEP_MIN, TS_STEP_MAX)
            exchange_ts = ts - random.randint(500, 3_000)

            quantity = round(random.uniform(0.001, 5.0), 8)
            side     = random.choice(sides)

            line = f"{ts};{exchange_ts};{price:.8f};{quantity:.8f};{side}\n"
            f.write(line)
            written += len(line)

    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"  → {output_path.name}: {size_mb:.1f} MB")
    return written


def generate_level(output_path: Path, target_bytes: int) -> int:
    """Генерирует файл стакана заявок (order book levels)."""
    sides   = ["bid", "ask"]
    written = 0
    ts      = START_TS + random.randint(0, 10_000)
    price   = BASE_PRICE

    print(f"Генерация {output_path.name}...")

    with open(output_path, "w", buffering=1 << 20) as f:
        f.write("receive_ts;exchange_ts;price;quantity;side;rebuild\n")

        while written < target_bytes:
            price *= 1.0 + random.uniform(-PRICE_VOLATILITY, PRICE_VOLATILITY)
            price  = round(price, 8)

            # Для стакана несколько уровней на одну временную метку
            levels_count = random.randint(1, 5)
            ts          += random.randint(TS_STEP_MIN, TS_STEP_MAX)
            exchange_ts  = ts - random.randint(500, 3_000)

            for i in range(levels_count):
                # Цены уровней рядом с текущей ценой
                level_price = round(price + random.uniform(-50, 50), 8)
                quantity    = round(random.uniform(0.001, 20.0), 8)
                side        = random.choice(sides)
                rebuild     = 1 if i == 0 else 0  # первый уровень — перестройка

                line = (f"{ts};{exchange_ts};"
                        f"{level_price:.8f};{quantity:.8f};{side};{rebuild}\n")
                f.write(line)
                written += len(line)

    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"  → {output_path.name}: {size_mb:.1f} MB")
    return written


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    target_bytes = TARGET_MB * 1024 * 1024
    trade_bytes  = int(target_bytes * TRADE_SHARE)
    level_bytes  = target_bytes - trade_bytes

    trade_path = OUTPUT_DIR / "btcusdt_trade_2024.csv"
    level_path = OUTPUT_DIR / "btcusdt_level_2024.csv"

    generate_trade(trade_path, trade_bytes)
    generate_level(level_path, level_bytes)

    total_mb = sum(
        p.stat().st_size for p in [trade_path, level_path]
    ) / 1024 / 1024

    print(f"\nГотово! Итого: {total_mb:.1f} MB в {OUTPUT_DIR}")


if __name__ == "__main__":
    random.seed(42)  # воспроизводимость
    main()