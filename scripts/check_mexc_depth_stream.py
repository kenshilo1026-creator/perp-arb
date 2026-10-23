"""Validate public MEXC hedge depth without credentials or placing orders."""
import argparse
import asyncio

from hydra_basis.execution_engine.mexc_spot_adapter import MexcSpotExecutionAdapter


async def main(symbol):
    adapter = MexcSpotExecutionAdapter(api_key="", api_secret="")
    try:
        await adapter.prepare_hedge_quotes(symbol)
        versions = set()
        for _ in range(20):
            await adapter.wait_for_hedge_quote()
            book = await adapter.hedge_quote()
            versions.add(book["version"])
        print(f"PASS: {symbol} public depth decoded; {len(versions)} versions; "
              f"bids={len(book['bids'])} asks={len(book['asks'])} age_ms={book['age_ms']:.2f}")
        print("No credentials used; no orders submitted.")
    finally:
        await adapter.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbol")
    asyncio.run(main(parser.parse_args().symbol))
