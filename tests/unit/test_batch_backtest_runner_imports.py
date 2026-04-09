import json

from scripts.batch_backtest_runner import (
    导入BLK到标的池,
    导入公式包到策略池,
    读取CSV,
    标的池列定义,
    标的池英文别名,
    策略池列定义,
    策略池英文别名,
)


def test_blk_import_to_stock_pool(tmp_path):
    blk = tmp_path / "a.blk"
    blk.write_text("600000 测试\n000001|平安\n", encoding="gbk")
    stock_pool = tmp_path / "stocks.csv"
    total, added, updated = 导入BLK到标的池(
        标的池路径=stock_pool,
        blk_path=blk,
        blk_encoding="auto",
        导入模式="append",
        market_tag="主板",
        industry_tag="导入",
        size_tag="未知",
        enabled="1",
    )
    rows = 读取CSV(stock_pool, 标的池列定义, 标的池英文别名)
    assert total == 2
    assert added == 2
    assert updated == 0
    assert {x["stock_code"] for x in rows} == {"600000.SH", "000001.SZ"}


def test_formula_pack_import_to_strategy_pool(tmp_path):
    pack = tmp_path / "pack.json"
    pack.write_text(json.dumps(["S1", {"strategy_id": "S2"}, "S1"], ensure_ascii=False), encoding="utf-8")
    strategy_pool = tmp_path / "strategies.csv"
    total, added, updated = 导入公式包到策略池(
        策略池路径=strategy_pool,
        公式包路径=pack,
        导入模式="append",
        enabled="1",
    )
    rows = 读取CSV(strategy_pool, 策略池列定义, 策略池英文别名)
    assert total == 2
    assert added == 2
    assert updated == 0
    assert {x["strategy_id"] for x in rows} == {"S1", "S2"}
