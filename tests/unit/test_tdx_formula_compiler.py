from src.tdx.formula_compiler import compile_tdx_formula


def test_compile_tdx_formula_generates_strategy_code():
    formula = """
    MA5:=MA(C,5);
    MA10:=MA(C,10);
    CROSS(MA5,MA10)
    """
    payload = compile_tdx_formula(
        formula_text=formula,
        strategy_id="T001",
        strategy_name="TDX_MA_CROSS",
        kline_type="1min",
    )
    code_text = str(payload.get("code", ""))
    assert payload["strategy_id"] == "T001"
    assert payload["class_name"].startswith("GeneratedTdx")
    assert payload["warmup_bars"] >= 60
    assert "Indicators.ma" in code_text
    assert "self._CROSS" in code_text
    assert "class " in code_text


def test_compile_tdx_formula_supports_if_and_ref():
    formula = """
    X:=REF(C,1);
    Y:=IF(C>X,1,0);
    Y=1
    """
    payload = compile_tdx_formula(
        formula_text=formula,
        strategy_id="T002",
        strategy_name="TDX_IF_REF",
        kline_type="D",
    )
    code_text = str(payload.get("code", ""))
    assert "self._REF" in code_text
    assert "self._IF" in code_text
    assert payload["used_functions"]
