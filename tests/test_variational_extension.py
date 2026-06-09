from pathlib import Path
import unittest


EXT_DIR = Path("browser_extensions/variational")


class VariationalExtensionCommandClientTests(unittest.TestCase):
    def test_manifest_allows_scripting_for_order_injection(self) -> None:
        manifest = (EXT_DIR / "manifest.json").read_text(encoding="utf-8")

        self.assertIn('"scripting"', manifest)
        self.assertIn("connect-src ws://127.0.0.1:*", manifest)

    def test_background_registers_extension_command_client(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn('commandEndpoint: "ws://127.0.0.1:8768"', background)
        self.assertIn('"type": "REGISTER"', background)
        self.assertIn('"role": "extension"', background)
        self.assertIn("executeVariationalOrder", background)
        self.assertIn('"type": "ORDER_RESULT"', background)

    def test_background_rejects_order_history_as_submit_button(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("ORDER_AUTOMATION_VERSION", background)
        self.assertIn("collectOrderDomDiagnostics", background)
        self.assertIn("collectVariationalPageDiagnostics", background)
        self.assertIn("Automation returned no result", background)
        self.assertIn("automationVersion: ORDER_AUTOMATION_VERSION", background)
        self.assertIn("clickableSelector", background)
        self.assertIn('button[data-testid="submit-button"]', background)
        self.assertIn("isRejectedTradeButtonText", background)
        self.assertIn("order history", background.lower())
        self.assertIn("findSubmitButton(side)", background)
        self.assertIn("automationVersion", background)

    def test_background_refuses_order_when_page_ticker_mismatches(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("currentVariationalSymbol", background)
        self.assertIn("normalizeVariationalSymbol", background)
        self.assertIn("Ticker mismatch", background)
        self.assertIn("requestedSymbol", background)
        self.assertIn("currentSymbol", background)

    def test_background_waits_for_enabled_submit_after_amount_input(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("waitForEnabledSubmitButton", background)
        self.assertIn("submitEnableTimeoutMs", background)
        self.assertIn("Submit button stayed disabled", background)

    def test_background_retries_injection_when_page_frame_is_removed(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("runVariationalOrderInjection", background)
        self.assertIn("isTransientFrameRemovalError", background)
        self.assertIn("Frame with ID", background)
        self.assertIn("await sleep(1000)", background)

    def test_background_supports_market_and_limit_order_modes(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("selectOrderType", background)
        self.assertIn("setLimitPriceOrClickMid", background)
        self.assertIn("findLimitPriceInput", background)
        self.assertIn("findLimitMidButton", background)
        self.assertIn("isLimitPriceInput", background)
        self.assertIn("findAmountInput(orderType", background)
        self.assertIn('input[data-testid="quantity-input"]', background)
        self.assertIn("limit-price-input", background)
        self.assertIn("excludedInput", background)
        self.assertIn('orderType === "LIMIT"', background)
        self.assertIn('"MARKET"', background)
        self.assertIn("explicitLimitPrice", background)
        self.assertIn("Mid", background)

    def test_background_supports_cancel_order_command(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("CANCEL_ORDER", background)
        self.assertIn("CANCEL_RESULT", background)
        self.assertIn("executeVariationalCancelOrder", background)
        self.assertIn("findCancelOrderButton", background)
        self.assertIn("cancelled", background.lower())

    def test_background_supports_limit_price_preview_without_submit(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("PREVIEW_LIMIT_ORDER_PRICE", background)
        self.assertIn("PRICE_PREVIEW_RESULT", background)
        self.assertIn("executeVariationalLimitPricePreview", background)
        self.assertIn("readLimitPriceValue", background)
        self.assertIn("previewOnly", background)

    def test_background_preview_uses_dedicated_limit_flow(self) -> None:
        background = (EXT_DIR / "background.js").read_text(encoding="utf-8")

        self.assertIn("findPreviewLimitOrderTypeButton", background)
        self.assertIn("waitForPreviewLimitPrice", background)
        self.assertIn("Could not switch Variational order form to Limit", background)
        self.assertNotIn(
            'return executeVariationalOrder({ ...command, orderType: "LIMIT", previewOnly: true });',
            background,
        )

    def test_popup_exposes_command_endpoint_status(self) -> None:
        popup_html = (EXT_DIR / "popup.html").read_text(encoding="utf-8")
        popup_js = (EXT_DIR / "popup.js").read_text(encoding="utf-8")

        self.assertIn("commandEndpoint", popup_html)
        self.assertIn("commandEndpoint", popup_js)
        self.assertIn("command", popup_js)


if __name__ == "__main__":
    unittest.main()
