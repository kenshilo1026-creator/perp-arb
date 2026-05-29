from __future__ import annotations


class ExecutionStateMachine:
    def __init__(self) -> None:
        self.state = "idle"

    def to_preview_ready(self) -> None:
        self.state = "preview_ready"

    def to_awaiting_confirm(self) -> None:
        self.state = "awaiting_confirm"

    def to_placing_maker_leg(self) -> None:
        self.state = "placing_maker_leg"

    def to_hedging_taker_leg(self) -> None:
        self.state = "hedging_taker_leg"

    def to_retrying_hedge(self) -> None:
        self.state = "retrying_hedge"

    def to_paused_risk(self) -> None:
        self.state = "paused_risk"

    def to_emergency_exit(self) -> None:
        self.state = "emergency_exit"

    def to_completed(self) -> None:
        self.state = "completed"
