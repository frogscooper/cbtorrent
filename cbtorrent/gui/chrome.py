"""Shared tkinter chrome for the desktop GUI."""
from ..policy import (AdaptivePolicy, BanditPolicy, OptimisticPolicy, RecoveryPolicy,
                      ThroughputPolicy, TimeBudgetPolicy)

BG = "#1e1e1e"
PANEL = "#2a2a2a"
TEXT = "#e8e8e8"
ACCENT = "#4a9eff"
MUTED = "#888888"

POLICIES = {
    "heuristic": ThroughputPolicy,
    "bandit": BanditPolicy,
    "adaptive": AdaptivePolicy,
    "optimistic": OptimisticPolicy,
    "recovery": RecoveryPolicy,
    "timed": TimeBudgetPolicy,
}
