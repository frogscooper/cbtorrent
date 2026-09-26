import unittest

from cbtorrent.benchmark import paired_comparisons, scenarios_for
from cbtorrent.policy import ActiveTransfer, AdaptivePolicy, SchedulingContext, ServiceModel


class ModelTests(unittest.TestCase):
    def test_learns_duration_scaled_by_bytes(self):
        model = ServiceModel()
        self.assertIsNone(model.predict(16384))
        model.update(32768, 0.2)
        model.update(16384, 0.1)
        self.assertAlmostEqual(model.predict(65536), 0.4)
        self.assertAlmostEqual(model.relative_error(), 0)

    def test_forgetting_adapts_to_changed_rate(self):
        adaptive, lifetime = ServiceModel(0.8), ServiceModel(1.0)
        for model in (adaptive, lifetime):
            for _ in range(20):
                model.update(32768, 0.05)
            for _ in range(8):
                model.update(32768, 0.3)
        self.assertLess(abs(adaptive.predict(32768) - 0.3), abs(lifetime.predict(32768) - 0.3))
        self.assertGreater(adaptive.relative_error(), 0)

    def test_rejects_invalid_training_samples(self):
        model = ServiceModel()
        for size, seconds in ((0, 1), (-1, 1), (1, 0), (1, -1), (1, float("nan")), (1, float("inf"))):
            with self.assertRaises(ValueError):
                model.update(size, seconds)
        self.assertEqual(model.samples, 0)


class PlannerTests(unittest.TestCase):
    def setUp(self):
        self.slow, self.fast, self.new = ("slow", 1), ("fast", 2), ("new", 3)
        self.policy = AdaptivePolicy()
        for _ in range(3):
            self.policy.observe(self.slow, 32768, 0.2)
            self.policy.observe(self.fast, 32768, 0.02)

    def context(self, *, now=1, available=True, count=1, active=True):
        return SchedulingContext(now, count, {3: 32768},
                                 {self.slow: frozenset({3}), self.fast: frozenset({3} if available else {})},
                                 {self.fast: ActiveTransfer(2, 32768, 16384, 1)} if active else {}, 2)

    def test_waits_when_busy_fast_peer_can_finish_tail_sooner(self):
        self.assertIsNone(self.policy.choose_with_context([self.slow], {}, self.context()))

    def test_never_waits_for_peer_missing_the_piece(self):
        self.assertEqual(self.policy.choose_with_context([self.slow], {}, self.context(available=False)), self.slow)

    def test_stalled_fast_peer_cannot_block_fallback(self):
        self.assertEqual(self.policy.choose_with_context([self.slow], {}, self.context(now=2)), self.slow)

    def test_no_wait_without_inflight_work(self):
        self.assertEqual(self.policy.choose_with_context([self.slow], {}, self.context(active=False)), self.slow)

    def test_exploration_depends_on_work_remaining(self):
        self.assertEqual(self.policy.choose_with_context([self.fast, self.new], {}, self.context(count=20)), self.new)
        self.assertEqual(self.policy.choose_with_context([self.fast, self.new], {}, self.context(active=False)), self.fast)

    def test_new_peers_always_make_progress_when_no_known_candidate(self):
        self.assertEqual(self.policy.choose_with_context([self.new], {}, self.context()), self.new)

    def test_no_defer_ablation_uses_same_model(self):
        self.policy.defer = False
        self.assertEqual(self.policy.choose_with_context([self.slow], {}, self.context()), self.slow)

    def test_online_model_can_reverse_an_old_preference(self):
        self.assertEqual(self.policy.choose([self.slow, self.fast], {}), self.fast)
        for _ in range(12):
            self.policy.observe(self.fast, 32768, 0.5)
        self.assertEqual(self.policy.choose([self.slow, self.fast], {}), self.slow)


class EvaluationTests(unittest.TestCase):
    def test_failure_pairs_are_reported_not_discarded_silently(self):
        def row(trial, policy, complete, duration):
            return dict(scenario="test", trial=trial, policy=policy, metrics=dict(
                complete=complete, completion_seconds=duration, protocol_overhead_bytes=100,
                wasted_payload_bytes=0, cpu_seconds=0.01))
        rows = [row(0, "heuristic", True, 2), row(0, "adaptive", True, 1),
                row(1, "heuristic", True, 2), row(1, "adaptive", False, None)]
        result = paired_comparisons(rows, ("heuristic", "adaptive"))[0]
        self.assertEqual(result["paired_successes"], 1)
        self.assertEqual(result["pairs_with_failure"], 1)
        self.assertEqual(result["mean_completion_speedup"], 0.5)
        self.assertIsNone(result["bootstrap_95_percent_interval"])

    def test_suites_are_distinct_and_validation_varies_piece_size(self):
        development = scenarios_for("development", 1024 * 1024)
        validation = scenarios_for("validation", 1024 * 1024)
        self.assertFalse(development.keys() & validation.keys())
        self.assertGreater(len({s["piece_length"] for s in validation.values()}), 1)
