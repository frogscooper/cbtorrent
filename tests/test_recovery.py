import random
import unittest

from cbtorrent.benchmark import paired_comparisons
from cbtorrent.policy import AdaptivePolicy, EWMAModel, RecoveryPolicy, ResponsiveModel, SchedulingContext


class ResponsiveModelTests(unittest.TestCase):
    def trained(self):
        model = ResponsiveModel()
        for _ in range(30):
            model.update(32768, 0.2)
        return model

    def test_two_consistent_changes_reset_in_both_directions(self):
        for duration in (0.02, 1.0):
            with self.subTest(duration=duration):
                model = self.trained()
                model.update(32768, duration)
                self.assertEqual(model.resets, 0)
                model.update(16384, duration / 2)
                self.assertEqual(model.resets, 1)
                self.assertEqual(model.samples, 32)
                self.assertAlmostEqual(model.predict(32768), duration)
                self.assertAlmostEqual(model.relative_error(), 0)

    def test_isolated_outlier_does_not_reset(self):
        model = self.trained()
        model.update(32768, 0.6)
        model.update(32768, 0.2)
        self.assertEqual(model.resets, 0)
        self.assertIsNone(model.pending)

    def test_opposite_errors_do_not_confirm_change(self):
        model = self.trained()
        model.update(32768, 0.6)
        model.update(32768, 0.02)
        self.assertEqual(model.resets, 0)

    def test_jitter_and_varied_sizes_remain_finite_and_do_not_reset(self):
        rng = random.Random(702)
        model = ResponsiveModel()
        for _ in range(10000):
            size = rng.choice((1, 16384, 32768, 65536))
            model.update(size, size / 16384 * rng.uniform(0.08, 0.12))
            self.assertGreater(model.predict(32768), 0)
            self.assertLessEqual(model.relative_error(), 1)
        self.assertEqual(model.resets, 0)
        self.assertEqual(model.samples, 10000)

    def test_invalid_update_cannot_change_pending_evidence(self):
        for factory in (ResponsiveModel, EWMAModel):
            model = factory()
            model.update(32768, 0.1)
            before = vars(model).copy()
            for size, seconds in ((0, 1), (-1, 1), (1, 0), (1, -1),
                                  (1, float("nan")), (1, float("inf"))):
                with self.assertRaises(ValueError):
                    model.update(size, seconds)
                self.assertEqual(vars(model), before)


class RecoveryPolicyTests(unittest.TestCase):
    fast, slow, third = ("fast", 1), ("slow", 2), ("third", 3)

    def prepared(self, **kwargs):
        policy = RecoveryPolicy(**kwargs)
        policy.observe(self.slow, 32768, 0.08)
        for _ in range(32):
            policy.observe(self.fast, 32768, 0.04)
        return policy

    def context(self, *, count=100, cached=True, size=32768):
        return SchedulingContext(10, count, {},
                                 {self.slow: frozenset(), self.fast: frozenset()} if cached else {},
                                 {}, 2, size)

    def test_revisits_stale_cached_peer(self):
        policy = self.prepared()
        self.assertEqual(policy.choose_with_context([self.fast, self.slow], {}, self.context()), self.slow)
        self.assertEqual(policy.diagnostics()["probes"], 1)
        self.assertEqual(policy.probe_bytes, 32768)

    def test_reservation_prevents_repeated_probe_without_observation(self):
        policy = self.prepared()
        policy.choose_with_context([self.fast, self.slow], {}, self.context())
        for _ in range(100):
            self.assertEqual(policy.choose_with_context([self.fast, self.slow], {}, self.context()), self.fast)
        self.assertEqual(policy.probes, 1)

    def test_probe_restrictions(self):
        for reason in ("tail", "evicted", "expensive", "budget", "disabled", "recent"):
            with self.subTest(reason=reason):
                policy = self.prepared(probe=reason != "disabled")
                context = self.context(count=8 if reason == "tail" else 100, cached=reason != "evicted")
                if reason == "expensive":
                    policy.models[self.slow] = ResponsiveModel()
                    policy.models[self.slow].update(32768, 10)
                if reason == "budget":
                    policy.probe_bytes = policy.verified_bytes / 16
                if reason == "recent":
                    policy.observe(self.slow, 32768, 0.08)
                self.assertEqual(policy.choose_with_context([self.fast, self.slow], {}, context), self.fast)
                self.assertEqual(policy.probes, 0)

    def test_byte_budget_and_eligibility_under_many_adversarial_decisions(self):
        rng = random.Random(309)
        policy = RecoveryPolicy()
        peers = [("peer", i) for i in range(200)]
        for peer in peers:
            policy.observe(peer, 16384, rng.uniform(0.01, 1))
        for _ in range(2000):
            eligible = rng.sample(peers, 8)
            size = rng.choice((16384, 32768, 65536))
            context = SchedulingContext(0, 1000, {}, {p: frozenset() for p in eligible}, {}, 4, size)
            choice = policy.choose_with_context(eligible, {}, context)
            self.assertIn(choice, eligible)
            self.assertLessEqual(policy.probe_bytes, policy.verified_bytes / 16)
            policy.observe(choice, size, rng.uniform(0.01, 1))
        self.assertLessEqual(len(policy.models), 200)
        self.assertGreater(policy.probes, 0)

    def test_simple_baseline_has_identical_probe_rules(self):
        for factory in (ResponsiveModel, EWMAModel):
            policy = self.prepared(model_factory=factory)
            self.assertEqual(policy.choose_with_context([self.fast, self.slow], {}, self.context()), self.slow)
            self.assertEqual(policy.probe_bytes, 32768)

    def test_unchanged_probe_backs_off_and_changed_probe_shortens_interval(self):
        policy = self.prepared()
        peers = [self.fast, self.slow]
        policy.choose_with_context(peers, {}, self.context())
        policy.observe(self.slow, 32768, 0.08)
        self.assertEqual(policy.probe_intervals[self.slow], 32)
        for _ in range(16):
            policy.observe(self.fast, 32768, 0.04)
        self.assertEqual(policy.choose_with_context(peers, {}, self.context()), self.fast)
        for _ in range(16):
            policy.observe(self.fast, 32768, 0.04)
        self.assertEqual(policy.choose_with_context(peers, {}, self.context()), self.slow)
        policy.observe(self.slow, 32768, 0.01)
        self.assertEqual(policy.probe_intervals[self.slow], 1)
        # Confirmation probe eligible after one intervening observe.
        policy.observe(self.fast, 32768, 0.04)
        self.assertEqual(policy.choose_with_context(peers, {}, self.context()), self.slow)

    def test_invalid_sample_does_not_spend_budget_or_advance_epoch(self):
        policy = self.prepared()
        before = policy.diagnostics(), policy.epoch, policy.last_seen.copy()
        with self.assertRaises(ValueError):
            policy.observe(self.fast, 32768, float("nan"))
        self.assertEqual((policy.diagnostics(), policy.epoch, policy.last_seen), before)

    def test_full_feedback_loop_discovers_recovery_without_future_information(self):
        durations = {}
        for name, policy in (("old", AdaptivePolicy(defer=False)), ("new", RecoveryPolicy())):
            policy.observe(self.slow, 32768, 0.08)
            policy.observe(self.fast, 32768, 0.02)
            total = 0
            recovered_pieces = 0
            for index in range(192):
                context = SchedulingContext(total, 192 - index, {},
                                            {self.fast: frozenset(), self.slow: frozenset()}, {}, 1, 32768)
                peer = policy.choose_with_context([self.fast, self.slow], {}, context)
                # The environment exposes only the duration of the chosen piece.
                # Its future rate schedule is never passed to the policy.
                seconds = 0.02 if peer == self.fast else (0.08 if index < 24 else 0.004)
                policy.observe(peer, 32768, seconds)
                total += seconds
                recovered_pieces += peer == self.slow and index >= 24
            durations[name] = total
            if name == "new":
                self.assertGreater(recovered_pieces, 80)
                self.assertLessEqual(policy.probe_bytes, policy.verified_bytes / 16)
                self.assertGreater(policy.diagnostics()["model_resets"], 0)
        self.assertLess(durations["new"], durations["old"] * 0.8)


class PairingTests(unittest.TestCase):
    def row(self, trial, policy):
        return dict(scenario="case", trial=trial, policy=policy, metrics=dict(
            complete=True, completion_seconds=1, protocol_overhead_bytes=1,
            wasted_payload_bytes=0, cpu_seconds=0))

    def test_missing_pairs_are_visible(self):
        rows = [self.row(0, "adaptive"), self.row(0, "recovery"), self.row(1, "adaptive")]
        result = paired_comparisons(rows, ("recovery",), baseline="adaptive")[0]
        self.assertEqual(result["pairs_missing"], 1)
        self.assertEqual(result["paired_successes"], 1)

    def test_duplicate_pairs_are_rejected(self):
        row = self.row(0, "adaptive")
        with self.assertRaises(ValueError):
            paired_comparisons([row, row], ("recovery",))
