import random
import unittest

from cbtorrent.policy import RecoveryPolicy, SchedulingContext, TimeBudgetPolicy


class TimeBudgetTests(unittest.TestCase):
    fast, slow, third = ("fast", 1), ("slow", 2), ("third", 3)

    def prepared(self, fraction=0.02):
        policy = TimeBudgetPolicy(time_fraction=fraction)
        policy.observe(self.slow, 32768, 0.08)
        policy.observe(self.third, 32768, 0.08)
        for _ in range(32):
            policy.observe(self.fast, 32768, 0.04)
        return policy

    def context(self, count=100):
        return SchedulingContext(0, count, {}, {self.slow: frozenset(), self.third: frozenset()}, {}, 2, 32768)

    def test_time_gate_rejects_a_byte_affordable_probe(self):
        policy = self.prepared(0.005)
        peers = [self.fast, self.slow]
        self.assertEqual(policy.choose_with_context(peers, {}, self.context()), self.fast)
        self.assertEqual(policy.probes, 0)
        self.assertEqual(policy.probe_bytes, 0)
        self.assertEqual(policy.budget_denials, 1)
        self.assertNotIn(self.slow, policy.pending_probes)

    def test_concurrent_reservations_cannot_spend_the_same_credit(self):
        policy = self.prepared()
        self.assertEqual(policy.choose_with_context([self.fast, self.slow], {}, self.context()), self.slow)
        self.assertEqual(policy.choose_with_context([self.fast, self.third], {}, self.context()), self.fast)
        self.assertEqual(len(policy.time_reservations), 1)
        self.assertLessEqual(policy.max_admission_fraction, policy.time_fraction)

    def test_quick_probe_releases_time_but_not_byte_reservation(self):
        policy = self.prepared()
        policy.choose_with_context([self.fast, self.slow], {}, self.context())
        policy.observe(self.slow, 32768, 0.01)
        policy.attempt_finished(self.slow, 0.015)
        self.assertEqual(policy.extra_seconds, 0)
        self.assertEqual(policy.time_reservations, {})
        self.assertEqual(policy.probe_bytes, 32768)
        self.assertEqual(policy.choose_with_context([self.fast, self.third], {}, self.context()), self.third)

    def test_failed_or_slow_probe_spends_time_without_training(self):
        policy = self.prepared()
        policy.choose_with_context([self.fast, self.slow], {}, self.context())
        before = policy.epoch, policy.verified_bytes, policy.models[self.slow].samples
        policy.attempt_finished(self.slow, 5)
        self.assertAlmostEqual(policy.extra_seconds, 4.96)
        self.assertEqual((policy.epoch, policy.verified_bytes, policy.models[self.slow].samples), before)
        self.assertNotIn(self.slow, policy.pending_probes)
        self.assertEqual(policy.time_reservations, {})
        self.assertEqual(policy.choose_with_context([self.fast, self.third], {}, self.context()), self.fast)
        policy.attempt_finished(self.slow, 5)  # Duplicate cleanup is harmless.
        self.assertAlmostEqual(policy.extra_seconds, 4.96)

    def test_zero_fraction_disables_revisits_but_not_initial_discovery(self):
        policy = self.prepared(0)
        self.assertEqual(policy.choose_with_context([self.fast, self.slow], {}, self.context()), self.fast)
        new = ("new", 4)
        self.assertEqual(policy.choose_with_context([self.fast, new], {}, self.context()), new)

    def test_finished_probe_uses_the_alternative_predicted_when_it_started(self):
        policy = self.prepared()
        policy.choose_with_context([self.fast, self.slow], {}, self.context())
        for _ in range(3):
            policy.observe(self.fast, 32768, 0.4)
        policy.attempt_finished(self.slow, 0.08)
        self.assertAlmostEqual(policy.extra_seconds, 0.04)

    def test_invalid_values_do_not_mutate_ledger(self):
        for fraction in (-1, 1.1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                TimeBudgetPolicy(time_fraction=fraction)
        policy = self.prepared()
        policy.choose_with_context([self.fast, self.slow], {}, self.context())
        before = policy.diagnostics(), policy.time_reservations.copy()
        for value in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                policy.attempt_finished(self.slow, value)
            self.assertEqual((policy.diagnostics(), policy.time_reservations), before)

    def test_admission_bound_with_variable_sizes_concurrency_and_failed_attempts(self):
        rng = random.Random(8109)
        policy = TimeBudgetPolicy()
        peers = [("peer", i) for i in range(200)]
        for peer in peers:
            policy.observe(peer, 32768, rng.uniform(0.03, 0.1))
        in_flight = []
        for _ in range(3000):
            busy = {p for p, _, _ in in_flight}
            candidates = rng.sample([p for p in peers if p not in busy], 8)
            size = rng.choice((16384, 32768, 65536))
            context = SchedulingContext(0, rng.randint(10, 1000), {},
                                        {p: frozenset() for p in candidates}, {}, 4, size)
            choice = policy.choose_with_context(candidates, {}, context)
            self.assertIn(choice, candidates)
            in_flight.append((choice, size, rng.uniform(0.01, 0.15)))
            self.assertLessEqual(policy.probe_bytes, policy.verified_bytes / 16)
            self.assertLessEqual(policy.max_admission_fraction, policy.time_fraction + 1e-12)
            if len(in_flight) >= 4:
                peer, size, elapsed = in_flight.pop(rng.randrange(len(in_flight)))
                if rng.random() > 0.1:
                    policy.observe(peer, size, elapsed)
                policy.attempt_finished(peer, elapsed)
        for peer, _, elapsed in in_flight:
            policy.attempt_finished(peer, elapsed)
        self.assertEqual(policy.time_reservations, {})
        self.assertGreater(policy.probes, 0)
        self.assertGreater(policy.budget_denials, 0)

    def test_feedback_retains_recovery_and_reduces_stable_peer_exploration(self):
        for recovers in (True, False):
            # Affordable recovery and an expensive permanently slow peer are
            # separate cases; a tight budget cannot discover every recovery.
            slow_seconds = 0.04 if recovers else 0.08
            totals = {}
            for factory in (RecoveryPolicy, TimeBudgetPolicy):
                policy = factory()
                policy.observe(self.slow, 32768, slow_seconds)
                policy.observe(self.fast, 32768, 0.02)
                total = 0
                for index in range(192):
                    context = SchedulingContext(total, 192 - index, {},
                                                {self.fast: frozenset(), self.slow: frozenset()}, {}, 1, 32768)
                    peer = policy.choose_with_context([self.fast, self.slow], {}, context)
                    elapsed = 0.02 if peer == self.fast else (0.004 if recovers and index >= 24 else slow_seconds)
                    policy.observe(peer, 32768, elapsed)
                    if hasattr(policy, "attempt_finished"):
                        policy.attempt_finished(peer, elapsed)
                    total += elapsed
                totals[factory.__name__] = total
            if recovers:
                self.assertLess(totals["TimeBudgetPolicy"], 192 * 0.02 * 0.8)
            else:
                self.assertLess(totals["TimeBudgetPolicy"], totals["RecoveryPolicy"])
