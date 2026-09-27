import unittest

from cbtorrent.policy import (ActiveTransfer, AdaptivePolicy, OptimisticPolicy,
                              SchedulingContext, ServiceModel)


class OptimisticPolicyTests(unittest.TestCase):
    def setUp(self):
        self.stable = ("stable", 1)
        self.noisy = ("noisy", 2)
        self.slow = ("slow", 3)
        self.new = ("new", 4)

    def context(self, *, count=20, piece_size=16384, concurrency=2):
        return SchedulingContext(
            now=1.0, unclaimed_count=count, missing={0: piece_size},
            available={self.stable: frozenset({0}), self.noisy: frozenset({0}),
                       self.slow: frozenset({0}), self.new: frozenset({0})},
            active={}, concurrency=concurrency, piece_size=piece_size)

    def test_beta_bounds(self):
        OptimisticPolicy(beta=0)
        OptimisticPolicy(beta=0.5)
        for beta in (-0.01, 0.51, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                OptimisticPolicy(beta=beta)

    def test_default_beta_matches_model_prior(self):
        # ServiceModel / EWMAModel use 0.25 when residual is unavailable.
        self.assertEqual(OptimisticPolicy().beta, 0.25)
        self.assertEqual(ServiceModel().relative_error(), 0.25)
        self.assertFalse(OptimisticPolicy().defer)

    def test_rejects_invalid_training_samples(self):
        policy = OptimisticPolicy()
        for size, seconds in ((0, 1), (-1, 1), (1, 0), (1, -1),
                              (1, float("nan")), (1, float("inf"))):
            with self.assertRaises(ValueError):
                policy.observe(self.stable, size, seconds)
        self.assertEqual(policy.models, {})

    def test_equal_mean_high_relative_error_is_preferred(self):
        # forgetting=1 keeps OLS means exact for the alternating series.
        policy = OptimisticPolicy(beta=0.25, forgetting=1.0)
        for _ in range(8):
            policy.observe(self.stable, 16384, 0.1)
        for seconds in (0.02, 0.18, 0.02, 0.18, 0.02, 0.18, 0.02, 0.18):
            policy.observe(self.noisy, 16384, seconds)
        self.assertAlmostEqual(policy.models[self.stable].predict(16384),
                               policy.models[self.noisy].predict(16384), places=6)
        self.assertGreater(policy.models[self.noisy].relative_error(),
                           policy.models[self.stable].relative_error())
        self.assertLess(policy.score(self.noisy), policy.score(self.stable))
        self.assertEqual(policy.choose([self.stable, self.noisy], {}), self.noisy)

    def test_lower_residual_fast_peer_beats_slow_rival(self):
        policy = OptimisticPolicy(beta=0.25)
        for _ in range(6):
            policy.observe(self.stable, 16384, 0.05)
            policy.observe(self.slow, 16384, 0.4)
        self.assertLess(policy.score(self.stable), policy.score(self.slow))
        self.assertEqual(policy.choose([self.stable, self.slow], {}), self.stable)

    def test_beta_zero_matches_adaptive_min_predict(self):
        optimistic = OptimisticPolicy(beta=0)
        adaptive = AdaptivePolicy(defer=False)
        for policy in (optimistic, adaptive):
            for _ in range(4):
                policy.observe(self.stable, 16384, 0.05)
                policy.observe(self.noisy, 16384, 0.2)
        peers = [self.stable, self.noisy]
        self.assertEqual(optimistic.choose(peers, {}), adaptive.choose(peers, {}))
        ctx = self.context()
        self.assertEqual(optimistic.choose_with_context(peers, {}, ctx),
                         adaptive.choose_with_context(peers, {}, ctx))

    def test_unseen_exploration_matches_adaptive(self):
        optimistic = OptimisticPolicy()
        adaptive = AdaptivePolicy(defer=False)
        for policy in (optimistic, adaptive):
            for _ in range(3):
                policy.observe(self.stable, 16384, 0.05)
        peers = [self.stable, self.new]
        self.assertEqual(
            optimistic.choose_with_context(peers, {}, self.context(count=20)),
            adaptive.choose_with_context(peers, {}, self.context(count=20)))
        self.assertEqual(
            optimistic.choose_with_context(peers, {}, self.context(count=1)),
            adaptive.choose_with_context(peers, {}, self.context(count=1)))
        self.assertEqual(optimistic.choose(peers, {}), adaptive.choose(peers, {}))

    def test_adversarial_loop_does_not_crash(self):
        policy = OptimisticPolicy()
        peers = [(f"p{i}", i) for i in range(64)]
        for step in range(2000):
            peer = peers[step % len(peers)]
            size = 16384 * (1 + step % 4)
            seconds = 0.001 + (step % 17) * 0.01
            if step % 23 != 0:
                policy.observe(peer, size, seconds)
            chosen = policy.choose_with_context(
                peers, {}, SchedulingContext(
                    now=float(step), unclaimed_count=1 + step % 40,
                    missing={step % 8: size},
                    available={p: frozenset({step % 8}) for p in peers},
                    active={peers[0]: ActiveTransfer(0, size, size // 2, float(step))}
                    if step % 5 == 0 else {},
                    concurrency=2 + step % 3, piece_size=size))
            self.assertIn(chosen, peers)
        self.assertGreater(len(policy.models), 0)


if __name__ == "__main__":
    unittest.main()
