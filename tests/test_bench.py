"""Tests for the grading function (pure) and bench error mapping."""

from swarm.proxies.bench import grade


def test_grade_fast_proxy_scores_high():
    # 3 MB/s, 200ms -> speed_norm=0.6, lat_norm high
    score = grade(200.0, 3000.0)
    assert score > 60


def test_grade_slow_proxy_scores_zero():
    assert grade(100.0, 100.0) == 0.0          # below min throughput


def test_grade_no_speed_falls_back_to_latency():
    # latency excellent, no throughput measurement -> score 0 (can't confirm MEGA work)
    assert grade(50.0, None) == 0.0


def test_grade_terrible_latency_capped():
    assert grade(4000.0, 5000.0) > 0           # slow but usable → still passes
    score = grade(10000.0, 5000.0)
    assert score < grade(1000.0, 5000.0)       # worse latency → worse score
