use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use statrs::distribution::{Beta, ContinuousCDF};
use std::time::Instant;

const CONFIDENCE_LEVEL: f64 = 90.0;
const ALPHA: f64 = 1.0 - CONFIDENCE_LEVEL / 100.0;
const LOWER_Q: f64 = ALPHA / 2.0; // e.g. 0.005 if alpha=0.01
const UPPER_Q: f64 = 1.0 - ALPHA / 2.0; // e.g. 0.995

/// Build a vector of N values from ~10^2 up to 10^7 (log scale).
fn build_n_values() -> Vec<usize> {
    let n_points = 100;
    let start_exp = 2.0; // 10^2 = 100
    let end_exp = 7.0; // 10^7 = 10,000,000
    let step = (end_exp - start_exp) / (n_points as f64 - 1.0);

    let mut values = Vec::with_capacity(n_points);
    for i in 0..n_points {
        let exponent = start_exp + step * (i as f64);
        let val = 10_f64.powf(exponent).round() as usize;
        values.push(val);
    }
    values
}

/// Build a p_values array from 0.01 to 0.99 (100 points).
fn build_p_values() -> Vec<f64> {
    let n_points = 100;
    let start_p = 0.01;
    let end_p = 0.99;
    let step = (end_p - start_p) / (n_points as f64 - 1.0);

    let mut values = Vec::with_capacity(n_points);
    for i in 0..n_points {
        values.push(start_p + step * (i as f64));
    }
    values
}

/// Precompute window sizes for every (N_values[i], p_values[j]) pair.
fn precompute_window_sizes(n_values: &[usize], p_values: &[f64]) -> Vec<Vec<usize>> {
    let mut table = vec![vec![0usize; p_values.len()]; n_values.len()];

    for (i, &n) in n_values.iter().enumerate() {
        for (j, &p) in p_values.iter().enumerate() {
            let a = p * (n as f64 + 1.0);
            let b = (1.0 - p) * (n as f64 + 1.0);

            let (lp, up) = if a <= 0.0 || b <= 0.0 {
                // fallback if Beta parameters invalid
                if p < 0.5 {
                    (0.0, 0.0)
                } else {
                    (1.0, 1.0)
                }
            } else {
                // Construct Beta distribution
                let beta_dist = Beta::new(a, b).unwrap_or_else(|_| Beta::new(0.5, 0.5).unwrap());
                let lower_p = beta_dist.inverse_cdf(LOWER_Q).clamp(0.0, 1.0);
                let upper_p = beta_dist.inverse_cdf(UPPER_Q).clamp(0.0, 1.0);
                (lower_p, upper_p)
            };

            // Convert these fractional bounds to an integer window size
            let window_size_f = (up - lp) * (n as f64);
            let mut window_size = window_size_f.round() as usize;
            if window_size < 1 {
                window_size = 1;
            }
            table[i][j] = window_size;
        }
    }
    table
}

/// Convert a `u64` to a percentile p in [0..1)
fn u64_to_percentile(x: u64) -> f64 {
    (x as f64) / (u64::MAX as f64 + 1.0)
}

/// Return the approximate window [lower, upper] for a given n, p, using the precomputed table.
fn get_window(
    n_values: &[usize],
    p_values: &[f64],
    window_sizes: &[Vec<usize>],
    n: usize,
    p: f64,
) -> (usize, usize) {
    // 1) find closest N in log-space
    let log_n = (n as f64).log10();
    let mut best_idx_n = 0;
    let mut best_dist_n = f64::MAX;
    for (i, &candidate) in n_values.iter().enumerate() {
        let dist = (candidate as f64).log10() - log_n;
        let dist = dist.abs();
        if dist < best_dist_n {
            best_dist_n = dist;
            best_idx_n = i;
        }
    }

    // 2) find closest p in p_values
    let mut best_idx_p = 0;
    let mut best_dist_p = f64::MAX;
    for (j, &candidate) in p_values.iter().enumerate() {
        let dist = (candidate - p).abs();
        if dist < best_dist_p {
            best_dist_p = dist;
            best_idx_p = j;
        }
    }

    // 3) retrieve the precomputed window size
    let window_size = window_sizes[best_idx_n][best_idx_p];

    // 4) center that window around middle = p*n
    let middle = (p * (n as f64)).round() as i64;
    let half_window = std::cmp::max(1, (window_size as i64) / 2);

    let mut lower = middle - half_window;
    let mut upper = middle + half_window;
    if lower < 0 {
        lower = 0;
    }
    if upper > n as i64 {
        upper = n as i64;
    }
    (lower as usize, upper as usize)
}

/// Generate a sorted list of random u64 values
fn generate_numbers(n: usize, rng: &mut StdRng) -> Vec<u64> {
    let mut nums = Vec::with_capacity(n);
    for _ in 0..n {
        nums.push(rng.gen::<u64>());
    }
    nums.sort();
    nums
}

/// Approx method
fn compute_hit_rate_approx(
    test_keys: &[(u64, usize)],
    n: usize,
    n_values: &[usize],
    p_values: &[f64],
    window_sizes: &[Vec<usize>],
) -> f64 {
    let mut hits = 0usize;
    for &(random_val, actual_idx) in test_keys {
        let p = u64_to_percentile(random_val);
        let (low, high) = get_window(n_values, p_values, window_sizes, n, p);
        if actual_idx >= low && actual_idx <= high {
            hits += 1;
        }
    }
    hits as f64 / (test_keys.len() as f64)
}

/// Exact method: for each random_val, compute Beta distribution on the fly
fn compute_hit_rate_exact(test_keys: &[(u64, usize)], n: usize) -> (f64, f64) {
    let mut hits = 0usize;
    let mut sum_window = 0usize;

    for &(random_val, actual_idx) in test_keys {
        let p = u64_to_percentile(random_val);
        let a = p * (n as f64 + 1.0);
        let b = (1.0 - p) * (n as f64 + 1.0);

        let (lp, up) = if a <= 0.0 || b <= 0.0 {
            if p < 0.5 {
                (0.0, 0.0)
            } else {
                (1.0, 1.0)
            }
        } else {
            let beta_dist = Beta::new(a, b).unwrap_or_else(|_| Beta::new(0.5, 0.5).unwrap());
            let lo = beta_dist.inverse_cdf(LOWER_Q).clamp(0.0, 1.0);
            let hi = beta_dist.inverse_cdf(UPPER_Q).clamp(0.0, 1.0);
            (lo, hi)
        };

        let lower_idx = (lp * (n as f64)).floor().max(0.0) as usize;
        let upper_idx = (up * (n as f64)).ceil().min(n as f64) as usize;

        let window_sz = upper_idx.saturating_sub(lower_idx);
        sum_window += window_sz;

        if actual_idx >= lower_idx && actual_idx < upper_idx {
            hits += 1;
        }
    }

    let hit_rate = hits as f64 / (test_keys.len() as f64);
    let avg_window = sum_window as f64 / (test_keys.len() as f64);
    (hit_rate, avg_window)
}

pub(crate) fn main() {
    let n_values = build_n_values();
    let p_values = build_p_values();
    println!("Precomputing window sizes...");
    let t0 = Instant::now();
    let window_sizes = precompute_window_sizes(&n_values, &p_values);
    let elapsed = t0.elapsed().as_secs_f64();
    println!("Precomputation complete in {elapsed:.2}s");

    let test_ns = [100, 1000, 10_000, 100_000, 1_000_000];
    let num_iterations = 10;
    let mut rng = StdRng::seed_from_u64(0x1234_5678_ABCD);

    for &n in &test_ns {
        let mut approx_hit_rate_sum = 0.0;
        let mut exact_hit_rate_sum = 0.0;
        let mut approx_time = 0.0;
        let mut exact_time = 0.0;

        for _ in 0..num_iterations {
            // 1) Generate sorted random u64s
            let numbers = generate_numbers(n, &mut rng);

            // 2) pick N//10 random test indices
            let test_size = (n / 10).max(1);
            let mut indices = Vec::new();
            for _ in 0..test_size {
                let idx = rng.gen_range(0..n);
                indices.push(idx);
            }

            // 3) Build test set => (random_value, actual_idx)
            let test_keys: Vec<(u64, usize)> = indices.iter().map(|&i| (numbers[i], i)).collect();

            // ~~~~~ Approx
            let start_a = Instant::now();
            let a_rate =
                compute_hit_rate_approx(&test_keys, n, &n_values, &p_values, &window_sizes);
            approx_time += start_a.elapsed().as_secs_f64();
            approx_hit_rate_sum += a_rate;

            // ~~~~~ Exact
            let start_e = Instant::now();
            let (x_rate, _avg_w) = compute_hit_rate_exact(&test_keys, n);
            exact_time += start_e.elapsed().as_secs_f64();
            exact_hit_rate_sum += x_rate;
        }

        let final_approx = approx_hit_rate_sum / (num_iterations as f64);
        let final_exact = exact_hit_rate_sum / (num_iterations as f64);
        let final_approx_time = approx_time / (num_iterations as f64);
        let final_exact_time = exact_time / (num_iterations as f64);

        println!(
            "{n:>8} entries: Approx hit rate {final_approx:.3}, approx time: {final_approx_time:.3}s"
        );
        println!(
            "{n:>8} entries: Exact  hit rate {final_exact:.3}, exact  time: {final_exact_time:.3}s"
        );
        println!();
    }
}
