use criterion::{black_box, criterion_group, criterion_main, Criterion};

use lazy_static::lazy_static;
use pgmq::query::check_input;
use regex::Regex;

pub fn check_regex(input: &str) {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"^[a-zA-Z0-9_]+$"#).unwrap();
    }
    if !RE.is_match(input) {
        panic!("Invalid queue name: {input}")
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("check bytes", |b| {
        b.iter(|| check_input(black_box("myqueue_123_longername")))
    });
    c.bench_function("check regex", |b| {
        b.iter(|| check_regex(black_box("myqueue_123_longername")))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
