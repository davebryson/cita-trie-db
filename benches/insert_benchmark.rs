use criterion::{criterion_group, criterion_main, Criterion};

use cita_trie::codec::RLPNodeCodec;
// Adapted from the cita-trie bench mark
use cita_trie::trie::{PatriciaTrie, Trie};
use cita_trie_db::RocksDb;

fn insert_worse_case_benchmark(c: &mut Criterion) {
    c.bench_function("insert 100 items", |b| {
        let test_dir = "test_dir";
        b.iter(|| {
            let mut db = RocksDb::new(test_dir);
            let mut trie = PatriciaTrie::new(&mut db, RLPNodeCodec::default());
            const N: usize = 100;
            let mut buf = Vec::new();
            for i in 0..N {
                buf.push(i as u8);
                trie.insert(&buf, b"testvalue").unwrap();
            }
        })
    });
}

criterion_group!(benches, insert_worse_case_benchmark);
criterion_main!(benches);
