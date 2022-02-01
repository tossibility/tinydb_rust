#![feature(test)]

extern crate test;

use kawaii::*;
use rand::Rng;

/// 与えられた文字からnだけコードを進めた文字を返します。
fn add_char(c: char, n: i32) -> char {
    std::char::from_u32(c as u32 + n as u32).unwrap_or(c)
}

/// テスト用テーブルを作成します。
/// shohin: 100000
///   shohin_id: seq
///   shohin_name: 8*82?
///   kubun_id: 10000
///   price: 990
/// kubun: 10000
///   kubun_id: seq
///   kubun_name: 8*82?
fn setup_tables() -> (Table, Table) {
    let mut rng = rand::thread_rng();
    let mut shohin = Table::create(
        "shohin",
        attribute_slice![
            ("shohin_id", TypeKind::Integer),
            ("shohin_name", TypeKind::Varchar),
            ("kubun_id", TypeKind::Integer),
            ("price", TypeKind::Integer)
        ],
    );
    for i in 0..100000 {
        let len = rng.gen_range(0, 5) + 3;
        let mut name = String::new();
        name.reserve(len);
        for _ in 0..len {
            name.push(add_char('あ', rng.gen_range(0, 82)));
        }
        shohin.insert(datum_slice![
            i + 1,
            &name,
            rng.gen_range(0, 10000) + 1,
            rng.gen_range(0, 990) * 10
        ]);
    }
    let mut kubun = Table::create(
        "kubun",
        attribute_slice![
            ("kubun_id", TypeKind::Integer),
            ("kubun_name", TypeKind::Varchar)
        ],
    );
    for i in 0..10000 {
        let len = rng.gen_range(0, 5) + 3;
        let mut name = String::new();
        name.reserve(len);
        for _ in 0..len {
            name.push(add_char('ア', rng.gen_range(0, 82)));
        }
        kubun.insert(datum_slice![i + 1, &name]);
    }
    (shohin, kubun)
}

/// LessThanのベンチマークテストです。
#[bench]
fn bench_less_than(b: &mut test::Bencher) {
    let (shohin, _) = setup_tables();
    b.iter(|| {
        let mut rng = rand::thread_rng();
        shohin.less_than("price", rng.gen_range(0, 10000))
    })
}

/// GroupByのベンチマークテストです。
#[bench]
fn bench_group_by(b: &mut test::Bencher) {
    let (shohin, _) = setup_tables();
    b.iter(|| {
        shohin.group_by(
            &["kubun_id"],
            &[Agg::count("shohin_name"), Agg::average("price")],
        )
    })
}
