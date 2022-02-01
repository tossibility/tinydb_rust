use kawaii::*;

fn main() {
    let mut shohin: Table = Table::create(
        "shohin",
        attribute_slice![
            ("shohin_id", TypeKind::Integer),
            ("shohin_name", TypeKind::Varchar),
            ("kubun_id", TypeKind::Integer),
            ("price", TypeKind::Integer)
        ],
    );
    shohin
        .insert(datum_slice!(1, "りんご", 1, 300))
        .insert(datum_slice!(2, "みかん", 1, 130))
        .insert(datum_slice!(3, "キャベツ", 2, 200))
        .insert(datum_slice!(4, "さんま", 3, 220))
        .insert(datum_slice!(5, "わかめ", NULL, 250)) //区分がNULL
        .insert(datum_slice!(6, "しいたけ", 4, 180)) //該当区分なし
        .insert(datum_slice!(7, "ドリアン", 1, NULL));
    println!("{}", shohin);
    let mut kubun: Table = Table::create(
        "kubun",
        attribute_slice![
            ("kubun_id", TypeKind::Integer),
            ("kubun_name", TypeKind::Varchar)
        ],
    );
    kubun
        .insert(datum_slice!(1, "くだもの"))
        .insert(datum_slice!(2, "野菜"))
        .insert(datum_slice!(3, "魚"));
    println!("{}", kubun);
    {
        println!("{}", shohin);
    }
    {
        println!("{}", shohin.less_than("price", 200));
    }
    {
        println!("{}", shohin.select(&["shohin_name", "price"]));
    }
    {
        // 参照が切れるのでメソッドチェーンできない
        // Vec<Box<dyn Relation>> に入れようとしてもmut参照が2つになるのでエラーとなる
        let r1 = shohin.less_than("price", 200);
        let r2 = r1.select(&["shohin_name", "price"]);
        println!("{}", r2);
    }
    {
        let r1 = shohin.group_by(
            &["kubun_id"],
            &[Agg::count("shohin_name"), Agg::average("price")],
        );
        println!("{}", r1);
    }
}
