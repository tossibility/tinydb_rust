use kawaii::*;

fn main() {
    let mut shohin: Table = Table::create(
        "shohin",
        attributes![
            ("shohin_id", TypeKind::Integer),
            ("shohin_name", TypeKind::Varchar),
            ("kubun_id", TypeKind::Integer),
            ("price", TypeKind::Integer)
        ],
    );
    shohin
        .insert(values!(1, "りんご", 1, 300))
        .insert(values!(2, "みかん", 1, 130))
        .insert(values!(3, "キャベツ", 2, 200))
        .insert(values!(4, "さんま", 3, 220))
        .insert(values!(5, "わかめ", NULL, 250)) //区分がNULL
        .insert(values!(6, "しいたけ", 4, 180)) //該当区分なし
        .insert(values!(7, "ドリアン", 1, NULL));
    println!("{}", shohin);
    let mut kubun: Table = Table::create(
        "kubun",
        attributes![
            ("kubun_id", TypeKind::Integer),
            ("kubun_name", TypeKind::Varchar)
        ],
    );
    kubun
        .insert(values!(1, "くだもの"))
        .insert(values!(2, "野菜"))
        .insert(values!(3, "魚"));
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

#[cfg(test)]
mod tests {
    use std::cmp::Ord;
    use std::cmp::Ordering;
    use std::collections::BTreeMap;
    use std::convert::From;
    use std::fmt;

    #[derive(Debug)]
    struct ReverseString(String);

    impl PartialEq for ReverseString {
        fn eq(&self, other: &Self) -> bool {
            self.0.eq(&other.0)
        }
    }

    // this does not actually have any methods, it's just a flag on the type
    impl Eq for ReverseString {}

    // make partial_cmp() just return result from cmp()
    impl PartialOrd for ReverseString {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            other.0.partial_cmp(&self.0)
        }
    }

    impl Ord for ReverseString {
        fn cmp(&self, other: &Self) -> Ordering {
            other.0.cmp(&self.0)
        }
    }

    impl From<String> for ReverseString {
        fn from(item: String) -> Self {
            ReverseString(item)
        }
    }

    impl From<&str> for ReverseString {
        fn from(item: &str) -> Self {
            ReverseString(String::from(item))
        }
    }

    impl fmt::Display for ReverseString {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    #[derive(Debug, Eq, Clone, Copy)]
    struct ValuePtr<'a> {
        index: usize,
        values: &'a [String],
    }

    impl PartialEq for ValuePtr<'_> {
        fn eq(&self, other: &Self) -> bool {
            let lhs = &self.values[self.index];
            let rhs = &other.values[other.index];
            lhs.eq(rhs)
        }
    }

    // make partial_cmp() just return result from cmp()
    impl PartialOrd for ValuePtr<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            let lhs = &self.values[self.index];
            let rhs = &other.values[other.index];
            lhs.partial_cmp(rhs)
        }
    }

    impl Ord for ValuePtr<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            let lhs = &self.values[self.index];
            let rhs = &other.values[other.index];
            lhs.cmp(rhs)
        }
    }

    impl fmt::Display for ValuePtr<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let value = &self.values[self.index];
            value.fmt(f)
        }
    }

    #[test]
    fn test_value_pointer() {
        let values: Vec<String> = ["Alice", "Bob", "Charlie"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let alice = ValuePtr {
            index: 0,
            values: &values,
        };
        let bob = ValuePtr {
            index: 1,
            values: &values,
        };
        let charlie = ValuePtr {
            index: 2,
            values: &values,
        };
        let mut map = BTreeMap::new();
        map.insert(bob.clone(), 1);
        map.insert(alice.clone(), 0);
        map.insert(charlie.clone(), 2);
        for (k, v) in &map {
            println!("{}, {}", k, v);
        }
        let mut iter = map.iter();
        assert_eq!(iter.next(), Some((&alice, &0)));
        assert_eq!(iter.next(), Some((&bob, &1)));
        assert_eq!(iter.next(), Some((&charlie, &2)));
    }

    #[test]
    fn test_btree_custom_order() {
        let mut map = BTreeMap::new();
        map.insert(String::from("Alice"), 0);
        map.insert(String::from("Bob"), 1);
        map.insert(String::from("Charlie"), 2);
        let mut iter = map.iter();
        assert_eq!(iter.next(), Some((&String::from("Alice"), &0)));
        assert_eq!(iter.next(), Some((&String::from("Bob"), &1)));
        assert_eq!(iter.next(), Some((&String::from("Charlie"), &2)));

        let mut map: BTreeMap<ReverseString, i32> = BTreeMap::new();
        map.insert(ReverseString::from("Alice"), 0);
        map.insert(ReverseString::from("Bob"), 1);
        map.insert(ReverseString::from("Charlie"), 2);
        let mut iter = map.iter();
        assert_eq!(iter.next(), Some((&ReverseString::from("Charlie"), &2)));
        assert_eq!(iter.next(), Some((&ReverseString::from("Bob"), &1)));
        assert_eq!(iter.next(), Some((&ReverseString::from("Alice"), &0)));
    }
}
