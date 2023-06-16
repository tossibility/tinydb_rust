//!
//! かわいいデータベースのRust実装です。
//!
//! https://nowokay.hatenablog.com/entry/20120817/1345197962
//!
//! # 実装したもの
//!
//! * 値を表すValue構造体
//! * リレーションTraitとそれを実装するテーブルとかいろいろ
//! * Select, LessThan, EqualsTo, GroupBy
//!
//! ## できてないもの
//!
//! * LeftJoin(ごめんなさい)
//! * エラーチェックと準正常系単体テスト(不正なカラム名とか全然だめ)
//! * メソッドチェーン(ボローチェッカーが厳しいので)
//! * いい感じのモジュール分割(lib.rsに全部入り)
//! * マルチスレッド
//!
//! # 体験できたRustの機能
//!
//! * 普通の構造体
//! * Enum
//! * Trait
//! * ジェネリクス
//! * クロージャー
//! * マクロ
//! * ライフタイムとボローチェッカー(きつかった)
//! * 単体テスト
//! * ベンチマークテスト
//! * ドキュメンテーション
//!
//! # 頑張ってみたこと
//!
//! * カラムナーです
//!
//! # 所感
//!
//! * 書き味は楽しいし結構速い
//! * ボローチェッカーのせいでDictionaryのインデックス部分に参照を置けない
//! * でもunsafe使うと負けな気がするので妥協した
//! * クエリ実行しながらinsertもできない
//!     * スレッドローカルとMVCCを駆使するしかないか...
//! * Shared-Nothing, Read-Only なシステムなら使えるかな
//!
use bit_vec::BitVec;
use std::borrow::Borrow;
use std::cmp;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::convert::From;
use std::fmt;
use std::ops::{Deref, DerefMut, Index, IndexMut, Range, RangeBounds};

/// BTreeの範囲検索結果。
type BTreeRange<'a, K, V> = btree_map::Range<'a, K, V>;
/// ビットマップ。
type BitMap = BitVec;

/// NULLを表す構造体。
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
pub struct Null {}

impl fmt::Display for Null {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "null")
    }
}

/// NULLを表す定数。
/// モジュールの外側で参照したいときは `pub` をつけます。
pub const NULL: Null = Null {};

/// 値の型を表します。
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum TypeKind {
    /// 文字列
    Varchar,
    /// 整数
    Integer,
}

///
/// 文字列, 整数, NULLのいずれかを持つEnumです。
///
/// # Examples
///
/// ```
/// use kawaii::{Value, NULL};
/// let s: kawaii::Value = "str".into();
/// let i: kawaii::Value = 32.into();
/// let n: kawaii::Value = NULL.into();
/// ```
///
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub enum Value<'a> {
    /// 文字列
    Varchar(&'a str),
    /// 整数
    Integer(i32),
    /// NULL
    Null(Null),
}

impl<'a> From<&'a String> for Value<'a> {
    fn from(item: &'a String) -> Self {
        Value::Varchar(item.as_ref())
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(item: &'a str) -> Self {
        Value::Varchar(item)
    }
}

impl<'a> From<i32> for Value<'a> {
    fn from(item: i32) -> Self {
        Value::Integer(item)
    }
}

impl<'a> From<Null> for Value<'a> {
    fn from(item: Null) -> Self {
        Value::Null(item)
    }
}

impl<'a> fmt::Display for Value<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Varchar(val) => write!(f, "{}", val),
            Value::Integer(val) => write!(f, "{}", val),
            Value::Null(val) => write!(f, "{}", val),
        }
    }
}

///
/// Valueとしての振る舞いを規定したTraitです。
///
/// # Examples
///
/// ```
/// use kawaii::{Value, NULL};
/// let s: kawaii::Value = "str".into();
/// let i: kawaii::Value = 32.into();
/// let n: kawaii::Value = NULL.into();
/// ```
///
pub trait AsValue {
    fn as_datum_ref(&self) -> Value;
}

impl<'a> AsValue for &'a str {
    fn as_datum_ref(&self) -> Value {
        (*self).into()
    }
}

impl AsValue for i32 {
    fn as_datum_ref(&self) -> Value {
        (*self).into()
    }
}

impl AsValue for Null {
    fn as_datum_ref(&self) -> Value {
        (*self).into()
    }
}

impl AsValue for Value<'_> {
    fn as_datum_ref(&self) -> Value {
        match self {
            Value::Varchar(val) => Value::Varchar(val),
            Value::Integer(val) => Value::Integer(*val),
            Value::Null(val) => Value::Null(*val),
        }
    }
}

/// 値に割り振られるID
type KeyId = usize;
/// 行番号
type RowId = usize;
/// 列番号
type ColumnId = usize;
/// NULLを表すKeyId
/// FIXME: KeyIdをEnumにしてif文を無くしたい
const NULL_KEY_ID: KeyId = KeyId::max_value();

///
/// 値とそのIDを管理します。
///
/// # Examples
///
/// ```
/// // ちなみにドキュメンテーションコメント内のテストは cargo test で実行されます。
/// let mut dictionary = kawaii::Dictionary::new();
/// assert_eq!(dictionary.insert("Alice"), 0);
/// assert_eq!(dictionary.insert("Bob"), 1);
/// assert_eq!(dictionary.insert("Alice"), 0);
/// assert_eq!(dictionary.num_keys(), 2);
/// assert_eq!(dictionary.id_of("Alice"), Some(0));
/// assert_eq!(dictionary.id_of("Bob"), Some(1));
/// assert_eq!(dictionary.id_of("Chris"), None);
/// let mut iter = dictionary.range("Alice".."Chris");
/// assert_eq!(iter.next(), Some((&"Alice", &0)));
/// assert_eq!(iter.next(), Some((&"Bob", &1)));
/// assert_eq!(iter.next(), None);
/// assert_eq!(dictionary.key_of(0), &"Alice");
/// assert_eq!(dictionary.key_of(1), &"Bob");
/// ```
///
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Dictionary<Key>
where
    Key: Ord + Clone,
{
    key_to_id: BTreeMap<Key, KeyId>,
    keys: Vec<Key>,
}

impl<Key> Dictionary<Key>
where
    Key: Ord + Clone,
{
    ///
    /// Dictionaryインスタンスを新規に作成します。
    ///
    pub fn new() -> Dictionary<Key> {
        Dictionary {
            key_to_id: BTreeMap::new(),
            keys: Vec::new(),
        }
    }
    ///
    /// 値の数を返します。
    ///
    pub fn num_keys(&self) -> KeyId {
        self.key_to_id.len()
    }
    ///
    /// 値を登録します。すでに存在していればそのIDを、新規なら新しいIDを返します。
    ///
    pub fn insert(&mut self, key: Key) -> KeyId {
        let n_keys = self.num_keys();
        let id = self.key_to_id.entry(key.clone()).or_insert(n_keys);
        if *id == n_keys {
            self.keys.push(key);
        }
        *id
    }
    ///
    /// 値に対応するIDを返します。存在しなければ`None`が返ります。
    ///
    pub fn id_of<Q: Ord + ?Sized>(&self, key: &Q) -> Option<KeyId>
    where
        Key: Borrow<Q>,
    {
        self.key_to_id.get(key).copied()
    }
    ///
    /// 値の範囲を指定して、対応する範囲のイテレーターを返します。
    ///
    pub fn range<T, R>(&self, range: R) -> BTreeRange<Key, KeyId>
    where
        Key: Borrow<T>,
        R: RangeBounds<T>,
        T: Ord + ?Sized,
    {
        self.key_to_id.range(range)
    }
    ///
    /// 値の範囲を指定して、対応するIDがtrueになったビットマップを返します。
    ///
    /// # Examples
    ///
    /// ```
    /// let mut dictionary = kawaii::Dictionary::new();
    /// dictionary.insert("Alice");
    /// dictionary.insert("Bob");
    /// dictionary.insert("Alice");
    /// let result = dictionary.range_into_bits("Alice".."Bob");
    /// assert_eq!(result.get(0), Some(true));
    /// assert_eq!(result.get(1), Some(false));
    /// ```
    ///
    pub fn range_into_bits<T, R>(&self, range: R) -> BitMap
    where
        Key: Borrow<T>,
        R: RangeBounds<T>,
        T: Ord + ?Sized,
    {
        let mut bitmap = BitMap::from_elem(self.num_keys(), false);
        for (&_, &key_id) in self.key_to_id.range(range) {
            bitmap.set(key_id, true);
        }
        bitmap
    }
    ///
    /// IDに対応する値の参照を返します。
    ///
    /// # Panics
    ///
    /// ``key_id >= self.num_keys()` の場合
    ///
    pub fn key_of(&self, key_id: KeyId) -> &Key {
        assert!(key_id < self.num_keys());
        &self.keys[key_id]
    }
}

///
/// カラムを表します。
///
/// # Examples
///
/// ```
/// use kawaii::Column;
/// let mut column = Column::new();
/// assert_eq!(column.append("Alice"), 0);
/// assert_eq!(column.append("Bob"), 1);
/// assert_eq!(column.append("Alice"), 0);
/// assert_eq!(column.num_keys(), 2);
/// assert_eq!(column.num_rows(), 3);
/// assert_eq!(column.id_of("Alice"), Some(0));
/// assert_eq!(column.id_of("Bob"), Some(1));
/// assert_eq!(column.id_of("Chris"), None);
/// let mut iter = column.range("Alice".."Chris");
/// assert_eq!(iter.next(), Some((&"Alice", &0)));
/// assert_eq!(iter.next(), Some((&"Bob", &1)));
/// assert_eq!(iter.next(), None);
/// assert_eq!(column.key_of(0), &"Alice");
/// assert_eq!(column.key_of(1), &"Bob");
/// assert_eq!(column.key_at(0), Some(&"Alice"));
/// assert_eq!(column.key_at(1), Some(&"Bob"));
/// assert_eq!(column.key_at(2), Some(&"Alice"));
/// ```
///
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Column<Key>
where
    Key: Ord + Clone,
{
    dictionary: Dictionary<Key>,
    key_ids: Vec<KeyId>,
}

impl<Key> Column<Key>
where
    Key: Ord + Clone,
{
    /// 新しい`Column`インスタンスを作成します。
    pub fn new() -> Column<Key> {
        Column {
            dictionary: Dictionary::new(),
            key_ids: Vec::new(),
        }
    }
    /// 登録されている値の数を返します。
    pub fn num_keys(&self) -> KeyId {
        self.dictionary.num_keys()
    }
    /// 行数を返します。
    pub fn num_rows(&self) -> RowId {
        self.key_ids.len()
    }
    /// 値を追記し、その値に割り振られたIDを返します。
    pub fn append(&mut self, key: Key) -> KeyId {
        let key_id = self.dictionary.insert(key);
        self.key_ids.push(key_id);
        key_id
    }
    /// NULLを追記し、NULL_KEY_IDを返します。
    /// FIXME: IDでの判定をやめたい
    pub fn append_null(&mut self) -> KeyId {
        self.key_ids.push(NULL_KEY_ID);
        NULL_KEY_ID
    }
    /// 末尾のIDを削除してそれを返します。num_rows() == 0 の場合 `None`を返します。
    pub fn pop(&mut self) -> Option<KeyId> {
        self.key_ids.pop()
    }
    pub fn id_of<Q: Ord + ?Sized>(&self, key: &Q) -> Option<KeyId>
    where
        Key: Borrow<Q>,
    {
        self.dictionary.id_of(key)
    }
    pub fn range<T, R>(&self, range: R) -> BTreeRange<Key, KeyId>
    where
        Key: Borrow<T>,
        R: RangeBounds<T>,
        T: Ord + ?Sized,
    {
        self.dictionary.range(range)
    }
    pub fn range_into_bits<T, R>(&self, range: R) -> BitMap
    where
        Key: Borrow<T>,
        R: RangeBounds<T>,
        T: Ord + ?Sized,
    {
        self.dictionary.range_into_bits(range)
    }
    pub fn key_of(&self, key_id: KeyId) -> &Key {
        self.dictionary.key_of(key_id)
    }
    ///
    /// 行番号を指定してキーIDを取得します。
    ///
    /// # Panics
    ///
    /// `row_id >= self.num_rows()` の場合
    ///
    pub fn id_at(&self, row_id: RowId) -> KeyId {
        assert!(row_id < self.num_rows());
        self.key_ids[row_id]
    }
    ///
    /// 行番号を指定してキーを取得します。
    ///
    /// # Panics
    ///
    /// `row_id >= self.num_rows()` の場合
    ///
    pub fn key_at(&self, row_id: RowId) -> Option<&Key> {
        let key_id = self.id_at(row_id);
        // TODO: enumつかう
        if key_id == NULL_KEY_ID {
            return None;
        }
        Some(self.key_of(key_id))
    }
}

///
/// カラムとしてのふるまいを規定するTraitです。
///
pub trait AsColumn: fmt::Debug {
    fn num_keys(&self) -> KeyId;
    fn num_rows(&self) -> RowId;
    fn append(&mut self, key: &dyn AsValue) -> Option<KeyId>;
    fn pop(&mut self) -> Option<KeyId>;
    fn id_of(&self, key: &dyn AsValue) -> Option<KeyId>;
    fn range(&self, range: Range<&dyn AsValue>) -> Option<BitMap>;
    fn range_from(&self, key: &dyn AsValue) -> Option<BitMap>;
    fn range_to(&self, key: &dyn AsValue) -> Option<BitMap>;
    fn key_of(&self, key_id: KeyId) -> Value;
    fn id_at(&self, row_id: RowId) -> KeyId;
    fn key_at(&self, row_id: RowId) -> Value;
}

/// Table用カラム
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum TableColumn {
    Varchar(Column<String>),
    Integer(Column<i32>),
}

impl TableColumn {
    pub fn new(kind: TypeKind) -> TableColumn {
        match kind {
            TypeKind::Varchar => TableColumn::Varchar(Column::new()),
            TypeKind::Integer => TableColumn::Integer(Column::new()),
        }
    }
}

impl AsColumn for TableColumn {
    fn num_keys(&self) -> KeyId {
        match self {
            TableColumn::Varchar(column) => column.num_keys(),
            TableColumn::Integer(column) => column.num_keys(),
        }
    }
    fn num_rows(&self) -> RowId {
        match self {
            TableColumn::Varchar(column) => column.num_rows(),
            TableColumn::Integer(column) => column.num_rows(),
        }
    }
    fn append(&mut self, key: &dyn AsValue) -> Option<KeyId> {
        match self {
            TableColumn::Varchar(column) => match key.as_datum_ref() {
                Value::Varchar(key) => Some(column.append(key.to_string())),
                Value::Null(_) => Some(column.append_null()),
                _ => None,
            },
            TableColumn::Integer(column) => match key.as_datum_ref() {
                Value::Integer(key) => Some(column.append(key)),
                Value::Null(_) => Some(column.append_null()),
                _ => None,
            },
        }
    }
    fn pop(&mut self) -> Option<KeyId> {
        match self {
            TableColumn::Varchar(column) => column.pop(),
            TableColumn::Integer(column) => column.pop(),
        }
    }
    fn id_of(&self, key: &dyn AsValue) -> Option<KeyId> {
        match (self, key.as_datum_ref()) {
            (TableColumn::Varchar(column), Value::Varchar(key)) => column.id_of(key),
            (TableColumn::Integer(column), Value::Integer(key)) => column.id_of(&key),
            _ => None,
        }
    }
    fn range(&self, range: Range<&dyn AsValue>) -> Option<BitMap> {
        match (self, range.start.as_datum_ref(), range.end.as_datum_ref()) {
            (TableColumn::Varchar(column), Value::Varchar(start), Value::Varchar(end)) => {
                Some(column.range_into_bits(start.to_string()..end.to_string()))
            }
            (TableColumn::Integer(column), Value::Integer(start), Value::Integer(end)) => {
                Some(column.range_into_bits(start..end))
            }
            _ => None,
        }
    }
    fn range_from(&self, key: &dyn AsValue) -> Option<BitMap> {
        match (self, key.as_datum_ref()) {
            (TableColumn::Varchar(column), Value::Varchar(key)) => {
                Some(column.range_into_bits(key.to_string()..))
            }
            (TableColumn::Integer(column), Value::Integer(key)) => {
                Some(column.range_into_bits(key..))
            }
            _ => None,
        }
    }
    fn range_to(&self, key: &dyn AsValue) -> Option<BitMap> {
        match (self, key.as_datum_ref()) {
            (TableColumn::Varchar(column), Value::Varchar(key)) => {
                Some(column.range_into_bits(..key.to_string()))
            }
            (TableColumn::Integer(column), Value::Integer(key)) => {
                Some(column.range_into_bits(..key))
            }
            _ => None,
        }
    }
    fn key_of(&self, key_id: KeyId) -> Value {
        if key_id >= self.num_keys() {
            return NULL.into();
        }
        match self {
            TableColumn::Varchar(column) => column.key_of(key_id).into(),
            TableColumn::Integer(column) => (*column.key_of(key_id)).into(),
        }
    }
    fn id_at(&self, row_id: RowId) -> KeyId {
        match self {
            TableColumn::Varchar(column) => column.id_at(row_id),
            TableColumn::Integer(column) => column.id_at(row_id),
        }
    }
    fn key_at(&self, row_id: RowId) -> Value {
        if let Some(key) = match self {
            TableColumn::Varchar(column) => column.key_at(row_id).map(|k| k.into()),
            TableColumn::Integer(column) => column.key_at(row_id).map(|k| (*k).into()),
        } {
            return key;
        }
        NULL.into()
    }
}

/// 属性(カラム定義)を表します。
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Attribute {
    name: String,
    kind: TypeKind,
}

impl Attribute {
    pub fn create(name: &str, kind: TypeKind) -> Attribute {
        Attribute {
            name: name.to_string(),
            kind: kind,
        }
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn kind(&self) -> TypeKind {
        self.kind
    }
}

///
/// 属性のスライスを簡便に定義します。
///
/// # Examples
///
/// ```
/// # use kawaii::*;
/// # fn main() {
/// #     let mut table = Table::create(
/// #         "shohin",
/// #         attributes![
/// #             ("shohin_id", TypeKind::Integer),
/// #             ("shohin_name", TypeKind::Varchar),
/// #             ("kubun_id", TypeKind::Integer),
/// #             ("price", TypeKind::Integer)
/// #         ],
/// #     );
/// # }
/// ```
#[macro_export]
macro_rules! attributes {
    ( $( { $name:expr, $($kind:expr),+ } ),* ) => {
        &[ $( Attribute::create($name.as_ref(), $( $kind ),+ ) ),* ]
    };
    ( $( ( $name:expr, $($kind:expr),+ ) ),* ) => {
        &[ $( Attribute::create($name.as_ref(), $( $kind ),+ ) ),* ]
    }
}

/// リレーション定義を表します。
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Definition {
    name: String,
    attributes: Vec<Attribute>,
}

impl Definition {
    pub fn create(name: &str, attributes: &[Attribute]) -> Definition {
        Definition {
            name: name.to_string(),
            attributes: attributes.to_vec(),
        }
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn num_columns(&self) -> ColumnId {
        self.attributes.len()
    }
    pub fn name_to_id(&self, col_name: &str) -> Option<ColumnId> {
        self.attributes
            .iter()
            .position(|column| &column.name == col_name)
    }
    pub fn select(&self, col_ids: &[ColumnId]) -> Definition {
        let mut attributes = Vec::new();
        for col_id in col_ids {
            if *col_id < self.num_columns() {
                attributes.push(self.attributes[*col_id].clone());
            }
        }
        Definition {
            name: self.name.clone(),
            attributes: attributes,
        }
    }
}

impl Index<ColumnId> for Definition {
    type Output = Attribute;
    fn index(&self, col_id: ColumnId) -> &Self::Output {
        &self.attributes[col_id]
    }
}

impl IndexMut<ColumnId> for Definition {
    fn index_mut(&mut self, col_id: ColumnId) -> &mut Self::Output {
        &mut self.attributes[col_id]
    }
}

/// タプルを表します(カラムナーなのでfetchでしか使われない)
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Tuple<'a> {
    values: Vec<Value<'a>>,
}

impl<'a> Tuple<'a> {
    pub fn new() -> Tuple<'a> {
        Tuple { values: Vec::new() }
    }
}

impl<'a> Deref for Tuple<'a> {
    type Target = Vec<Value<'a>>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'a> DerefMut for Tuple<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

impl<'a> Index<ColumnId> for Tuple<'a> {
    type Output = Value<'a>;
    fn index(&self, col_id: ColumnId) -> &Self::Output {
        &self.values[col_id]
    }
}

impl<'a> IndexMut<ColumnId> for Tuple<'a> {
    fn index_mut(&mut self, col_id: ColumnId) -> &mut Self::Output {
        &mut self.values[col_id]
    }
}

/// タプルのリストを表します(カラムナーなのでfetchでしか使われない)
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Tuples<'a> {
    values: Vec<Tuple<'a>>,
}

impl<'a> Tuples<'a> {
    pub fn new() -> Tuples<'a> {
        Tuples { values: Vec::new() }
    }
}

impl<'a> Deref for Tuples<'a> {
    type Target = Vec<Tuple<'a>>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'a> DerefMut for Tuples<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

impl<'a> Index<RowId> for Tuples<'a> {
    type Output = Tuple<'a>;
    fn index(&self, row_id: RowId) -> &Self::Output {
        &self.values[row_id]
    }
}

impl<'a> IndexMut<RowId> for Tuples<'a> {
    fn index_mut(&mut self, row_id: RowId) -> &mut Self::Output {
        &mut self.values[row_id]
    }
}

/// リレーションとしての振る舞いを規定するTraitです。
pub trait Relation: fmt::Display {
    /// 行数を返します。
    fn num_rows(&self) -> RowId;
    /// カラム数を返します。
    fn num_columns(&self) -> ColumnId;
    /// 自身の定義を返します。
    fn definition(&self) -> &Definition;
    /// col_id番目のカラムを返します。
    fn column_at(&self, col_id: ColumnId) -> &dyn AsColumn;
    /// 行範囲を指定して、その範囲内の有効な行番号をdestに書き込み、スライスとして返します。
    fn scan_row_ids<'a>(&self, range: Range<RowId>, dest: &'a mut [RowId]) -> &'a [RowId] {
        let start = range.start;
        let end = cmp::min(range.end, self.num_rows());
        if start >= end {
            return &dest[0..0];
        }
        let n_range = end - start;
        if dest.len() < n_range {
            return &dest[0..0];
        }
        let mut i = 0;
        for row_id in start..end {
            dest[i] = row_id;
            i += 1;
        }
        &dest[0..i]
    }
    /// 指定された範囲をフェッチします。範囲が無効なら`None`を返します。
    fn fetch(&self, range: Range<RowId>) -> Option<Tuples> {
        let range = {
            let start = range.start;
            let end = cmp::min(range.end, self.num_rows());
            start..end
        };
        if range.start >= range.end {
            return None;
        }
        let n_fetches = range.end - range.start;
        let n_cols = self.num_columns();
        let mut tuples = Tuples::new();
        tuples.reserve(n_fetches);
        const STEP: RowId = 64;
        let mut start = range.start;
        let mut buffer = [0; STEP];
        while start < range.end {
            let step = cmp::min(STEP, range.end - start);
            let end = start + step;
            let row_ids = self.scan_row_ids(start..end, &mut buffer);
            for row_id in row_ids {
                let mut tuple = Tuple::new();
                tuple.reserve(n_cols);
                for col_id in 0..n_cols {
                    let column = &self.column_at(col_id);
                    tuple.push(column.key_at(*row_id));
                }
                tuples.push(tuple);
            }
            start += step;
        }
        tuples.shrink_to_fit();
        Some(tuples)
    }
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(tuples) = self.fetch(0..self.num_rows()) {
            for col_id in 0..self.num_columns() {
                let name = &self.definition()[col_id].name();
                if let fmt::Result::Err(err) = write!(f, "│{}", name) {
                    return fmt::Result::Err(err);
                }
            }
            if let fmt::Result::Err(err) = write!(f, "│\n") {
                return fmt::Result::Err(err);
            }
            for tuple in tuples.deref() {
                for field in tuple.deref() {
                    if let fmt::Result::Err(err) = write!(f, "│{}", field) {
                        return fmt::Result::Err(err);
                    }
                }
                if let fmt::Result::Err(err) = write!(f, "│\n") {
                    return fmt::Result::Err(err);
                }
            }
        }
        write!(f, "")
    }
}

/// テーブルを表します。
#[derive(PartialEq, Eq, Debug)]
pub struct Table {
    num_rows: RowId,
    definition: Definition,
    columns: Vec<TableColumn>,
}

impl Table {
    pub fn new(definition: Definition) -> Table {
        let mut columns = Vec::new();
        for attribute in &definition.attributes {
            columns.push(TableColumn::new(attribute.kind));
        }
        Table {
            num_rows: 0,
            definition: definition,
            columns: columns,
        }
    }
    pub fn create(name: &str, attributes: &[Attribute]) -> Table {
        Table::new(Definition::create(name, attributes))
    }
    pub fn insert<T: AsValue>(&mut self, tuple: &[T]) -> Option<&mut Table> {
        let n_cols = self.num_columns();
        if n_cols != tuple.len() {
            return None;
        }
        for col_id in 0..n_cols {
            // appendに失敗したらappend済みのカラムをもとに戻す
            if self.columns[col_id].append(&tuple[col_id]).is_none() {
                for j in 0..col_id {
                    self.columns[j].pop();
                }
                return None;
            }
        }
        self.num_rows += 1;
        Some(self)
    }
}

impl Relation for Table {
    fn num_columns(&self) -> ColumnId {
        self.columns.len()
    }
    fn num_rows(&self) -> RowId {
        self.num_rows
    }
    fn definition(&self) -> &Definition {
        &self.definition
    }
    fn column_at(&self, col_id: ColumnId) -> &dyn AsColumn {
        &self.columns[col_id]
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Relation::fmt(self, f)
    }
}

pub trait Insertable<'a> {
    fn insert<T: AsValue>(self, tuple: &[T]) -> Option<&'a mut Table>;
}

impl<'a> Insertable<'a> for Option<&'a mut Table> {
    fn insert<T: AsValue>(self, tuple: &[T]) -> Option<&'a mut Table> {
        self.and_then(|table| table.insert(tuple))
    }
}

///
/// 射影結果リレーションを表します。
///
#[derive(Clone)]
pub struct SelectedRelation<'a> {
    definition: Definition,
    relation: &'a dyn Relation,
    col_ids: Vec<ColumnId>,
}

impl<'a> Relation for SelectedRelation<'a> {
    fn num_columns(&self) -> ColumnId {
        self.col_ids.len()
    }
    fn num_rows(&self) -> RowId {
        self.relation.num_rows()
    }
    fn definition(&self) -> &Definition {
        &self.definition
    }
    fn column_at(&self, col_id: ColumnId) -> &dyn AsColumn {
        let col_id = self.col_ids[col_id];
        self.relation.column_at(col_id)
    }
}

impl fmt::Display for SelectedRelation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Relation::fmt(self, f)
    }
}

///
/// リレーションに対してカラムを指定して射影します。
///
pub trait Select: Relation {
    fn select(&self, col_names: &[&str]) -> SelectedRelation;
}

impl<T> Select for T
where
    T: Relation,
{
    fn select(&self, col_names: &[&str]) -> SelectedRelation {
        let mut col_ids = Vec::new();
        for col_name in col_names {
            if let Some(col_id) = self.definition().name_to_id(col_name) {
                col_ids.push(col_id);
            }
        }
        let definition = self.definition().select(&col_ids);
        SelectedRelation {
            definition: definition,
            relation: self,
            col_ids: col_ids.to_vec(),
        }
    }
}

///
/// 抽出結果リレーションを表します。
///
#[derive(Clone)]
pub struct FilteredRelation<'a> {
    relation: &'a dyn Relation,
    valid_row_ids: Vec<RowId>,
}

impl<'a> Relation for FilteredRelation<'a> {
    fn num_columns(&self) -> ColumnId {
        self.relation.num_columns()
    }
    fn num_rows(&self) -> RowId {
        self.valid_row_ids.len()
    }
    fn definition(&self) -> &Definition {
        &self.relation.definition()
    }
    fn column_at(&self, col_id: ColumnId) -> &dyn AsColumn {
        self.relation.column_at(col_id)
    }
    fn scan_row_ids<'b>(&self, range: Range<RowId>, dest: &'b mut [RowId]) -> &'b [RowId] {
        let start = range.start;
        let end = cmp::min(range.end, self.num_rows());
        if start >= end {
            return &dest[0..0];
        }
        let n_range = end - start;
        if dest.len() < n_range {
            return &dest[0..0];
        }
        let mut i = 0;
        for row_id in start..end {
            dest[i] = self.valid_row_ids[row_id];
            i += 1;
        }
        &dest[0..i]
    }
}

impl fmt::Display for FilteredRelation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Relation::fmt(self, f)
    }
}

///
/// リレーションに対してカラム名と値を指定して '<' 比較します。
///
pub trait LessThan: Relation {
    fn less_than<Key: AsValue>(&self, col_id: &str, key: Key) -> FilteredRelation;
}

impl<T> LessThan for T
where
    T: Relation,
{
    fn less_than<Key: AsValue>(&self, col_name: &str, key: Key) -> FilteredRelation {
        let valid_row_ids = {
            let mut valid_row_ids = Vec::new();
            if let Some(col_id) = self.definition().name_to_id(col_name) {
                let column = self.column_at(col_id);
                if let Some(target_key_ids) = column.range_to(&key) {
                    let n_rows = self.num_rows();
                    valid_row_ids.reserve(n_rows);
                    const STEP: RowId = 64;
                    let mut start = 0;
                    let mut buffer = [0; STEP];
                    while start < n_rows {
                        let step = cmp::min(STEP, n_rows - start);
                        let end = start + step;
                        let row_ids = self.scan_row_ids(start..end, &mut buffer);
                        for row_id in row_ids {
                            if let Some(true) = target_key_ids.get(column.id_at(*row_id)) {
                                valid_row_ids.push(*row_id);
                            }
                        }
                        start += step;
                    }
                }
            }
            valid_row_ids.shrink_to_fit();
            valid_row_ids
        };
        FilteredRelation {
            relation: self,
            valid_row_ids: valid_row_ids,
        }
    }
}

///
/// リレーションに対してカラム名と値を指定して '==' 比較します。
///
pub trait EqualTo: Relation {
    fn equal_to<Key: AsValue>(&self, col_name: &str, key: Key) -> FilteredRelation;
}

impl<T> EqualTo for T
where
    T: Relation,
{
    fn equal_to<Key: AsValue>(&self, col_name: &str, key: Key) -> FilteredRelation {
        let valid_row_ids = {
            let mut valid_row_ids = Vec::new();
            if let Some(col_id) = self.definition().name_to_id(col_name) {
                let column = self.column_at(col_id);
                if let Some(target_key_id) = column.id_of(&key) {
                    let n_rows = self.num_rows();
                    valid_row_ids.reserve(n_rows);
                    const STEP: RowId = 64;
                    let mut start = 0;
                    let mut buffer = [0; STEP];
                    while start < n_rows {
                        let step = cmp::min(STEP, n_rows - start);
                        let end = start + step;
                        let row_ids = self.scan_row_ids(start..end, &mut buffer);
                        for row_id in row_ids {
                            if target_key_id == column.id_at(*row_id) {
                                valid_row_ids.push(*row_id);
                            }
                        }
                        start += step;
                    }
                }
            }
            valid_row_ids.shrink_to_fit();
            valid_row_ids
        };
        FilteredRelation {
            relation: self,
            valid_row_ids: valid_row_ids,
        }
    }
}

/// Count集計用の構造体です。
#[derive(Debug, Clone, Copy)]
pub struct Count {
    result: i32,
}

impl Count {
    fn calculate<T: AsValue>(&mut self, value: T) {
        if let Value::Null(_) = value.as_datum_ref() {
            return;
        }
        self.result += 1;
    }
    fn get_result(&self) -> i32 {
        self.result
    }
}

/// Average集計用の構造体です。
#[derive(Debug, Clone, Copy)]
pub struct Average {
    count: i32,
    sum: i32,
}

impl Average {
    fn calculate<T: AsValue>(&mut self, value: T) {
        if let Value::Integer(val) = value.as_datum_ref() {
            self.sum += val;
            self.count += 1;
        }
    }
    fn get_result(&self) -> i32 {
        if self.count == 0 {
            return 0;
        }
        self.sum / self.count
    }
}

/// 集計関数をまとめたEnumです。
#[derive(Debug, Clone, Copy)]
pub enum AggFunc {
    /// COUNT
    Count(Count),
    /// AVG
    Average(Average),
}

impl AggFunc {
    fn calculate<T: AsValue>(&mut self, value: T) {
        match self {
            AggFunc::Count(func) => func.calculate(value),
            AggFunc::Average(func) => func.calculate(value),
        }
    }
    fn get_result(&self) -> i32 {
        match self {
            AggFunc::Count(func) => func.get_result(),
            AggFunc::Average(func) => func.get_result(),
        }
    }
}

/// 集計パラメーターを表します。
#[derive(Debug, Clone)]
pub struct Agg {
    /// 対象カラム名
    name: &'static str,
    /// 集計関数
    func: AggFunc,
}

impl Agg {
    pub fn count(name: &'static str) -> Agg {
        Agg {
            name: name,
            func: AggFunc::Count(Count { result: 0 }),
        }
    }
    pub fn average(name: &'static str) -> Agg {
        Agg {
            name: name,
            func: AggFunc::Average(Average { sum: 0, count: 0 }),
        }
    }
}

///
/// リレーションに対して集計集約を行います。
///
pub trait GroupBy: Relation {
    fn group_by(&self, group_col_names: &[&str], agg_params: &[Agg]) -> Table;
}

impl<T> GroupBy for T
where
    T: Relation,
{
    fn group_by(&self, group_col_names: &[&str], agg_params: &[Agg]) -> Table {
        // 集約列IDリスト
        let mut group_col_ids = Vec::new();
        // TODO: 不正なカラム名の処置(Optionで返すように変更?)
        for group_col_name in group_col_names {
            if let Some(col_id) = self.definition().name_to_id(group_col_name) {
                group_col_ids.push(col_id);
            }
        }
        // イミュータブルに再束縛
        let group_col_ids = group_col_ids;
        // 集計列IDリスト
        let mut agg_col_ids = Vec::new();
        // 集計関数リスト
        let mut master_agg_funcs = Vec::new();
        // TODO: 不正なカラム名の処置(Optionで返すように変更?)
        for agg_param in agg_params {
            if let Some(col_id) = self.definition().name_to_id(agg_param.name) {
                agg_col_ids.push(col_id);
                master_agg_funcs.push(agg_param.func);
            }
        }
        let agg_col_ids = agg_col_ids;
        let master_agg_funcs = master_agg_funcs;
        let n_group_cols = group_col_ids.len();
        let n_agg_cols = agg_col_ids.len();
        let mut group_columns: Vec<&dyn AsColumn> = Vec::new();
        for col_id in &group_col_ids {
            group_columns.push(self.column_at(*col_id));
        }
        let group_columns = group_columns;
        let mut agg_columns: Vec<&dyn AsColumn> = Vec::new();
        for col_id in &agg_col_ids {
            agg_columns.push(self.column_at(*col_id));
        }
        let agg_columns = agg_columns;
        let mut group_map = HashMap::new();
        let n_rows = self.num_rows();
        const STEP: RowId = 64;
        let mut start = 0;
        let mut buffer = [0; STEP];
        while start < n_rows {
            let step = cmp::min(STEP, n_rows - start);
            let end = start + step;
            let row_ids = self.scan_row_ids(start..end, &mut buffer);
            for row_id in row_ids {
                let mut group_key_ids = Vec::new();
                group_key_ids.reserve(group_columns.len());
                for column in &group_columns {
                    group_key_ids.push(column.id_at(*row_id));
                }
                let group_key_ids = group_key_ids;
                let grouped_values = group_map
                    .entry(group_key_ids)
                    .or_insert(master_agg_funcs.clone());
                for agg_i in 0..n_agg_cols {
                    grouped_values[agg_i].calculate(agg_columns[agg_i].key_at(*row_id));
                }
            }
            start += step;
        }
        let mut attributes = Vec::new();
        for col_id in group_col_ids {
            attributes.push(self.definition()[col_id].clone());
        }
        for agg_i in 0..n_agg_cols {
            attributes.push(Attribute {
                name: match master_agg_funcs[agg_i] {
                    AggFunc::Count(_) => "count",
                    AggFunc::Average(_) => "average",
                }
                .to_string(),
                kind: TypeKind::Integer,
            });
        }
        let attributes = attributes;
        let mut table = Table::create(&self.definition().name(), &attributes);
        assert!(table.num_columns() == n_group_cols + n_agg_cols);
        let mut num_rows = 0;
        for (group_key_ids, grouped_values) in group_map.iter() {
            for group_i in 0..n_group_cols {
                let col_id = group_i;
                let key_id = group_key_ids[group_i];
                let datum = group_columns[group_i].key_of(key_id);
                table.columns[col_id].append(&datum);
            }
            for agg_i in 0..n_agg_cols {
                let col_id = agg_i + n_group_cols;
                let result = grouped_values[agg_i].get_result();
                table.columns[col_id].append(&result);
            }
            num_rows += 1;
        }
        table.num_rows = num_rows;
        table
    }
}

///
/// テーブルinser時のパラメーターを簡便にします。
///
/// # Examples
///
/// ```
/// # use kawaii::*;
/// # fn main() {
/// #     let mut table = Table::create(
/// #         "shohin",
/// #         attributes![
/// #             ("shohin_id", TypeKind::Integer),
/// #             ("shohin_name", TypeKind::Varchar),
/// #             ("kubun_id", TypeKind::Integer),
/// #             ("price", TypeKind::Integer)
/// #         ],
/// #     );
/// #     table
/// #         .insert(values!(1, "りんご", 1, 300))
/// #         .insert(values!(2, "みかん", 1, 130))
/// #         .insert(values!(3, "キャベツ", 2, 200))
/// #         .insert(values!(4, "さんま", 3, 220))
/// #         .insert(values!(5, "わかめ", NULL, 250)) //区分がNULL
/// #         .insert(values!(6, "しいたけ", 4, 180)) //該当区分なし
/// #         .insert(values!(7, "ドリアン", 1, NULL));
/// # }
/// ```
///
#[macro_export]
macro_rules! values {
    ( $( $x:expr ),* ) => ( &[ $( Value::from($x) ),* ] )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_varchar() {
        let mut dictionary = Dictionary::new();
        assert_eq!(dictionary.insert("Alice".to_string()), 0);
        assert_eq!(dictionary.insert("Bob".to_string()), 1);
        assert_eq!(dictionary.insert("Alice".to_string()), 0);
        assert_eq!(dictionary.num_keys(), 2);
        assert_eq!(dictionary.id_of("Alice"), Some(0));
        assert_eq!(dictionary.id_of("Bob"), Some(1));
        assert_eq!(dictionary.id_of("Chris"), None);
        let mut iter = dictionary.range("Alice".to_string().."Chris".to_string());
        assert_eq!(iter.next(), Some((&"Alice".to_string(), &0)));
        assert_eq!(iter.next(), Some((&"Bob".to_string(), &1)));
        assert_eq!(iter.next(), None);
        assert_eq!(dictionary.key_of(0), &"Alice".to_string());
        assert_eq!(dictionary.key_of(1), &"Bob".to_string());
    }

    #[test]
    fn test_column_varchar() {
        let mut column = Column::new();
        assert_eq!(column.append("Alice".to_string()), 0);
        assert_eq!(column.append("Bob".to_string()), 1);
        assert_eq!(column.append("Alice".to_string()), 0);
        assert_eq!(column.num_keys(), 2);
        assert_eq!(column.num_rows(), 3);
        assert_eq!(column.id_of("Alice"), Some(0));
        assert_eq!(column.id_of("Bob"), Some(1));
        assert_eq!(column.id_of("Chris"), None);
        let mut iter = column.range("Alice".to_string().."Chris".to_string());
        assert_eq!(iter.next(), Some((&"Alice".to_string(), &0)));
        assert_eq!(iter.next(), Some((&"Bob".to_string(), &1)));
        assert_eq!(iter.next(), None);
        assert_eq!(column.key_of(0), &"Alice".to_string());
        assert_eq!(column.key_of(1), &"Bob".to_string());
        assert_eq!(column.key_at(0), Some(&"Alice".to_string()));
        assert_eq!(column.key_at(1), Some(&"Bob".to_string()));
        assert_eq!(column.key_at(2), Some(&"Alice".to_string()));
    }

    #[test]
    fn test_dictionary_integer() {
        let mut dictionary = Dictionary::new();
        assert_eq!(dictionary.insert(10), 0);
        assert_eq!(dictionary.insert(20), 1);
        assert_eq!(dictionary.insert(10), 0);
        assert_eq!(dictionary.num_keys(), 2);
        assert_eq!(dictionary.id_of(&10), Some(0));
        assert_eq!(dictionary.id_of(&20), Some(1));
        assert_eq!(dictionary.id_of(&30), None);
        let mut iter = dictionary.range(10..30);
        assert_eq!(iter.next(), Some((&10, &0)));
        assert_eq!(iter.next(), Some((&20, &1)));
        assert_eq!(iter.next(), None);
        assert_eq!(dictionary.key_of(0), &10);
        assert_eq!(dictionary.key_of(1), &20);
    }

    #[test]
    fn test_column_integer() {
        let mut column = Column::new();
        assert_eq!(column.append(10), 0);
        assert_eq!(column.append(20), 1);
        assert_eq!(column.append(10), 0);
        assert_eq!(column.num_keys(), 2);
        assert_eq!(column.num_rows(), 3);
        assert_eq!(column.id_of(&10), Some(0));
        assert_eq!(column.id_of(&20), Some(1));
        assert_eq!(column.id_of(&30), None);
        let mut iter = column.range(10..30);
        assert_eq!(iter.next(), Some((&10, &0)));
        assert_eq!(iter.next(), Some((&20, &1)));
        assert_eq!(iter.next(), None);
        assert_eq!(column.key_of(0), &10);
        assert_eq!(column.key_of(1), &20);
        assert_eq!(column.key_at(0), Some(&10));
        assert_eq!(column.key_at(1), Some(&20));
        assert_eq!(column.key_at(2), Some(&10));
    }

    fn create_shohin_table() -> Table {
        let mut table = Table::create(
            "shohin",
            attributes![
                ("shohin_id", TypeKind::Integer),
                ("shohin_name", TypeKind::Varchar),
                ("kubun_id", TypeKind::Integer),
                ("price", TypeKind::Integer)
            ],
        );
        table
            .insert(values!(1, "りんご", 1, 300))
            .insert(values!(2, "みかん", 1, 130))
            .insert(values!(3, "キャベツ", 2, 200))
            .insert(values!(4, "さんま", 3, 220))
            .insert(values!(5, "わかめ", NULL, 250)) //区分がNULL
            .insert(values!(6, "しいたけ", 4, 180)) //該当区分なし
            .insert(values!(7, "ドリアン", 1, NULL));
        table
    }

    fn create_kubun_table() -> Table {
        let mut table = Table::create(
            "kubun",
            attributes![
                ("kubun_id", TypeKind::Integer),
                ("kubun_name", TypeKind::Varchar)
            ],
        );
        table
            .insert(values!(1, "くだもの"))
            .insert(values!(2, "野菜"))
            .insert(values!(3, "魚"));
        table
    }

    #[test]
    fn test_table() {
        let shohin1 = create_shohin_table();
        let shohin2 = create_shohin_table();
        assert_eq!(shohin1, shohin2);
        let kubun1 = create_kubun_table();
        let kubun2 = create_kubun_table();
        assert_eq!(kubun1, kubun2);
    }

    #[test]
    fn test_select() {
        let shohin = create_shohin_table();
        let actual = shohin.select(&["shohin_id", "shohin_name"]);
        let mut expected = Table::create(
            "shohin",
            attributes![
                ("shohin_id", TypeKind::Integer),
                ("shohin_name", TypeKind::Varchar)
            ],
        );
        expected
            .insert(values!(1, "りんご"))
            .insert(values!(2, "みかん"))
            .insert(values!(3, "キャベツ"))
            .insert(values!(4, "さんま"))
            .insert(values!(5, "わかめ")) //区分がNULL
            .insert(values!(6, "しいたけ")) //該当区分なし
            .insert(values!(7, "ドリアン"));
        assert_eq!(actual.fetch(0..10), expected.fetch(0..10));
    }

    #[test]
    fn test_less_than() {
        let shohin = create_shohin_table();
        let actual = shohin.less_than("shohin_id", 4);
        let mut expected = Table::create(
            "shohin",
            attributes![
                ("shohin_id", TypeKind::Integer),
                ("shohin_name", TypeKind::Varchar),
                ("kubun_id", TypeKind::Integer),
                ("price", TypeKind::Integer)
            ],
        );
        expected
            .insert(values!(1, "りんご", 1, 300))
            .insert(values!(2, "みかん", 1, 130))
            .insert(values!(3, "キャベツ", 2, 200));
        assert_eq!(actual.fetch(0..10), expected.fetch(0..10));
    }

    #[test]
    fn test_equal_to() {
        let shohin = create_shohin_table();
        let actual = shohin.equal_to("shohin_id", 4);
        let mut expected = Table::create(
            "shohin",
            attributes![
                ("shohin_id", TypeKind::Integer),
                ("shohin_name", TypeKind::Varchar),
                ("kubun_id", TypeKind::Integer),
                ("price", TypeKind::Integer)
            ],
        );
        expected.insert(values!(4, "さんま", 3, 220));
        assert_eq!(actual.fetch(0..10), expected.fetch(0..10));
    }

    #[test]
    fn test_group_by() {
        let shohin = create_shohin_table();
        let actual = shohin.group_by(
            &["kubun_id"],
            &[Agg::count("shohin_name"), Agg::average("price")],
        );
        let mut expected = Table::create(
            "shohin",
            attributes![
                ("kubun_id", TypeKind::Integer),
                ("count", TypeKind::Integer),
                ("average", TypeKind::Integer)
            ],
        );
        expected
            .insert(values!(NULL, 1, 250))
            .insert(values!(1, 3, 215))
            .insert(values!(2, 1, 200))
            .insert(values!(3, 1, 220))
            .insert(values!(4, 1, 180));
        assert_eq!(
            actual.fetch(0..10).map(|mut t| {
                t.sort();
                t
            }),
            expected.fetch(0..10).map(|mut t| {
                t.sort();
                t
            })
        );
    }
}
