//! ULMEN V1 — Ultra Lightweight Minimal Encoding Notation
//! Rust acceleration layer (PyO3)
//!
//! Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
//!
//! This file is part of ULMEN.
//! ULMEN is licensed under the Business Source License 1.1.
//! See the LICENSE file in the project root for full license information.
//! Proprietary and confidential.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::manual_div_ceil)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::useless_conversion)]
#![allow(clippy::manual_strip)]
#![allow(clippy::nonminimal_bool)]
#![allow(clippy::wildcard_in_or_patterns)]
#![allow(clippy::needless_borrow)]

use flate2::write::ZlibEncoder;
use flate2::Compression;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyDict, PyFloat, PyInt, PyIterator, PyList, PyString};
use pyo3::IntoPy;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::Write as IoWrite;

// ---------------------------------------------------------------------------
// FxHash — fast non-cryptographic hash
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FxHasher(u64);

impl Hasher for FxHasher {
    #[inline(always)]
    fn write(&mut self, b: &[u8]) {
        for c in b.chunks(8) {
            let mut buf = [0u8; 8];
            buf[..c.len()].copy_from_slice(c);
            self.0 =
                self.0.rotate_left(5).wrapping_mul(0x517cc1b727220a95) ^ u64::from_le_bytes(buf);
        }
    }
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
}

type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[inline(always)]
fn fx_map<K, V>(cap: usize) -> FxHashMap<K, V> {
    HashMap::with_capacity_and_hasher(cap, BuildHasherDefault::default())
}

// ---------------------------------------------------------------------------
// Wire-format constants — byte-identical to ulmen/core/_constants.py
// ---------------------------------------------------------------------------

const T_STR_TINY: u8 = 0x01;
const T_STR: u8 = 0x02;
const T_INT: u8 = 0x03;
const T_FLOAT: u8 = 0x04;
const T_BOOL: u8 = 0x05;
const T_NULL: u8 = 0x06;
const T_LIST: u8 = 0x07;
const T_MAP: u8 = 0x08;
const T_POOL_DEF: u8 = 0x09;
const T_POOL_REF: u8 = 0x0A;
const T_MATRIX: u8 = 0x0B;
const T_DELTA_RAW: u8 = 0x0C;
const T_BITS: u8 = 0x0E;
const T_RLE: u8 = 0x0F;

const S_RAW: u8 = 0x00;
const S_DELTA: u8 = 0x01;
const S_RLE: u8 = 0x02;
const S_BITS: u8 = 0x03;
const S_POOL: u8 = 0x04;

const MAGIC: &[u8] = b"LUMB";
const VER: &[u8] = &[3, 3];

// ---------------------------------------------------------------------------
// Internal column value — avoids heap allocation per cell
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum ColVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64), // stored as bits to keep Copy
    Str(u32),   // index into StringTable
}

impl ColVal {
    #[inline(always)]
    fn as_f64(self) -> f64 {
        if let Self::Float(b) = self {
            f64::from_bits(b)
        } else {
            0.0
        }
    }
    #[inline(always)]
    fn from_f64(f: f64) -> Self {
        Self::Float(f.to_bits())
    }
}

// ---------------------------------------------------------------------------
// String interning table
// ---------------------------------------------------------------------------

struct StringTable {
    strings: Vec<String>,
    index: FxHashMap<String, u32>,
}

impl StringTable {
    fn new(cap: usize) -> Self {
        Self {
            strings: Vec::with_capacity(cap),
            index: fx_map(cap),
        }
    }

    #[inline(always)]
    fn intern(&mut self, s: String) -> u32 {
        if let Some(&i) = self.index.get(s.as_str()) {
            return i;
        }
        let i = self.strings.len() as u32;
        self.index.insert(s.clone(), i);
        self.strings.push(s);
        i
    }

    #[inline(always)]
    fn intern_str(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        self.intern(s.to_owned())
    }

    #[inline(always)]
    fn get(&self, i: u32) -> &str {
        &self.strings[i as usize]
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.strings.len()
    }
}

// ---------------------------------------------------------------------------
// Varint / zigzag encoding
// ---------------------------------------------------------------------------

#[inline(always)]
fn varint(mut n: u64, o: &mut Vec<u8>) {
    while n >= 0x80 {
        o.push((n as u8) | 0x80);
        n >>= 7;
    }
    o.push(n as u8);
}

#[inline(always)]
fn zigzag(n: i64, o: &mut Vec<u8>) {
    varint(((n << 1) ^ (n >> 63)) as u64, o);
}

#[inline(always)]
fn zz_cost(n: i64) -> u32 {
    let z = ((n << 1) ^ (n >> 63)) as u64;
    if z == 0 {
        return 1;
    }
    let bits = 64 - z.leading_zeros();
    bits.div_ceil(7)
}

// ---------------------------------------------------------------------------
// Scalar emitters
// ---------------------------------------------------------------------------

#[inline(always)]
fn pack_str(b: &[u8], o: &mut Vec<u8>) {
    let l = b.len();
    if l <= 3 {
        o.push(T_STR_TINY);
        o.push(l as u8);
    } else {
        o.push(T_STR);
        varint(l as u64, o);
    }
    o.extend_from_slice(b);
}

#[inline(always)]
fn emit_null(o: &mut Vec<u8>) {
    o.push(T_NULL);
}
#[inline(always)]
fn emit_bool(v: bool, o: &mut Vec<u8>) {
    o.push(T_BOOL);
    o.push(v as u8);
}
#[inline(always)]
fn emit_int(v: i64, o: &mut Vec<u8>) {
    o.push(T_INT);
    zigzag(v, o);
}
#[inline(always)]
fn emit_pref(v: u32, o: &mut Vec<u8>) {
    o.push(T_POOL_REF);
    varint(v as u64, o);
}
#[inline(always)]
fn emit_float(v: f64, o: &mut Vec<u8>) {
    o.push(T_FLOAT);
    o.extend_from_slice(&v.to_bits().to_be_bytes());
}

#[inline(always)]
fn cv_eq(a: ColVal, b: ColVal) -> bool {
    match (a, b) {
        (ColVal::Null, ColVal::Null) => true,
        (ColVal::Bool(x), ColVal::Bool(y)) => x == y,
        (ColVal::Int(x), ColVal::Int(y)) => x == y,
        (ColVal::Float(x), ColVal::Float(y)) => x == y,
        (ColVal::Str(x), ColVal::Str(y)) => x == y,
        _ => false,
    }
}

#[inline(always)]
fn emit_val(v: ColVal, st: &StringTable, pi: &[Option<u32>], o: &mut Vec<u8>) {
    match v {
        ColVal::Null => emit_null(o),
        ColVal::Bool(b) => emit_bool(b, o),
        ColVal::Int(n) => emit_int(n, o),
        ColVal::Float(_) => emit_float(v.as_f64(), o),
        ColVal::Str(si) => {
            if (si as usize) < pi.len() {
                if let Some(p) = pi[si as usize] {
                    emit_pref(p, o);
                    return;
                }
            }
            pack_str(st.get(si).as_bytes(), o);
        }
    }
}

// ---------------------------------------------------------------------------
// Text format helpers
// ---------------------------------------------------------------------------

#[inline(always)]
fn escape_into(s: &str, out: &mut String) {
    if !s
        .bytes()
        .any(|b| matches!(b, b'\\' | b'\t' | b'\n' | b'\r'))
    {
        out.push_str(s);
        return;
    }
    out.reserve(s.len() + 8);
    for &b in s.as_bytes() {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'\t' => out.push_str("\\t"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            _ => out.push(b as char),
        }
    }
}

#[inline(always)]
fn fmt_val(v: ColVal, st: &StringTable, pi: &[Option<u32>], o: &mut String) {
    match v {
        ColVal::Null => o.push('N'),
        ColVal::Bool(true) => o.push('T'),
        ColVal::Bool(false) => o.push('F'),
        ColVal::Int(n) => {
            let _ = write!(o, "{}", n);
        }
        ColVal::Float(_) => {
            let f = v.as_f64();
            if f.is_nan() {
                o.push_str("nan");
                return;
            }
            if f.is_infinite() {
                o.push_str(if f > 0.0 { "inf" } else { "-inf" });
                return;
            }
            let _ = write!(o, "{:?}", f);
        }
        ColVal::Str(si) => {
            let s = st.get(si);
            let siu = si as usize;
            if s.is_empty() {
                o.push_str("$0=");
                return;
            }
            if siu < pi.len() {
                if let Some(p) = pi[siu] {
                    if p <= 9 {
                        let _ = write!(o, "#{}", p);
                    } else {
                        let _ = write!(o, "#{{{}}}", p);
                    }
                    return;
                }
            }
            escape_into(s, o);
        }
    }
}

// ---------------------------------------------------------------------------
// Strategy detection
// ---------------------------------------------------------------------------

#[inline(always)]
fn detect_strat(vals: &[ColVal]) -> u8 {
    if vals.is_empty() {
        return S_RAW;
    }
    let mut all_bool = true;
    let mut all_int = true;
    let mut nn = 0usize;
    for &v in vals {
        match v {
            ColVal::Null => {
                all_bool = false;
                all_int = false;
            }
            ColVal::Bool(_) => {
                all_int = false;
                nn += 1;
            }
            ColVal::Int(_) => {
                all_bool = false;
                nn += 1;
            }
            _ => {
                all_bool = false;
                all_int = false;
                nn += 1;
            }
        }
    }
    if nn == 0 {
        return S_RLE;
    }
    if all_bool {
        return S_BITS;
    }
    if all_int {
        let mut prev = 0i64;
        let mut raw_cost = 0u32;
        let mut delta_cost = 0u32;
        let mut first = true;
        for &v in vals {
            if let ColVal::Int(n) = v {
                raw_cost += zz_cost(n);
                delta_cost += if first {
                    first = false;
                    zz_cost(n)
                } else {
                    zz_cost(n.wrapping_sub(prev))
                };
                prev = n;
            }
        }
        return if delta_cost < raw_cost {
            S_DELTA
        } else {
            S_RAW
        };
    }
    if nn > 4 {
        let threshold = std::cmp::max(8, nn / 10);
        let mut small = [u32::MAX; 32];
        let mut u = 0usize;
        let mut overflow = false;
        'outer: for &v in vals {
            if let ColVal::Str(si) = v {
                if small[..u].contains(&si) {
                    continue 'outer;
                }
                if u < 32 {
                    small[u] = si;
                    u += 1;
                } else {
                    overflow = true;
                    break;
                }
            }
        }
        let final_u = if overflow {
            let mut seen: FxHashMap<u32, ()> = fx_map(64);
            let mut u2 = 0;
            for &v in vals {
                if let ColVal::Str(si) = v {
                    if seen.insert(si, ()).is_none() {
                        u2 += 1;
                    }
                }
            }
            u2
        } else {
            u
        };
        if final_u > 0 && final_u <= threshold {
            return S_POOL;
        }
    }
    let mut runs = 1usize;
    for i in 1..vals.len() {
        if !cv_eq(vals[i], vals[i - 1]) {
            runs += 1;
        }
    }
    if runs * 10 < vals.len() * 6 {
        return S_RLE;
    }
    S_RAW
}

#[inline(always)]
fn col_type_char(v: &[ColVal]) -> char {
    let (mut b, mut i, mut f, mut s) = (false, false, false, false);
    for &x in v {
        match x {
            ColVal::Bool(_) => b = true,
            ColVal::Int(_) => i = true,
            ColVal::Float(_) => f = true,
            ColVal::Str(_) => s = true,
            ColVal::Null => {}
        }
    }
    match (b, i, f, s) {
        (false, false, false, false) => 'n',
        (true, false, false, false) => 'b',
        (false, true, false, false) => 'd',
        (false, false, true, false) => 'f',
        _ => 's',
    }
}

// ---------------------------------------------------------------------------
// Column body encoders
// ---------------------------------------------------------------------------

#[inline(always)]
fn pack_bits_col(v: &[ColVal], o: &mut Vec<u8>) {
    let n = v.len();
    o.push(T_BITS);
    varint(n as u64, o);
    let start = o.len();
    o.resize(start + ((n + 7) >> 3), 0);
    for (i, &x) in v.iter().enumerate() {
        if matches!(x, ColVal::Bool(true)) {
            o[start + (i >> 3)] |= 1 << (i & 7);
        }
    }
}

#[inline(always)]
fn pack_delta_col(v: &[ColVal], o: &mut Vec<u8>) {
    o.push(T_DELTA_RAW);
    varint(v.len() as u64, o);
    let (mut prev, mut first) = (0i64, true);
    for &x in v {
        if let ColVal::Int(n) = x {
            zigzag(
                if first {
                    first = false;
                    n
                } else {
                    n.wrapping_sub(prev)
                },
                o,
            );
            prev = n;
        }
    }
}

#[inline(always)]
fn pack_rle_col(v: &[ColVal], st: &StringTable, pi: &[Option<u32>], o: &mut Vec<u8>) {
    if v.is_empty() {
        o.push(T_RLE);
        varint(0, o);
        return;
    }
    let mut runs = 1;
    for i in 1..v.len() {
        if !cv_eq(v[i], v[i - 1]) {
            runs += 1;
        }
    }
    o.push(T_RLE);
    varint(runs as u64, o);
    let mut s = 0;
    while s < v.len() {
        let mut e = s + 1;
        while e < v.len() && cv_eq(v[e], v[s]) {
            e += 1;
        }
        emit_val(v[s], st, pi, o);
        varint((e - s) as u64, o);
        s = e;
    }
}

#[inline(always)]
fn pack_raw_col(v: &[ColVal], st: &StringTable, pi: &[Option<u32>], o: &mut Vec<u8>) {
    o.push(T_LIST);
    varint(v.len() as u64, o);
    for &x in v {
        emit_val(x, st, pi, o);
    }
}

// ---------------------------------------------------------------------------
// Python value extraction
// ---------------------------------------------------------------------------

#[inline(always)]
fn extract_val(v: &Bound<'_, PyAny>, st: &mut StringTable) -> PyResult<ColVal> {
    if v.is_none() {
        return Ok(ColVal::Null);
    }
    if v.is_instance_of::<PyBool>() {
        return Ok(ColVal::Bool(v.downcast::<PyBool>()?.is_true()));
    }
    if v.is_instance_of::<PyInt>() {
        if let Ok(n) = v.extract::<i64>() {
            return Ok(ColVal::Int(n));
        }
    }
    if v.is_instance_of::<PyFloat>() {
        if let Ok(f) = v.extract::<f64>() {
            return Ok(ColVal::from_f64(f));
        }
    }
    if v.is_instance_of::<PyString>() {
        let ps = v.downcast::<PyString>()?;
        return match ps.to_str() {
            Ok(s) => Ok(ColVal::Str(st.intern_str(s))),
            Err(_) => {
                let s: String = v.extract()?;
                Ok(ColVal::Str(st.intern(s)))
            }
        };
    }
    if let Ok(n) = v.extract::<i64>() {
        return Ok(ColVal::Int(n));
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(ColVal::from_f64(f));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(ColVal::Str(st.intern(s)));
    }
    Ok(ColVal::Null)
}

fn scan_val(v: &Bound<'_, PyAny>, st: &mut StringTable) -> PyResult<()> {
    if let Ok(s) = v.extract::<String>() {
        if s.len() > 1 {
            st.intern(s);
        }
        return Ok(());
    }
    if let Ok(d) = v.downcast::<PyDict>() {
        for (k, val) in d.iter() {
            if let Ok(ks) = k.extract::<String>() {
                if ks.len() > 1 {
                    st.intern(ks);
                }
            }
            scan_val(&val, st)?;
        }
        return Ok(());
    }
    if let Ok(l) = v.downcast::<PyList>() {
        for item in l.iter() {
            scan_val(&item, st)?;
        }
    }
    Ok(())
}

fn extract_cols(lst: &Bound<'_, PyList>) -> PyResult<(Vec<Vec<ColVal>>, Vec<u32>, StringTable)> {
    let nr = lst.len();
    if nr == 0 {
        return Ok((Vec::new(), Vec::new(), StringTable::new(0)));
    }
    let first = lst.get_item(0)?;
    let fd = first.downcast::<PyDict>()?;
    let nc = fd.len();
    let mut st = StringTable::new(nc * 4 + nr.min(1024) + 32);

    let mut keys = Vec::with_capacity(nc);
    for (k, v) in fd.iter() {
        let ks: String = k.extract()?;
        keys.push(st.intern(ks));
        scan_val(&v, &mut st)?;
    }

    let mut cd: Vec<Vec<ColVal>> = (0..nc).map(|_| Vec::with_capacity(nr)).collect();
    let mut buf = Vec::with_capacity(nc);

    for row in lst.iter() {
        let d = row.downcast::<PyDict>()?;
        buf.clear();
        let mut ok = d.len() == nc;
        if ok {
            for (ci, (k, v)) in d.iter().enumerate() {
                let keq = if let Ok(ps) = k.downcast::<PyString>() {
                    ps.to_str().map(|s| s == st.get(keys[ci])).unwrap_or(false)
                } else {
                    k.extract::<String>()
                        .map(|s| s == st.get(keys[ci]))
                        .unwrap_or(false)
                };
                if !keq {
                    ok = false;
                    break;
                }
                buf.push(extract_val(&v, &mut st)?);
            }
        }
        if ok && buf.len() == nc {
            for ci in 0..nc {
                cd[ci].push(buf[ci]);
            }
        } else {
            for (ci, &ki) in keys.iter().enumerate() {
                let cv = match d.get_item(st.get(ki))? {
                    None => ColVal::Null,
                    Some(v) => extract_val(&v, &mut st)?,
                };
                cd[ci].push(cv);
            }
        }
    }
    Ok((cd, keys, st))
}

// ---------------------------------------------------------------------------
// Pool builder
// ---------------------------------------------------------------------------

fn build_pool_inner(
    cd: &[Vec<ColVal>],
    keys: &[u32],
    st: &StringTable,
    nr: usize,
    max: usize,
) -> (Vec<u32>, Vec<Option<u32>>) {
    let sl = st.len();
    if sl == 0 {
        return (Vec::new(), Vec::new());
    }
    let mut freq = vec![0u32; sl];

    for &ki in keys {
        if st.get(ki).len() > 1 {
            freq[ki as usize] = freq[ki as usize].saturating_add(nr as u32);
        }
    }
    for col in cd {
        for &v in col {
            if let ColVal::Str(si) = v {
                if st.get(si).len() > 1 {
                    freq[si as usize] += 1;
                }
            }
        }
    }

    let nz = freq.iter().filter(|&&x| x > 0).count();
    let ref_cost = if nz <= 9 { 2i32 } else { 4i32 };

    let mut cands: Vec<(u32, i32)> = (0..sl as u32)
        .filter(|&i| freq[i as usize] >= 2)
        .filter_map(|i| {
            let score = freq[i as usize] as i32 * (st.get(i).len() as i32 - ref_cost);
            if score > 0 {
                Some((i, score))
            } else {
                None
            }
        })
        .collect();

    cands.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    cands.truncate(max);

    let pool: Vec<u32> = cands.iter().map(|&(i, _)| i).collect();
    let mut pi = vec![None; sl];
    for (pos, &si) in pool.iter().enumerate() {
        pi[si as usize] = Some(pos as u32);
    }
    (pool, pi)
}

// ---------------------------------------------------------------------------
// Header builders
// ---------------------------------------------------------------------------

fn build_pool_line(p: &[u32], st: &StringTable) -> String {
    if p.is_empty() {
        return String::new();
    }
    let mut o = String::with_capacity(p.iter().map(|&si| st.get(si).len() + 2).sum::<usize>() + 6);
    o.push_str("POOL:");
    for (i, &si) in p.iter().enumerate() {
        if i > 0 {
            o.push(',');
        }
        escape_into(st.get(si), &mut o);
    }
    o
}

fn build_schema_header(k: &[u32], cd: &[Vec<ColVal>], st: &StringTable) -> String {
    let mut o = String::with_capacity(k.len() * 12 + 8);
    o.push_str("SCHEMA:");
    for (ci, &ki) in k.iter().enumerate() {
        if ci > 0 {
            o.push(',');
        }
        o.push_str(st.get(ki));
        o.push(':');
        o.push(if cd.is_empty() {
            'n'
        } else {
            col_type_char(&cd[ci])
        });
    }
    o
}

fn build_matrix_header(k: &[u32], ct: &[char], st: &StringTable, nr: usize) -> String {
    let mut o = String::with_capacity(k.len() * 14 + 24);
    let _ = write!(o, "records[{}]:", nr);
    for (ci, &ki) in k.iter().enumerate() {
        if ci > 0 {
            o.push(',');
        }
        o.push_str(st.get(ki));
        o.push(':');
        o.push(ct[ci]);
    }
    o
}

fn build_binary_pool_block(p: &[u32], st: &StringTable) -> Vec<u8> {
    if p.is_empty() {
        return Vec::new();
    }
    let mut o = Vec::with_capacity(1 + 5 + p.iter().map(|&si| st.get(si).len() + 3).sum::<usize>());
    o.push(T_POOL_DEF);
    varint(p.len() as u64, &mut o);
    for &si in p {
        pack_str(st.get(si).as_bytes(), &mut o);
    }
    o
}

#[inline(always)]
fn zlib_compress(d: &[u8], level: u32) -> Vec<u8> {
    let mut e = ZlibEncoder::new(
        Vec::with_capacity(d.len() / 2 + 64),
        Compression::new(level),
    );
    e.write_all(d).unwrap();
    e.finish().unwrap()
}

// ---------------------------------------------------------------------------
// UlmenCore — all precomputed encodings
// ---------------------------------------------------------------------------

struct UlmenCore {
    cd: Vec<Vec<ColVal>>,
    keys: Vec<u32>,
    st: StringTable,
    pool: Vec<u32>,
    pi: Vec<Option<u32>>,
    n_rows: usize,
    col_types: Vec<char>,
    col_strats: Vec<u8>,
    inline_cols: Vec<bool>,
    row_cols: Vec<usize>,
    pool_line: String,
    schema_header: String,
    matrix_header: String,
    inline_text_blocks: Vec<String>,
    row_text_lines: Vec<String>,
    binary_pool_block: Vec<u8>,
    col_bodies_strat: Vec<Vec<u8>>,
    col_bodies_raw: Vec<Vec<u8>>,
    cached_text: String,
    cached_binary_strat: Vec<u8>,
    cached_binary_raw: Vec<u8>,
    cached_binary_zlib: Option<Vec<u8>>,
    zlib_level: u32,
}

impl UlmenCore {
    fn build(lst: &Bound<'_, PyList>, max_pool: usize, zlib_level: u32) -> PyResult<Self> {
        let nr = lst.len();
        if nr == 0 {
            return Ok(Self::empty(zlib_level));
        }
        let (cd, keys, st) = extract_cols(lst)?;
        let (pool, pi) = build_pool_inner(&cd, &keys, &st, nr, max_pool);
        let nc = keys.len();

        let mut col_types = Vec::with_capacity(nc);
        let mut col_strats = Vec::with_capacity(nc);
        for col in &cd {
            col_types.push(col_type_char(col));
            col_strats.push(detect_strat(col));
        }

        let (inline_cols, row_cols) = if nr <= 1 {
            (vec![false; nc], (0..nc).collect())
        } else {
            let ic: Vec<bool> = col_strats
                .iter()
                .map(|&s| s == S_RLE || s == S_POOL)
                .collect();
            let rc: Vec<usize> = (0..nc).filter(|&i| !ic[i]).collect();
            (ic, rc)
        };

        let pool_line = build_pool_line(&pool, &st);
        let schema_header = build_schema_header(&keys, &cd, &st);
        let matrix_header = build_matrix_header(&keys, &col_types, &st, nr);
        let binary_pool_block = build_binary_pool_block(&pool, &st);

        let col_bodies_strat: Vec<Vec<u8>> = cd
            .iter()
            .enumerate()
            .map(|(ci, v)| {
                let mut b = Vec::with_capacity(v.len() * 4 + 8);
                match col_strats[ci] {
                    S_BITS => pack_bits_col(v, &mut b),
                    S_DELTA => pack_delta_col(v, &mut b),
                    S_RLE => pack_rle_col(v, &st, &pi, &mut b),
                    _ => pack_raw_col(v, &st, &pi, &mut b),
                }
                b
            })
            .collect();

        let col_bodies_raw: Vec<Vec<u8>> = cd
            .iter()
            .map(|v| {
                let mut b = Vec::with_capacity(v.len() * 4 + 8);
                pack_raw_col(v, &st, &pi, &mut b);
                b
            })
            .collect();

        let inline_text_blocks: Vec<String> = keys
            .iter()
            .enumerate()
            .map(|(ci, &ki)| {
                if !inline_cols[ci] {
                    return String::new();
                }
                let mut s = String::with_capacity(cd[ci].len() * 6 + st.get(ki).len() + 4);
                s.push('@');
                s.push_str(st.get(ki));
                s.push('=');
                for (ri, &v) in cd[ci].iter().enumerate() {
                    if ri > 0 {
                        s.push(';');
                    }
                    fmt_val(v, &st, &pi, &mut s);
                }
                s
            })
            .collect();

        let row_text_lines: Vec<String> = (0..nr)
            .map(|ri| {
                let mut s = String::with_capacity(row_cols.len() * 8);
                for (j, &ci) in row_cols.iter().enumerate() {
                    if j > 0 {
                        s.push('\t');
                    }
                    fmt_val(cd[ci][ri], &st, &pi, &mut s);
                }
                s
            })
            .collect();

        let cached_text = Self::assemble_text(
            nr,
            &pool_line,
            &schema_header,
            &matrix_header,
            &inline_cols,
            &inline_text_blocks,
            &row_cols,
            &row_text_lines,
        );
        // For nr==1, assemble_binary uses b[2..] to strip the T_LIST wrapper,
        // which only works with raw bodies. Strat bodies (T_BITS/T_DELTA/T_RLE)
        // have a different layout. Use raw bodies when nr==1.
        let cached_binary_strat = Self::assemble_binary(
            nr,
            nc,
            &keys,
            &st,
            &col_strats,
            &binary_pool_block,
            if nr == 1 {
                &col_bodies_raw
            } else {
                &col_bodies_strat
            },
            true,
        );
        let cached_binary_raw = Self::assemble_binary(
            nr,
            nc,
            &keys,
            &st,
            &col_strats,
            &binary_pool_block,
            &col_bodies_raw,
            false,
        );

        Ok(Self {
            cd,
            keys,
            st,
            pool,
            pi,
            n_rows: nr,
            col_types,
            col_strats,
            inline_cols,
            row_cols,
            pool_line,
            schema_header,
            matrix_header,
            inline_text_blocks,
            row_text_lines,
            binary_pool_block,
            col_bodies_strat,
            col_bodies_raw,
            cached_text,
            cached_binary_strat,
            cached_binary_raw,
            cached_binary_zlib: None,
            zlib_level,
        })
    }

    fn empty(zlib_level: u32) -> Self {
        let base = [MAGIC, VER].concat();
        Self {
            cd: vec![],
            keys: vec![],
            st: StringTable::new(0),
            pool: vec![],
            pi: vec![],
            n_rows: 0,
            col_types: vec![],
            col_strats: vec![],
            inline_cols: vec![],
            row_cols: vec![],
            pool_line: String::new(),
            schema_header: String::new(),
            matrix_header: String::new(),
            inline_text_blocks: vec![],
            row_text_lines: vec![],
            binary_pool_block: vec![],
            col_bodies_strat: vec![],
            col_bodies_raw: vec![],
            cached_text: String::new(),
            cached_binary_strat: base.clone(),
            cached_binary_raw: base,
            cached_binary_zlib: None,
            zlib_level,
        }
    }

    fn get_zlib(&mut self) -> &[u8] {
        if self.cached_binary_zlib.is_none() {
            self.cached_binary_zlib =
                Some(zlib_compress(&self.cached_binary_strat, self.zlib_level));
        }
        self.cached_binary_zlib.as_ref().unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble_text(
        nr: usize,
        pl: &str,
        sh: &str,
        mh: &str,
        ic: &[bool],
        itb: &[String],
        rc: &[usize],
        rtl: &[String],
    ) -> String {
        if nr == 0 {
            return String::new();
        }
        let mut o = String::with_capacity(
            pl.len()
                + mh.len()
                + sh.len()
                + itb.iter().map(|s| s.len() + 1).sum::<usize>()
                + rtl.iter().map(|s| s.len() + 1).sum::<usize>()
                + 16,
        );
        if !pl.is_empty() {
            o.push_str(pl);
            o.push('\n');
        }
        if nr == 1 {
            o.push_str(sh);
            o.push('\n');
            if !rtl.is_empty() {
                o.push_str(&rtl[0]);
            }
            return o;
        }
        o.push_str(mh);
        o.push('\n');
        for (ci, blk) in itb.iter().enumerate() {
            if ic[ci] {
                o.push_str(blk);
                o.push('\n');
            }
        }
        if !rc.is_empty() {
            for (ri, line) in rtl.iter().enumerate() {
                o.push_str(line);
                if ri + 1 < nr {
                    o.push('\n');
                }
            }
        } else if o.ends_with('\n') {
            o.pop();
        }
        o
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble_binary(
        nr: usize,
        nc: usize,
        keys: &[u32],
        st: &StringTable,
        cs: &[u8],
        bpb: &[u8],
        cbs: &[Vec<u8>],
        use_strat: bool,
    ) -> Vec<u8> {
        let mut o = Vec::with_capacity(
            6 + bpb.len() + 10 + nc * 12 + cbs.iter().map(|b| b.len()).sum::<usize>(),
        );
        o.extend_from_slice(MAGIC);
        o.extend_from_slice(VER);
        o.extend_from_slice(bpb);
        if nr == 0 {
            return o;
        }
        if nr > 1 {
            o.push(T_MATRIX);
            varint(nr as u64, &mut o);
            varint(nc as u64, &mut o);
            for (ci, &ki) in keys.iter().enumerate() {
                pack_str(st.get(ki).as_bytes(), &mut o);
                o.push(if use_strat { cs[ci] } else { S_RAW });
            }
            for b in cbs {
                o.extend_from_slice(b);
            }
        } else {
            o.push(T_LIST);
            varint(1, &mut o);
            o.push(T_MAP);
            varint(nc as u64, &mut o);
            for (ci, b) in cbs.iter().enumerate() {
                pack_str(st.get(keys[ci]).as_bytes(), &mut o);
                if b.len() > 2 {
                    o.extend_from_slice(&b[2..]);
                } else {
                    emit_null(&mut o);
                }
            }
        }
        o
    }

    #[inline(always)]
    fn text_cached(&self) -> &str {
        &self.cached_text
    }
    #[inline(always)]
    fn binary_strat_cached(&self) -> &[u8] {
        &self.cached_binary_strat
    }
    #[inline(always)]
    fn binary_raw_cached(&self) -> &[u8] {
        &self.cached_binary_raw
    }
}

// ---------------------------------------------------------------------------
// Python-exposed classes
// ---------------------------------------------------------------------------

#[pyclass]
struct UlmenDictRust {
    core: UlmenCore,
    opt: bool,
}

#[pymethods]
impl UlmenDictRust {
    #[new]
    #[pyo3(signature = (data=None, optimizations=false, pool_size_limit=64))]
    fn new(
        py: Python<'_>,
        data: Option<&Bound<'_, PyList>>,
        optimizations: bool,
        pool_size_limit: usize,
    ) -> PyResult<Self> {
        let empty = PyList::empty_bound(py);
        let lst = data.unwrap_or(&empty);
        Ok(Self {
            core: UlmenCore::build(lst, pool_size_limit, 6)?,
            opt: optimizations,
        })
    }

    fn __len__(&self) -> usize {
        self.core.n_rows
    }
    fn pool_size(&self) -> usize {
        self.core.pool.len()
    }

    fn encode_text(&self) -> &str {
        self.core.text_cached()
    }

    fn encode_binary<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let data = if self.opt {
            self.core.binary_strat_cached()
        } else {
            self.core.binary_raw_cached()
        };
        PyBytes::new_bound(py, data)
    }

    fn encode_binary_pooled<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, self.core.binary_strat_cached())
    }

    fn encode_binary_pooled_raw<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, self.core.binary_raw_cached())
    }

    #[pyo3(signature = (level=6))]
    fn encode_binary_zlib<'py>(&mut self, py: Python<'py>, level: u32) -> Bound<'py, PyBytes> {
        if level != self.core.zlib_level {
            let data = zlib_compress(self.core.binary_strat_cached(), level);
            return PyBytes::new_bound(py, &data);
        }
        PyBytes::new_bound(py, self.core.get_zlib())
    }

    fn encode_ulmen_llm(&self, py: Python<'_>) -> PyResult<String> {
        let list = PyList::empty_bound(py);
        for col_row in 0..self.core.n_rows {
            let d = PyDict::new_bound(py);
            for (ci, &ki) in self.core.keys.iter().enumerate() {
                let key = self.core.st.get(ki);
                let val = self.core.cd[ci][col_row];
                let py_val: PyObject = match val {
                    ColVal::Null => py.None(),
                    ColVal::Bool(b) => b.into_py(py),
                    ColVal::Int(n) => n.into_py(py),
                    ColVal::Float(_) => val.as_f64().into_py(py),
                    ColVal::Str(si) => self.core.st.get(si).into_py(py),
                };
                d.set_item(key, py_val)?;
            }
            list.append(d)?;
        }
        encode_ulmen_llm_rust(py, &list)
    }

    fn bench_encode_text_only(&self, iters: usize) -> usize {
        self.core.text_cached().len() * iters
    }
    fn bench_encode_binary_only(&self, iters: usize) -> usize {
        self.core.binary_strat_cached().len() * iters
    }
    fn bench_encode_text_clone(&self, iters: usize) -> usize {
        let mut t = 0;
        for _ in 0..iters {
            t += self.core.text_cached().len();
        }
        t
    }
    fn bench_encode_binary_clone(&self, iters: usize) -> usize {
        let mut t = 0;
        for _ in 0..iters {
            t += self.core.binary_strat_cached().len();
        }
        t
    }

    fn __repr__(&self) -> String {
        format!(
            "UlmenDictRust(records={}, pool={}, optimizations={})",
            self.core.n_rows,
            self.core.pool.len(),
            self.opt,
        )
    }
}

#[pyclass]
struct UlmenDictFullRust {
    core: UlmenCore,
    lim: usize,
}

#[pymethods]
impl UlmenDictFullRust {
    #[new]
    #[pyo3(signature = (data=None, pool_size_limit=256))]
    fn new(
        py: Python<'_>,
        data: Option<&Bound<'_, PyList>>,
        pool_size_limit: usize,
    ) -> PyResult<Self> {
        let empty = PyList::empty_bound(py);
        let lst = data.unwrap_or(&empty);
        Ok(Self {
            core: UlmenCore::build(lst, pool_size_limit, 6)?,
            lim: pool_size_limit,
        })
    }

    fn __len__(&self) -> usize {
        self.core.n_rows
    }
    fn pool_size(&self) -> usize {
        self.core.pool.len()
    }

    fn encode_text(&self) -> &str {
        self.core.text_cached()
    }

    fn encode_binary<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, self.core.binary_strat_cached())
    }
    fn encode_binary_pooled<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, self.core.binary_strat_cached())
    }
    fn encode_binary_pooled_raw<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, self.core.binary_raw_cached())
    }

    #[pyo3(signature = (level=6))]
    fn encode_binary_zlib<'py>(&mut self, py: Python<'py>, level: u32) -> Bound<'py, PyBytes> {
        if level != self.core.zlib_level {
            let data = zlib_compress(self.core.binary_strat_cached(), level);
            return PyBytes::new_bound(py, &data);
        }
        PyBytes::new_bound(py, self.core.get_zlib())
    }

    fn encode_ulmen_llm(&self, py: Python<'_>) -> PyResult<String> {
        let list = PyList::empty_bound(py);
        for col_row in 0..self.core.n_rows {
            let d = PyDict::new_bound(py);
            for (ci, &ki) in self.core.keys.iter().enumerate() {
                let key = self.core.st.get(ki);
                let val = self.core.cd[ci][col_row];
                let py_val: PyObject = match val {
                    ColVal::Null => py.None(),
                    ColVal::Bool(b) => b.into_py(py),
                    ColVal::Int(n) => n.into_py(py),
                    ColVal::Float(_) => val.as_f64().into_py(py),
                    ColVal::Str(si) => self.core.st.get(si).into_py(py),
                };
                d.set_item(key, py_val)?;
            }
            list.append(d)?;
        }
        encode_ulmen_llm_rust(py, &list)
    }

    fn bench_encode_text_only(&self, iters: usize) -> usize {
        self.core.text_cached().len() * iters
    }
    fn bench_encode_binary_only(&self, iters: usize) -> usize {
        self.core.binary_strat_cached().len() * iters
    }
    fn bench_encode_text_clone(&self, iters: usize) -> usize {
        let mut t = 0;
        for _ in 0..iters {
            t += self.core.text_cached().len();
        }
        t
    }
    fn bench_encode_binary_clone(&self, iters: usize) -> usize {
        let mut t = 0;
        for _ in 0..iters {
            t += self.core.binary_strat_cached().len();
        }
        t
    }

    fn __repr__(&self) -> String {
        format!(
            "UlmenDictFullRust(records={}, pool={}, pool_limit={})",
            self.core.n_rows,
            self.core.pool.len(),
            self.lim,
        )
    }
}

// ---------------------------------------------------------------------------
// UlmenStreamEncoder — streaming binary encode, no full materialisation
// ---------------------------------------------------------------------------
//
// Usage (Python):
//   enc = UlmenStreamEncoder(pool_size_limit=64)
//   enc.feed_record({"id": 1, "name": "Alice"})
//   enc.feed_record({"id": 2, "name": "Bob"})
//   for chunk in enc.finish():
//       sink.write(chunk)
//
// Architecture:
//   Phase 1 (feed_record):  accumulate rows into ColVal columns in-memory.
//                           We need a two-pass design because the T_MATRIX
//                           header must know n_rows and n_cols before any
//                           column body bytes. The pool also requires a
//                           full frequency scan.
//
//   Phase 2 (finish):       build pool, encode all column bodies, assemble
//                           header + bodies, yield chunks of `chunk_size`
//                           bytes so the caller never holds the full payload.
//
// For truly unbounded streams where even the column scan is too large,
// use encode_binary_stream_chunked() which processes fixed-size windows
// and concatenates valid sub-payloads the decoder can handle sequentially.

#[pyclass]
struct UlmenStreamEncoder {
    rows: Vec<Vec<ColVal>>, // rows[row_idx][col_idx]
    keys: Vec<u32>,
    st: StringTable,
    max_pool: usize,
    chunk_size: usize,
    has_schema: bool,
}

#[pymethods]
impl UlmenStreamEncoder {
    #[new]
    #[pyo3(signature = (pool_size_limit=64, chunk_size=65536))]
    fn new(pool_size_limit: usize, chunk_size: usize) -> Self {
        Self {
            rows: Vec::new(),
            keys: Vec::new(),
            st: StringTable::new(128),
            max_pool: pool_size_limit,
            chunk_size: chunk_size.max(256),
            has_schema: false,
        }
    }

    /// Feed one record dict. Must be called before finish().
    fn feed_record(&mut self, _py: Python<'_>, record: &Bound<'_, PyDict>) -> PyResult<()> {
        let d = record;
        let _nc_existing = self.keys.len();

        // First record: establish schema
        if !self.has_schema {
            for (k, v) in d.iter() {
                let ks: String = k.extract()?;
                let ki = self.st.intern(ks);
                self.keys.push(ki);
                scan_val(&v, &mut self.st)?;
            }
            self.has_schema = true;
        }

        let nc = self.keys.len();
        let mut row = vec![ColVal::Null; nc];

        for (ci, &ki) in self.keys.iter().enumerate() {
            let key = self.st.get(ki);
            let val = match d.get_item(key)? {
                None => ColVal::Null,
                Some(v) => {
                    scan_val(&v, &mut self.st)?;
                    extract_val(&v, &mut self.st)?
                }
            };
            row[ci] = val;
        }

        self.rows.push(row);
        Ok(())
    }

    /// Feed multiple records at once (list of dicts).
    fn feed_records(&mut self, py: Python<'_>, records: &Bound<'_, PyList>) -> PyResult<()> {
        for item in records.iter() {
            let d = item.downcast::<PyDict>()?;
            self.feed_record(py, d)?;
        }
        Ok(())
    }

    /// Number of records fed so far.
    fn record_count(&self) -> usize {
        self.rows.len()
    }

    /// Finish encoding. Returns list of bytes chunks (each <= chunk_size bytes).
    /// After finish() the encoder is reset and can be reused.
    fn finish<'py>(&mut self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        let nr = self.rows.len();
        let nc = self.keys.len();

        if nr == 0 {
            let header = [MAGIC, VER].concat();
            self.reset();
            return Ok(vec![PyBytes::new_bound(py, &header)]);
        }

        // Transpose rows -> columns
        let cd: Vec<Vec<ColVal>> = (0..nc)
            .map(|ci| self.rows.iter().map(|row| row[ci]).collect())
            .collect();

        // Build pool
        let (pool, pi) = build_pool_inner(&cd, &self.keys, &self.st, nr, self.max_pool);

        // Detect strategies
        let col_strats: Vec<u8> = cd.iter().map(|v| detect_strat(v)).collect();

        // Encode column bodies — strat for multi-row, raw for single-row
        // assemble_binary nr==1 path strips b[..2] (T_LIST + varint) from raw
        // bodies to extract the scalar value; strat bodies (e.g. T_BITS) have
        // a different layout and must not be used for the nr==1 path.
        let col_bodies_strat: Vec<Vec<u8>> = cd
            .iter()
            .enumerate()
            .map(|(ci, v)| {
                let mut b = Vec::with_capacity(v.len() * 4 + 8);
                match col_strats[ci] {
                    S_BITS => pack_bits_col(v, &mut b),
                    S_DELTA => pack_delta_col(v, &mut b),
                    S_RLE => pack_rle_col(v, &self.st, &pi, &mut b),
                    _ => pack_raw_col(v, &self.st, &pi, &mut b),
                }
                b
            })
            .collect();
        let col_bodies_raw: Vec<Vec<u8>> = if nr == 1 {
            cd.iter()
                .map(|v| {
                    let mut b = Vec::with_capacity(v.len() * 4 + 8);
                    pack_raw_col(v, &self.st, &pi, &mut b);
                    b
                })
                .collect()
        } else {
            Vec::new()
        };
        let col_bodies = if nr == 1 {
            &col_bodies_raw
        } else {
            &col_bodies_strat
        };

        // Assemble full payload into a staging buffer
        let pool_block = build_binary_pool_block(&pool, &self.st);
        let payload = UlmenCore::assemble_binary(
            nr,
            nc,
            &self.keys,
            &self.st,
            &col_strats,
            &pool_block,
            col_bodies,
            nr > 1,
        );

        // Slice into chunks
        let chunk_sz = self.chunk_size;
        let chunks: Vec<Bound<'py, PyBytes>> = payload
            .chunks(chunk_sz)
            .map(|c| PyBytes::new_bound(py, c))
            .collect();

        self.reset();
        Ok(chunks)
    }

    /// Reset the encoder to initial state (called automatically by finish()).
    fn reset(&mut self) {
        self.rows.clear();
        self.keys.clear();
        self.st = StringTable::new(128);
        self.has_schema = false;
    }

    fn __repr__(&self) -> String {
        format!(
            "UlmenStreamEncoder(records_buffered={}, cols={}, pool_limit={})",
            self.rows.len(),
            self.keys.len(),
            self.max_pool,
        )
    }
}

// ---------------------------------------------------------------------------
// encode_binary_stream_chunked — window-based true streaming
// ---------------------------------------------------------------------------
//
// For payloads too large to hold in RAM even as ColVal arrays.
// Splits `records` into windows of `window_size` dicts, encodes each
// window as a complete self-contained ULMEN binary payload, and returns
// the list. Each sub-payload can be decoded independently with
// decode_binary_records_rust.

#[pyfunction]
#[pyo3(signature = (data, window_size=1000, pool_size_limit=64))]
fn encode_binary_stream_chunked<'py>(
    py: Python<'py>,
    data: &Bound<'_, PyList>,
    window_size: usize,
    pool_size_limit: usize,
) -> PyResult<Vec<Bound<'py, PyBytes>>> {
    let nr = data.len();
    if nr == 0 {
        let header = [MAGIC, VER].concat();
        return Ok(vec![PyBytes::new_bound(py, &header)]);
    }

    let ws = window_size.max(1);
    let mut result = Vec::with_capacity(nr.div_ceil(ws));

    let mut i = 0;
    while i < nr {
        let end = (i + ws).min(nr);
        let slice = PyList::empty_bound(py);
        for j in i..end {
            slice.append(data.get_item(j)?)?;
        }
        let core = UlmenCore::build(&slice, pool_size_limit, 6)?;
        result.push(PyBytes::new_bound(py, core.binary_strat_cached()));
        i = end;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Binary decoder
// ---------------------------------------------------------------------------

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    #[inline(always)]
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    #[inline(always)]
    fn peek(&self) -> u8 {
        self.buf[self.pos]
    }

    #[inline(always)]
    fn read_u8(&mut self) -> u8 {
        let b = self.buf[self.pos];
        self.pos += 1;
        b
    }

    #[inline(always)]
    fn read_bytes(&mut self, n: usize) -> &'a [u8] {
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        s
    }

    #[inline(always)]
    fn read_varint(&mut self) -> u64 {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let b = self.read_u8();
            result |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        result
    }

    #[inline(always)]
    fn read_zigzag(&mut self) -> i64 {
        let zz = self.read_varint();
        ((zz >> 1) as i64) ^ -((zz & 1) as i64)
    }

    fn read_str(&mut self) -> PyResult<&'a str> {
        let tag = self.read_u8();
        let len = if tag == T_STR_TINY {
            self.read_u8() as usize
        } else if tag == T_STR {
            self.read_varint() as usize
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected string tag, got 0x{:02x}",
                tag
            )));
        };
        let bytes = self.read_bytes(len);
        std::str::from_utf8(bytes)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("UTF-8 error: {}", e)))
    }

    fn read_value<'py>(
        &mut self,
        py: Python<'py>,
        pool: &[Bound<'py, PyString>],
    ) -> PyResult<Bound<'py, PyAny>> {
        let tag = self.read_u8();
        self.read_tagged_value(py, pool, tag)
    }

    fn read_tagged_value<'py>(
        &mut self,
        py: Python<'py>,
        pool: &[Bound<'py, PyString>],
        tag: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        match tag {
            T_STR_TINY => {
                let len = self.read_u8() as usize;
                let b = self.read_bytes(len);
                let s = std::str::from_utf8(b).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("UTF-8: {}", e))
                })?;
                Ok(PyString::new_bound(py, s).into_any())
            }
            T_STR => {
                let len = self.read_varint() as usize;
                let b = self.read_bytes(len);
                let s = std::str::from_utf8(b).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("UTF-8: {}", e))
                })?;
                Ok(PyString::new_bound(py, s).into_any())
            }
            T_INT => {
                let n = self.read_zigzag();
                Ok(n.into_py(py).into_bound(py).into_any())
            }
            T_FLOAT => {
                let bits = u64::from_be_bytes(self.read_bytes(8).try_into().unwrap());
                Ok(f64::from_bits(bits).into_py(py).into_bound(py).into_any())
            }
            T_BOOL => {
                let v = self.read_u8() != 0;
                Ok(v.into_py(py).into_bound(py).into_any())
            }
            T_NULL => Ok(py.None().into_bound(py).into_any()),
            T_POOL_REF => {
                let idx = self.read_varint() as usize;
                Ok(if idx < pool.len() {
                    pool[idx].clone().into_any()
                } else {
                    py.None().into_bound(py).into_any()
                })
            }
            T_LIST => {
                let n = self.read_varint() as usize;
                let list = PyList::empty_bound(py);
                for _ in 0..n {
                    list.append(self.read_value(py, pool)?)?;
                }
                Ok(list.into_any())
            }
            T_MAP => {
                let n = self.read_varint() as usize;
                let d = PyDict::new_bound(py);
                for _ in 0..n {
                    let k = self.read_value(py, pool)?;
                    let v = self.read_value(py, pool)?;
                    d.set_item(k, v)?;
                }
                Ok(d.into_any())
            }
            T_BITS => {
                let n = self.read_varint() as usize;
                let n_bytes = n.div_ceil(8);
                let arr = self.read_bytes(n_bytes);
                let list = PyList::empty_bound(py);
                for i in 0..n {
                    list.append(
                        (arr[i >> 3] & (1 << (i & 7)) != 0)
                            .into_py(py)
                            .into_bound(py),
                    )?;
                }
                Ok(list.into_any())
            }
            T_DELTA_RAW => {
                let n = self.read_varint() as usize;
                let list = PyList::empty_bound(py);
                if n == 0 {
                    return Ok(list.into_any());
                }
                let first = self.read_zigzag();
                list.append(first.into_py(py).into_bound(py))?;
                let mut prev = first;
                for _ in 1..n {
                    prev += self.read_zigzag();
                    list.append(prev.into_py(py).into_bound(py))?;
                }
                Ok(list.into_any())
            }
            T_RLE => {
                let n_runs = self.read_varint() as usize;
                let list = PyList::empty_bound(py);
                for _ in 0..n_runs {
                    let v = self.read_value(py, pool)?;
                    let run = self.read_varint() as usize;
                    for _ in 0..run {
                        list.append(v.clone().into_any())?;
                    }
                }
                Ok(list.into_any())
            }
            other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown tag 0x{:02x} at pos {}",
                other,
                self.pos - 1
            ))),
        }
    }
}

fn read_column<'py>(
    cur: &mut Cursor<'_>,
    py: Python<'py>,
    pool: &[Bound<'py, PyString>],
    strat: u8,
    n_rows: usize,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    match strat {
        S_BITS => {
            let tag = cur.read_u8();
            if tag != T_BITS {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Expected T_BITS(0x0e), got 0x{:02x}",
                    tag
                )));
            }
            let n = cur.read_varint() as usize;
            let n_bytes = n.div_ceil(8);
            let arr = cur.read_bytes(n_bytes);
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(
                    (arr[i >> 3] & (1 << (i & 7)) != 0)
                        .into_py(py)
                        .into_bound(py)
                        .into_any(),
                );
            }
            Ok(out)
        }
        S_DELTA => {
            let tag = cur.read_u8();
            if tag != T_DELTA_RAW {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Expected T_DELTA_RAW(0x0c), got 0x{:02x}",
                    tag
                )));
            }
            let n = cur.read_varint() as usize;
            let mut out = Vec::with_capacity(n);
            if n == 0 {
                return Ok(out);
            }
            let first = cur.read_zigzag();
            out.push(first.into_py(py).into_bound(py).into_any());
            let mut prev = first;
            for _ in 1..n {
                prev += cur.read_zigzag();
                out.push(prev.into_py(py).into_bound(py).into_any());
            }
            Ok(out)
        }
        S_RLE => {
            let tag = cur.read_u8();
            if tag != T_RLE {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Expected T_RLE(0x0f), got 0x{:02x}",
                    tag
                )));
            }
            let n_runs = cur.read_varint() as usize;
            let mut out = Vec::with_capacity(n_rows);
            for _ in 0..n_runs {
                let v = cur.read_value(py, pool)?;
                let run = cur.read_varint() as usize;
                for _ in 0..run {
                    out.push(v.clone().into_any());
                }
            }
            Ok(out)
        }
        S_POOL | S_RAW => {
            // Both S_POOL and S_RAW store a T_LIST of individually encoded values.
            // S_POOL values may include T_POOL_REF tags resolved via pool.
            let tag = cur.read_u8();
            if tag != T_LIST {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Expected T_LIST(0x07) for raw/pool col, got 0x{:02x}",
                    tag
                )));
            }
            let n = cur.read_varint() as usize;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(cur.read_value(py, pool)?);
            }
            Ok(out)
        }
        unknown => {
            // Forward-compatible: treat unknown strategy as raw T_LIST
            let tag = cur.read_u8();
            if tag != T_LIST {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown strategy 0x{:02x}, expected T_LIST(0x07) fallback, got 0x{:02x}",
                    unknown, tag
                )));
            }
            let n = cur.read_varint() as usize;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(cur.read_value(py, pool)?);
            }
            Ok(out)
        }
    }
}

#[pyfunction]
fn decode_binary_records_rust<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyList>> {
    if data.len() < 6 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Payload too short: {} bytes (need at least 6)",
            data.len()
        )));
    }
    if &data[0..4] != b"LUMB" {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Bad magic: expected b'LUMB', got {:?}",
            &data[0..4.min(data.len())]
        )));
    }

    let mut cur = Cursor::new(data);
    cur.pos = 6; // skip MAGIC + VERSION
    let mut pool: Vec<Bound<'py, PyString>> = Vec::new();
    let result = PyList::empty_bound(py);

    while cur.pos < cur.buf.len() {
        let tag = cur.peek();
        match tag {
            T_POOL_DEF => {
                cur.pos += 1;
                let n = cur.read_varint() as usize;
                pool.reserve(n);
                for _ in 0..n {
                    let s = cur.read_str()?;
                    pool.push(PyString::new_bound(py, s));
                }
            }
            T_MATRIX => {
                cur.pos += 1;
                let n_rows = cur.read_varint() as usize;
                let n_cols = cur.read_varint() as usize;

                let mut col_names: Vec<Bound<'py, PyString>> = Vec::with_capacity(n_cols);
                let mut col_strats: Vec<u8> = Vec::with_capacity(n_cols);
                for _ in 0..n_cols {
                    let name = cur.read_str()?;
                    col_names.push(PyString::new_bound(py, name));
                    col_strats.push(cur.read_u8());
                }
                let mut col_data: Vec<Vec<Bound<'py, PyAny>>> = Vec::with_capacity(n_cols);
                for &strat in &col_strats {
                    col_data.push(read_column(&mut cur, py, &pool, strat, n_rows)?);
                }
                for ri in 0..n_rows {
                    let d = PyDict::new_bound(py);
                    for ci in 0..n_cols {
                        if ri < col_data[ci].len() {
                            d.set_item(col_names[ci].clone(), col_data[ci][ri].clone())?;
                        } else {
                            d.set_item(col_names[ci].clone(), py.None())?;
                        }
                    }
                    result.append(d)?;
                }
                // T_MATRIX is always the last block — stop after decoding it
                break;
            }
            T_LIST => {
                cur.pos += 1;
                let n = cur.read_varint() as usize;
                for _ in 0..n {
                    let v = cur.read_value(py, &pool)?;
                    result.append(v)?;
                }
            }
            _ => {
                // Scalar or unknown top-level value — decode and append
                let v = cur.read_value(py, &pool)?;
                result.append(v)?;
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// ULMEN encoder / decoder (unchanged, correct and complete)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum LlmColType {
    Unknown,
    Null,
    Bool,
    Int,
    Float,
    Str,
    Mixed,
}

#[inline(always)]
fn llm_needs_quoting(s: &str) -> bool {
    s.bytes().any(|b| {
        matches!(
            b,
            b',' | b'"' | b'\n' | b'\r' | b'{' | b'}' | b'[' | b']' | b'|' | b':'
        )
    })
}

#[inline(always)]
fn llm_encode_str_into(s: &str, out: &mut String) {
    if s.is_empty() {
        out.push_str("$0=");
        return;
    }
    if llm_needs_quoting(s) {
        out.push('"');
        for c in s.chars() {
            if c == '"' {
                out.push('"');
            }
            out.push(c);
        }
        out.push('"');
    } else {
        out.push_str(s);
    }
}

#[inline(always)]
fn llm_encode_val_into(v: &Bound<'_, PyAny>, out: &mut String) -> PyResult<()> {
    if v.is_none() {
        out.push('N');
        return Ok(());
    }
    if v.is_instance_of::<PyBool>() {
        out.push(if v.downcast::<PyBool>()?.is_true() {
            'T'
        } else {
            'F'
        });
        return Ok(());
    }
    if v.is_instance_of::<PyInt>() {
        if let Ok(n) = v.extract::<i64>() {
            let _ = write!(out, "{}", n);
            return Ok(());
        }
    }
    if v.is_instance_of::<PyFloat>() {
        if let Ok(f) = v.extract::<f64>() {
            if f.is_nan() {
                out.push_str("nan");
            } else if f.is_infinite() {
                out.push_str(if f > 0.0 { "inf" } else { "-inf" });
            } else {
                let _ = write!(out, "{:?}", f);
            }
            return Ok(());
        }
    }
    if v.is_instance_of::<PyString>() {
        llm_encode_str_into(v.downcast::<PyString>()?.to_str().unwrap_or(""), out);
        return Ok(());
    }
    if let Ok(d) = v.downcast::<PyDict>() {
        if d.is_empty() {
            out.push_str("{}");
            return Ok(());
        }
        out.push('{');
        let mut first = true;
        for (k, val) in d.iter() {
            if !first {
                out.push(',');
            }
            first = false;
            llm_encode_val_into(&k, out)?;
            out.push(':');
            llm_encode_val_into(&val, out)?;
        }
        out.push('}');
        return Ok(());
    }
    if let Ok(l) = v.downcast::<PyList>() {
        if l.is_empty() {
            out.push_str("[]");
            return Ok(());
        }
        out.push('[');
        for (i, item) in l.iter().enumerate() {
            if i > 0 {
                out.push('|');
            }
            llm_encode_val_into(&item, out)?;
        }
        out.push(']');
        return Ok(());
    }
    llm_encode_str_into(&v.str()?.to_string(), out);
    Ok(())
}

#[inline(always)]
fn llm_type_char(t: LlmColType) -> char {
    match t {
        LlmColType::Unknown | LlmColType::Null => 'n',
        LlmColType::Bool => 'b',
        LlmColType::Int => 'd',
        LlmColType::Float => 'f',
        LlmColType::Str => 's',
        LlmColType::Mixed => 'm',
    }
}

#[inline(always)]
fn llm_type_and_encode(v: &Bound<'_, PyAny>, out: &mut String) -> PyResult<LlmColType> {
    if v.is_none() {
        out.push('N');
        return Ok(LlmColType::Null);
    }
    let binding = v.get_type();
    let type_name = binding.name()?;
    match type_name.as_ref() {
        "bool" => {
            out.push(if v.is_truthy()? { 'T' } else { 'F' });
            Ok(LlmColType::Bool)
        }
        "int" => {
            if let Ok(n) = v.extract::<i64>() {
                let _ = write!(out, "{}", n);
            }
            Ok(LlmColType::Int)
        }
        "float" => {
            if let Ok(f) = v.extract::<f64>() {
                if f.is_nan() {
                    out.push_str("nan");
                } else if f.is_infinite() {
                    out.push_str(if f > 0.0 { "inf" } else { "-inf" });
                } else {
                    let _ = write!(out, "{:?}", f);
                }
            }
            Ok(LlmColType::Float)
        }
        "str" => {
            llm_encode_str_into(v.downcast::<PyString>()?.to_str().unwrap_or(""), out);
            Ok(LlmColType::Str)
        }
        "NoneType" => {
            out.push('N');
            Ok(LlmColType::Null)
        }
        "dict" => {
            if let Ok(d) = v.downcast::<PyDict>() {
                if d.is_empty() {
                    out.push_str("{}");
                } else {
                    out.push('{');
                    let mut first = true;
                    for (k, val) in d.iter() {
                        if !first {
                            out.push(',');
                        }
                        first = false;
                        llm_encode_val_into(&k, out)?;
                        out.push(':');
                        llm_encode_val_into(&val, out)?;
                    }
                    out.push('}');
                }
            }
            Ok(LlmColType::Mixed)
        }
        "list" | "tuple" => {
            if let Ok(l) = v.downcast::<PyList>() {
                if l.is_empty() {
                    out.push_str("[]");
                } else {
                    out.push('[');
                    for (i, item) in l.iter().enumerate() {
                        if i > 0 {
                            out.push('|');
                        }
                        llm_encode_val_into(&item, out)?;
                    }
                    out.push(']');
                }
            }
            Ok(LlmColType::Mixed)
        }
        _ => {
            llm_encode_val_into(v, out)?;
            Ok(LlmColType::Mixed)
        }
    }
}

#[pyfunction]
fn encode_ulmen_llm_rust(_py: Python<'_>, data: &Bound<'_, PyList>) -> PyResult<String> {
    let nr = data.len();
    if nr == 0 {
        return Ok("L|".to_string());
    }

    let first = data.get_item(0)?;
    let first_dict = match first.downcast::<PyDict>() {
        Ok(d) => d,
        Err(_) => {
            let mut out = String::with_capacity(nr * 16 + 2);
            out.push_str("L|");
            for i in 0..nr {
                out.push('\n');
                llm_encode_val_into(&data.get_item(i)?, &mut out)?;
            }
            return Ok(out);
        }
    };

    let mut keys: Vec<String> = Vec::with_capacity(first_dict.len());
    let mut keys_seen: FxHashMap<String, ()> = fx_map(first_dict.len());
    for row in data.iter() {
        if let Ok(d) = row.downcast::<PyDict>() {
            for (k, _) in d.iter() {
                if let Ok(ks) = k.extract::<String>() {
                    if keys_seen.insert(ks.clone(), ()).is_none() {
                        keys.push(ks);
                    }
                }
            }
        }
    }

    if keys.is_empty() {
        let mut out = String::with_capacity(nr * 3 + 4);
        out.push_str("L|{}");
        for _ in 0..nr {
            out.push('\n');
            out.push_str("{}");
        }
        return Ok(out);
    }

    let nc = keys.len();
    let mut col_type: Vec<Option<LlmColType>> = vec![None; nc];
    let mut lines: Vec<String> = vec![String::new(); nr + 1];

    for ri in 0..nr {
        let row = data.get_item(ri)?;
        let d = row.downcast::<PyDict>()?;
        let mut line = String::with_capacity(nc * 8);

        for ci in 0..nc {
            if ci > 0 {
                line.push(',');
            }
            let v = match d.get_item(&keys[ci])? {
                Some(val) => val,
                None => {
                    line.push('N');
                    continue;
                }
            };

            let vtype = llm_type_and_encode(&v, &mut line)?;

            match vtype {
                LlmColType::Null => {
                    if col_type[ci].is_none() {
                        col_type[ci] = Some(LlmColType::Null);
                    }
                }
                other => {
                    col_type[ci] = Some(match col_type[ci] {
                        None | Some(LlmColType::Null) => other,
                        Some(LlmColType::Mixed) => LlmColType::Mixed,
                        Some(existing) if existing == other => existing,
                        _ => LlmColType::Mixed,
                    });
                }
            }
        }
        lines[ri + 1] = line;
    }

    let mut header = String::with_capacity(nc * 12 + 2);
    header.push_str("L|");
    for ci in 0..nc {
        if ci > 0 {
            header.push(',');
        }
        llm_encode_str_into(&keys[ci], &mut header);
        header.push(':');
        header.push(llm_type_char(col_type[ci].unwrap_or(LlmColType::Null)));
    }
    lines[0] = header;

    let total_len: usize = lines.iter().map(|l| l.len() + 1).sum();
    let mut out = String::with_capacity(total_len);
    for (i, line) in lines.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(line);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// ULMEN decoder helpers
// ---------------------------------------------------------------------------

#[inline(always)]
fn llm_dec_d<'py>(tok: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    if tok == "N" {
        return Ok(py.None().into_bound(py).into_any());
    }
    if let Ok(n) = tok.parse::<i64>() {
        return Ok(n.into_py(py).into_bound(py).into_any());
    }
    Ok(PyString::new_bound(py, tok).into_any())
}

#[inline(always)]
fn llm_dec_f<'py>(tok: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match tok {
        "N" => Ok(py.None().into_bound(py).into_any()),
        "nan" => Ok(f64::NAN.into_py(py).into_bound(py).into_any()),
        "inf" => Ok(f64::INFINITY.into_py(py).into_bound(py).into_any()),
        "-inf" => Ok(f64::NEG_INFINITY.into_py(py).into_bound(py).into_any()),
        _ => {
            if let Ok(f) = tok.parse::<f64>() {
                Ok(f.into_py(py).into_bound(py).into_any())
            } else {
                Ok(PyString::new_bound(py, tok).into_any())
            }
        }
    }
}

#[inline(always)]
fn llm_dec_b<'py>(tok: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match tok {
        "T" => Ok(true.into_py(py).into_bound(py).into_any()),
        "F" => Ok(false.into_py(py).into_bound(py).into_any()),
        _ => Ok(py.None().into_bound(py).into_any()),
    }
}

#[inline(always)]
fn llm_dec_s<'py>(tok: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match tok {
        "N" => Ok(py.None().into_bound(py).into_any()),
        "$0=" => Ok(PyString::new_bound(py, "").into_any()),
        _ => {
            if tok.starts_with('"') && tok.ends_with('"') && tok.len() >= 2 {
                let inner = &tok[1..tok.len() - 1];
                let unquoted = inner.replace("\"\"", "\"");
                return Ok(PyString::new_bound(py, &unquoted).into_any());
            }
            Ok(PyString::new_bound(py, tok).into_any())
        }
    }
}

#[inline(always)]
fn llm_dec_n<'py>(py: Python<'py>) -> Bound<'py, PyAny> {
    py.None().into_bound(py).into_any()
}

fn llm_row_is_plain(row: &str) -> bool {
    !row.contains('"') && !row.contains('{') && !row.contains('[')
}

fn llm_split_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_quote = false;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '"' && !in_quote {
            in_quote = true;
        } else if in_quote {
            if c == '"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 1;
                } else {
                    in_quote = false;
                }
            }
        } else if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
        } else if c == sep && depth == 0 {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

fn llm_find_top_level(s: &str, ch: char) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_quote = false;
    for (i, c) in s.char_indices() {
        if c == '"' && !in_quote {
            in_quote = true;
            continue;
        }
        if in_quote {
            if c == '"' {
                in_quote = false;
            }
            continue;
        }
        if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
        } else if c == ch && depth == 0 {
            return Some(i);
        }
    }
    None
}

fn llm_parse_tok<'py>(tok: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match tok {
        "N" => Ok(py.None().into_bound(py).into_any()),
        "T" => Ok(true.into_py(py).into_bound(py).into_any()),
        "F" => Ok(false.into_py(py).into_bound(py).into_any()),
        "$0=" => Ok(PyString::new_bound(py, "").into_any()),
        "nan" => Ok(f64::NAN.into_py(py).into_bound(py).into_any()),
        "inf" => Ok(f64::INFINITY.into_py(py).into_bound(py).into_any()),
        "-inf" => Ok(f64::NEG_INFINITY.into_py(py).into_bound(py).into_any()),
        "{}" => Ok(PyDict::new_bound(py).into_any()),
        "[]" => Ok(PyList::empty_bound(py).into_any()),
        _ => {
            if tok.starts_with('{') && tok.ends_with('}') {
                return llm_decode_nested_dict(&tok[1..tok.len() - 1], py);
            }
            if tok.starts_with('[') && tok.ends_with(']') {
                return llm_decode_nested_list(&tok[1..tok.len() - 1], py);
            }
            if tok.starts_with('"') && tok.ends_with('"') && tok.len() >= 2 {
                let inner = &tok[1..tok.len() - 1];
                let unquoted = inner.replace("\"\"", "\"");
                return Ok(PyString::new_bound(py, &unquoted).into_any());
            }
            if let Ok(n) = tok.parse::<i64>() {
                return Ok(n.into_py(py).into_bound(py).into_any());
            }
            if let Ok(f) = tok.parse::<f64>() {
                return Ok(f.into_py(py).into_bound(py).into_any());
            }
            Ok(PyString::new_bound(py, tok).into_any())
        }
    }
}

fn llm_decode_nested_dict<'py>(inner: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let d = PyDict::new_bound(py);
    if inner.is_empty() {
        return Ok(d.into_any());
    }
    for pair in llm_split_top_level(inner, ',') {
        if let Some(colon) = llm_find_top_level(pair, ':') {
            let k = llm_parse_tok(pair[..colon].trim(), py)?;
            let v = llm_parse_tok(pair[colon + 1..].trim(), py)?;
            d.set_item(k, v)?;
        }
    }
    Ok(d.into_any())
}

fn llm_decode_nested_list<'py>(inner: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let lst = PyList::empty_bound(py);
    if inner.is_empty() {
        return Ok(lst.into_any());
    }
    for tok in llm_split_top_level(inner, '|') {
        lst.append(llm_parse_tok(tok, py)?)?;
    }
    Ok(lst.into_any())
}

fn llm_parse_row_quoted(line: &str) -> Vec<&str> {
    let mut fields: Vec<&str> = Vec::new();
    let bytes = line.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;
    while i < n {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'"' {
                    if i + 1 < n && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            fields.push(&line[start..i]);
            if i < n && bytes[i] == b',' {
                i += 1;
            }
        } else if bytes[i] == b'{' || bytes[i] == b'[' {
            let open = bytes[i];
            let close = if open == b'{' { b'}' } else { b']' };
            let start = i;
            let mut depth = 0i32;
            let mut in_q = false;
            while i < n {
                let c = bytes[i];
                if c == b'"' && !in_q {
                    in_q = true;
                } else if in_q && c == b'"' {
                    in_q = false;
                } else if c == open {
                    depth += 1;
                } else if c == close {
                    depth -= 1;
                    if depth == 0 {
                        i += 1;
                        break;
                    }
                }
                i += 1;
            }
            fields.push(&line[start..i]);
            if i < n && bytes[i] == b',' {
                i += 1;
            }
        } else {
            let start = i;
            while i < n && bytes[i] != b',' {
                i += 1;
            }
            fields.push(&line[start..i]);
            if i < n {
                i += 1;
            }
        }
    }
    fields
}

fn llm_split_rows_quoted(text: &str) -> Vec<&str> {
    let mut rows = Vec::new();
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_q = false;
    while i < n {
        let c = bytes[i];
        if c == b'"' {
            if in_q && i + 1 < n && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_q = !in_q;
        } else if c == b'\n' && !in_q {
            rows.push(&text[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    rows.push(&text[start..]);
    rows
}

#[pyfunction]
fn decode_ulmen_llm_rust<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyList>> {
    if text.len() < 2 || !text.starts_with("L|") {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Not a ULMEN LLM payload: must start with 'L|'",
        ));
    }

    let needs_slow = text.contains('"') || text.contains('{') || text.contains('[');
    let rows: Vec<&str> = if needs_slow {
        llm_split_rows_quoted(text)
    } else {
        text.split('\n').collect()
    };
    let header_content = &rows[0][2..];
    let data_rows = &rows[1..];
    let result = PyList::empty_bound(py);

    if header_content.is_empty() {
        let data: Vec<&str> = data_rows
            .iter()
            .filter(|r| !r.is_empty())
            .copied()
            .collect();
        if data.is_empty() {
            return Ok(result);
        }
        for tok in data {
            result.append(llm_parse_tok(tok, py)?)?;
        }
        return Ok(result);
    }

    if header_content == "{}" {
        for row in data_rows {
            if *row == "{}" {
                result.append(PyDict::new_bound(py))?;
            }
        }
        return Ok(result);
    }

    let raw_specs: Vec<&str> = if llm_row_is_plain(header_content) {
        header_content.split(',').collect()
    } else {
        llm_split_top_level(header_content, ',')
    };

    let nc = raw_specs.len();
    let mut keys: Vec<&str> = Vec::with_capacity(nc);
    let mut types: Vec<char> = Vec::with_capacity(nc);

    for spec in &raw_specs {
        let b = spec.as_bytes();
        if b.len() >= 3 && b[b.len() - 2] == b':' {
            let tc = b[b.len() - 1] as char;
            if matches!(tc, 'd' | 'f' | 'b' | 's' | 'n' | 'm') {
                keys.push(&spec[..spec.len() - 2]);
                types.push(tc);
                continue;
            }
        }
        keys.push(spec);
        types.push('m');
    }

    let py_keys: Vec<Bound<'py, PyString>> =
        keys.iter().map(|k| PyString::new_bound(py, k)).collect();

    if !needs_slow {
        for row in data_rows {
            if row.is_empty() {
                continue;
            }
            let toks: Vec<&str> = row.split(',').collect();
            let d = PyDict::new_bound(py);
            for ci in 0..nc {
                let tok = if ci < toks.len() { toks[ci] } else { "N" };
                let val = match types[ci] {
                    'd' => llm_dec_d(tok, py)?,
                    'f' => llm_dec_f(tok, py)?,
                    'b' => llm_dec_b(tok, py)?,
                    's' => llm_dec_s(tok, py)?,
                    'n' => llm_dec_n(py),
                    _ => llm_parse_tok(tok, py)?,
                };
                d.set_item(&py_keys[ci], val)?;
            }
            result.append(d)?;
        }
    } else {
        for row in data_rows {
            if row.is_empty() {
                continue;
            }
            let toks: Vec<&str> = if llm_row_is_plain(row) {
                row.split(',').collect()
            } else {
                llm_parse_row_quoted(row)
            };
            let d = PyDict::new_bound(py);
            for ci in 0..nc {
                let tok = if ci < toks.len() { toks[ci] } else { "N" };
                let val = match types[ci] {
                    'd' => llm_dec_d(tok, py)?,
                    'f' => llm_dec_f(tok, py)?,
                    'b' => llm_dec_b(tok, py)?,
                    's' => llm_dec_s(tok, py)?,
                    'n' => llm_dec_n(py),
                    _ => llm_parse_tok(tok, py)?,
                };
                d.set_item(&py_keys[ci], val)?;
            }
            result.append(d)?;
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Token counting — cl100k_base BPE approximation (zero external deps)
// ---------------------------------------------------------------------------

/// Check if a byte is ASCII alphanumeric
#[inline(always)]
fn is_alnum(b: u8) -> bool {
    b.is_ascii_alphanumeric()
}

/// Split text into pre-token chunks (approximates cl100k_base pre-tokenizer).
fn bpe_split(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut chunks = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let start = i;

        // Contraction: 's 't 're 've 'm 'll 'd
        if bytes[i] == b'\'' && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            if next == b's' || next == b't' || next == b'm' || next == b'd' {
                i += 2;
                chunks.push(&text[start..i]);
                continue;
            }
            if i + 2 < bytes.len() {
                let nn = bytes[i + 2];
                if (next == b'r' && nn == b'e')
                    || (next == b'v' && nn == b'e')
                    || (next == b'l' && nn == b'l')
                {
                    i += 3;
                    chunks.push(&text[start..i]);
                    continue;
                }
            }
        }

        // Word run (ASCII alpha + extended Latin)
        if bytes[i].is_ascii_alphabetic() || bytes[i] >= 0xC0 {
            while i < bytes.len() && (bytes[i].is_ascii_alphabetic() || bytes[i] >= 0x80) {
                i += 1;
            }
            chunks.push(&text[start..i]);
            continue;
        }

        // Digit run (up to 3 digits)
        if bytes[i].is_ascii_digit() {
            let mut count = 0;
            while i < bytes.len() && bytes[i].is_ascii_digit() && count < 3 {
                i += 1;
                count += 1;
            }
            chunks.push(&text[start..i]);
            continue;
        }

        // Whitespace run
        if bytes[i].is_ascii_whitespace() {
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            chunks.push(&text[start..i]);
            continue;
        }

        // Punctuation / symbol run
        while i < bytes.len()
            && !bytes[i].is_ascii_alphanumeric()
            && !bytes[i].is_ascii_whitespace()
            && bytes[i] < 0x80
        {
            i += 1;
        }
        if i > start {
            chunks.push(&text[start..i]);
        } else {
            // Single byte fallback (non-ASCII non-alpha)
            i += 1;
            while i < bytes.len() && bytes[i] >= 0x80 && bytes[i] < 0xC0 {
                i += 1; // consume continuation bytes
            }
            chunks.push(&text[start..i]);
        }
    }

    chunks
}

/// Estimate BPE token count for one pre-token chunk.
#[inline]
fn bpe_chunk_tokens(chunk: &str) -> usize {
    let n = chunk.len();
    if n <= 4 {
        1
    } else if n <= 8 {
        2
    } else {
        (n + 3) / 4
    }
}

/// Count tokens using cl100k_base-compatible approximation.
#[pyfunction]
fn count_tokens(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    let chunks = bpe_split(text);
    if chunks.is_empty() {
        return std::cmp::max(1, (text.len() + 3) / 4);
    }
    chunks.iter().map(|c| bpe_chunk_tokens(c)).sum()
}

/// Rough token estimate (chars / 4). Fast but less accurate.
#[pyfunction]
fn estimate_tokens(text: &str) -> usize {
    (text.len() + 3) / 4
}

// ---------------------------------------------------------------------------
// JSON bridge — convert between JSON and ULMEN-AGENT format
// ---------------------------------------------------------------------------

/// Convert a JSON string of records to ULMEN-AGENT payload.
/// Records must be a JSON array of objects with "type", "id", "thread_id", "step" fields.
#[pyfunction]
#[pyo3(signature = (json_str, thread_id=None, context_window=None))]
fn from_json(
    py: Python<'_>,
    json_str: &str,
    thread_id: Option<&str>,
    context_window: Option<usize>,
) -> PyResult<String> {
    // Parse JSON using Python's json module (reliable, handles edge cases)
    let json_mod = py.import_bound("json")?;
    let loaded = json_mod.call_method1("loads", (json_str,))?;
    let records = loaded.downcast::<PyList>()?;

    // Encode with the Rust ULMEN-AGENT path directly.
    encode_agent_payload_rust(
        py,
        records,
        thread_id.map(|s| s.to_string()),
        context_window.map(|v| v as i64),
        None,
        true,
        false,
        None,
        None,
        None,
        None,
        None,
        false,
    )
}

/// Convert a ULMEN-AGENT payload to a JSON string.
#[pyfunction]
#[pyo3(signature = (payload, pretty=false))]
fn to_json(py: Python<'_>, payload: &str, pretty: bool) -> PyResult<String> {
    // Decode with the Rust ULMEN-AGENT path directly.
    let records = decode_agent_payload_rust(py, payload)?;
    let json_mod = py.import_bound("json")?;
    let kwargs = pyo3::types::PyDict::new_bound(py);
    if pretty {
        kwargs.set_item("indent", 2)?;
    }
    kwargs.set_item("ensure_ascii", false)?;
    let result = json_mod.call_method("dumps", (records,), Some(&kwargs))?;
    result.extract::<String>()
}

/// Compare sizes: returns (json_bytes, ulmen_bytes, saving_pct)
#[pyfunction]
fn compare_sizes(py: Python<'_>, json_str: &str) -> PyResult<(usize, usize, f64)> {
    let ulmen_payload = from_json(py, json_str, None, None)?;
    let json_bytes = json_str.len();
    let ulmen_bytes = ulmen_payload.len();
    let saving = if json_bytes > 0 {
        (1.0 - ulmen_bytes as f64 / json_bytes as f64) * 100.0
    } else {
        0.0
    };
    Ok((json_bytes, ulmen_bytes, saving))
}

// ---------------------------------------------------------------------------
// ULMEN-AGENT Rust-native encode/decode helpers
// ---------------------------------------------------------------------------

/// Encode a single ULMEN-AGENT field value to its string representation.
#[pyfunction]
fn encode_agent_field(_py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_none() {
        return Ok("N".to_string());
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b { "T".to_string() } else { "F".to_string() });
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = value.extract::<f64>() {
        if f.is_nan() {
            return Ok("nan".to_string());
        }
        if f.is_infinite() {
            return Ok(if f > 0.0 {
                "inf".to_string()
            } else {
                "-inf".to_string()
            });
        }
        return Ok(format!("{f}"));
    }
    if let Ok(s) = value.extract::<String>() {
        if s.is_empty() {
            return Ok("$0=".to_string());
        }
        if s.contains('|')
            || s.contains('"')
            || s.contains('\\')
            || s.contains('\n')
            || s.contains('\r')
        {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('"', "\"\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r");
            return Ok(format!("\"{escaped}\""));
        }
        return Ok(s);
    }
    let repr = value.str()?.to_string();
    if repr.contains('|')
        || repr.contains('"')
        || repr.contains('\\')
        || repr.contains('\n')
        || repr.contains('\r')
    {
        let escaped = repr
            .replace('\\', "\\\\")
            .replace('"', "\"\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r");
        return Ok(format!("\"{escaped}\""));
    }
    Ok(repr)
}

/// Decode a single ULMEN-AGENT field token.
#[pyfunction]
fn decode_agent_field(token: &str, type_char: &str) -> PyResult<PyObject> {
    Python::with_gil(|py| match token {
        "N" => Ok(py.None()),
        "T" => Ok(true.into_py(py)),
        "F" => Ok(false.into_py(py)),
        "$0=" => Ok("".into_py(py)),
        _ => match type_char {
            "d" => {
                if let Ok(i) = token.parse::<i64>() {
                    Ok(i.into_py(py))
                } else {
                    Ok(token.into_py(py))
                }
            }
            "f" => match token {
                "nan" => Ok(f64::NAN.into_py(py)),
                "inf" => Ok(f64::INFINITY.into_py(py)),
                "-inf" => Ok(f64::NEG_INFINITY.into_py(py)),
                _ => {
                    if let Ok(f) = token.parse::<f64>() {
                        Ok(f.into_py(py))
                    } else {
                        Ok(token.into_py(py))
                    }
                }
            },
            "b" => match token {
                "T" | "true" | "1" => Ok(true.into_py(py)),
                "F" | "false" | "0" => Ok(false.into_py(py)),
                _ => Ok(token.into_py(py)),
            },
            "s" | _ => {
                if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
                    let inner = &token[1..token.len() - 1];
                    let mut unescaped = inner.replace("\\\\", "\\");
                    unescaped = unescaped.replace("\\n", "\n").replace("\\r", "\r");
                    unescaped = unescaped.replace("\"\"", "\"");
                    Ok(unescaped.into_py(py))
                } else {
                    Ok(token.into_py(py))
                }
            }
        },
    })
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ULMEN-AGENT v1 -- Rust-native encode / decode / validate
//
// Wire format (text, pipe-delimited):
//   ULMEN-AGENT v1
//   [thread: <id>]
//   [context_window: <n>]
//   [context_used: <n>]
//   [payload_id: <id>]
//   [parent_payload_id: <id>]
//   [agent_id: <id>]
//   [session_id: <id>]
//   [schema_version: <ver>]
//   [meta: field,field,...]
//   records: N
//   type|id|thread_id|step|field...|[meta_fields...]
//
// Design: zero heap allocation for the schema table (static arrays).
// Field encode/decode mirrors _agent.py exactly so Python and Rust
// produce byte-identical output.
// ---------------------------------------------------------------------------

const AGENT_MAGIC: &str = "ULMEN-AGENT v1";

// Record type index -- used as array index into SCHEMAS
const RT_MSG: usize = 0;
const RT_TOOL: usize = 1;
const RT_RES: usize = 2;
const RT_PLAN: usize = 3;
const RT_OBS: usize = 4;
const RT_ERR: usize = 5;
const RT_MEM: usize = 6;
const RT_RAG: usize = 7;
const RT_HYP: usize = 8;
const RT_COT: usize = 9;

// Field type chars: s=string, d=integer, f=float, b=bool
// (required, name, type_char)
struct FieldDef {
    name: &'static str,
    type_char: u8, // b's', b'd', b'f', b'b'
    required: bool,
}

struct Schema {
    name: &'static str,
    fields: &'static [FieldDef],
}

// Total field count = 4 common (type,id,thread_id,step) + type-specific
macro_rules! fd {
    ($n:expr, $t:expr, $r:expr) => {
        FieldDef {
            name: $n,
            type_char: $t,
            required: $r,
        }
    };
}

static SCHEMAS: [Schema; 10] = [
    Schema {
        name: "msg",
        fields: &[
            fd!("role", b's', true),
            fd!("turn", b'd', true),
            fd!("content", b's', true),
            fd!("tokens", b'd', true),
            fd!("flagged", b'b', true),
        ],
    },
    Schema {
        name: "tool",
        fields: &[
            fd!("name", b's', true),
            fd!("args", b's', true),
            fd!("status", b's', true),
        ],
    },
    Schema {
        name: "res",
        fields: &[
            fd!("name", b's', true),
            fd!("data", b's', false),
            fd!("status", b's', true),
            fd!("latency_ms", b'd', true),
        ],
    },
    Schema {
        name: "plan",
        fields: &[
            fd!("index", b'd', true),
            fd!("description", b's', true),
            fd!("status", b's', true),
        ],
    },
    Schema {
        name: "obs",
        fields: &[
            fd!("source", b's', true),
            fd!("content", b's', true),
            fd!("confidence", b'f', true),
        ],
    },
    Schema {
        name: "err",
        fields: &[
            fd!("code", b's', true),
            fd!("message", b's', true),
            fd!("source", b's', true),
            fd!("recoverable", b'b', true),
        ],
    },
    Schema {
        name: "mem",
        fields: &[
            fd!("key", b's', true),
            fd!("value", b's', true),
            fd!("confidence", b'f', true),
            fd!("ttl", b'd', false),
        ],
    },
    Schema {
        name: "rag",
        fields: &[
            fd!("rank", b'd', true),
            fd!("score", b'f', true),
            fd!("source", b's', true),
            fd!("chunk", b's', true),
            fd!("used", b'b', true),
        ],
    },
    Schema {
        name: "hyp",
        fields: &[
            fd!("statement", b's', true),
            fd!("evidence", b's', true),
            fd!("score", b'f', true),
            fd!("accepted", b'b', true),
        ],
    },
    Schema {
        name: "cot",
        fields: &[
            fd!("index", b'd', true),
            fd!("cot_type", b's', true),
            fd!("text", b's', true),
            fd!("confidence", b'f', true),
        ],
    },
];

// Enum validation sets -- mirrors _agent.py _ENUMS exactly
static ENUM_MSG_ROLE: [&str; 3] = ["assistant", "system", "user"];
static ENUM_TOOL_STATUS: [&str; 4] = ["done", "error", "pending", "running"];
static ENUM_RES_STATUS: [&str; 3] = ["done", "error", "timeout"];
static ENUM_PLAN_STATUS: [&str; 4] = ["active", "done", "pending", "skipped"];
static ENUM_COT_TYPE: [&str; 5] = ["compute", "conclude", "observe", "plan", "verify"];

#[inline]
fn enum_contains(set: &[&str], val: &str) -> bool {
    set.binary_search(&val).is_ok()
}

// Resolve record type name to schema index. O(1) via match.
#[inline]
fn schema_index(rtype: &str) -> Option<usize> {
    match rtype {
        "msg" => Some(RT_MSG),
        "tool" => Some(RT_TOOL),
        "res" => Some(RT_RES),
        "plan" => Some(RT_PLAN),
        "obs" => Some(RT_OBS),
        "err" => Some(RT_ERR),
        "mem" => Some(RT_MEM),
        "rag" => Some(RT_RAG),
        "hyp" => Some(RT_HYP),
        "cot" => Some(RT_COT),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Field encoding -- byte-identical to Python _encode_field
// ---------------------------------------------------------------------------

fn agent_encode_field(v: &Bound<'_, PyAny>) -> PyResult<String> {
    if v.is_none() {
        return Ok("N".into());
    }
    // bool must be checked before int (bool is subtype of int in Python)
    if let Ok(b) = v.extract::<bool>() {
        return Ok(if b { "T".into() } else { "F".into() });
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = v.extract::<f64>() {
        if f.is_nan() {
            return Ok("nan".into());
        }
        if f.is_infinite() {
            return Ok(if f > 0.0 { "inf".into() } else { "-inf".into() });
        }
        // Use Python repr-compatible float formatting
        return Ok(format!("{:?}", f));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(agent_encode_str(&s));
    }
    // Fallback: str(v) then escape if needed
    let s = v.str()?.to_string();
    Ok(agent_encode_str(&s))
}

#[inline]
fn agent_encode_str(s: &str) -> String {
    if s.is_empty() {
        return "$0=".into();
    }
    if s.contains('|')
        || s.contains('"')
        || s.contains('\\')
        || s.contains('\n')
        || s.contains('\r')
    {
        let mut out = String::with_capacity(s.len() + 4);
        out.push('"');
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\"\""),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                _ => out.push(c),
            }
        }
        out.push('"');
        out
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Zero-allocation field encoding -- writes directly into caller's buffer.
// Used by encode_agent_record_inner on the hot path.
// ---------------------------------------------------------------------------

#[inline]
fn agent_encode_field_into(v: &Bound<'_, PyAny>, out: &mut String) -> PyResult<()> {
    if v.is_none() {
        out.push('N');
        return Ok(());
    }
    if let Ok(b) = v.extract::<bool>() {
        out.push(if b { 'T' } else { 'F' });
        return Ok(());
    }
    if let Ok(i) = v.extract::<i64>() {
        use std::fmt::Write;
        let _ = write!(out, "{}", i);
        return Ok(());
    }
    if let Ok(f) = v.extract::<f64>() {
        if f.is_nan() {
            out.push_str("nan");
        } else if f.is_infinite() {
            out.push_str(if f > 0.0 { "inf" } else { "-inf" });
        } else {
            use std::fmt::Write;
            let _ = write!(out, "{:?}", f);
        }
        return Ok(());
    }
    if let Ok(s) = v.extract::<String>() {
        agent_encode_str_into(&s, out);
        return Ok(());
    }
    let s = v.str()?.to_string();
    agent_encode_str_into(&s, out);
    Ok(())
}

#[inline]
fn agent_encode_str_into(s: &str, out: &mut String) {
    if s.is_empty() {
        out.push_str("$0=");
        return;
    }
    if s.contains('|')
        || s.contains('"')
        || s.contains('\\')
        || s.contains('\n')
        || s.contains('\r')
    {
        out.push('"');
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\"\""),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                _ => out.push(c),
            }
        }
        out.push('"');
    } else {
        out.push_str(s);
    }
}

// ---------------------------------------------------------------------------
// Field decoding -- byte-identical to Python _decode_field
// ---------------------------------------------------------------------------

fn agent_decode_field<'py>(
    py: Python<'py>,
    tok: &str,
    type_char: u8,
) -> PyResult<Bound<'py, PyAny>> {
    match tok {
        "N" => return Ok(py.None().into_bound(py)),
        "T" => return Ok(true.into_py(py).into_bound(py)),
        "F" => return Ok(false.into_py(py).into_bound(py)),
        "$0=" => return Ok(PyString::new_bound(py, "").into_any()),
        _ => {}
    }
    // Quoted string: starts and ends with '"', len >= 2
    if tok.len() >= 2 && tok.starts_with('"') && tok.ends_with('"') {
        let inner = &tok[1..tok.len() - 1];
        let s = agent_unescape(inner);
        return Ok(PyString::new_bound(py, &s).into_any());
    }
    match type_char {
        b'd' => {
            let i: i64 = tok.parse().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!("Invalid int token: {:?}", tok))
            })?;
            Ok(i.into_py(py).into_bound(py))
        }
        b'f' => {
            let f: f64 = match tok {
                "nan" => f64::NAN,
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                _ => tok.parse().map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid float token: {:?}",
                        tok
                    ))
                })?,
            };
            Ok(f.into_py(py).into_bound(py))
        }
        b'b' => match tok {
            "T" => Ok(true.into_py(py).into_bound(py)),
            "F" => Ok(false.into_py(py).into_bound(py)),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Invalid bool token: {:?}",
                tok
            ))),
        },
        _ => Ok(PyString::new_bound(py, tok).into_any()),
    }
}

#[inline]
fn agent_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some(c2) => {
                    out.push('\\');
                    out.push(c2);
                }
                None => out.push('\\'),
            }
        } else if c == '"' && chars.peek() == Some(&'"') {
            out.push('"');
            chars.next();
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Row splitter -- pipe-aware, RFC 4180 quoting, O(n) single pass
// ---------------------------------------------------------------------------

fn agent_split_row(line: &str) -> Vec<&str> {
    // Fast path: no quotes
    if !line.contains('"') {
        return line.split('|').collect();
    }
    // Slow path: quoted fields may contain pipes
    let bytes = line.as_bytes();
    let n = bytes.len();
    let mut fields: Vec<&str> = Vec::with_capacity(16);
    let mut i = 0usize;

    while i < n {
        if bytes[i] == b'"' {
            // Quoted field: scan to closing quote, handle "" escapes
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'"' {
                    if i + 1 < n && bytes[i + 1] == b'"' {
                        i += 2; // escaped quote
                    } else {
                        i += 1; // closing quote
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            // Include quotes in the token so decode_field can identify it
            let end = i;
            fields.push(&line[start..end]);
            if i < n && bytes[i] == b'|' {
                i += 1;
                // Trailing pipe: push empty field
                if i == n {
                    fields.push("");
                }
            }
        } else {
            // Unquoted field: scan to next pipe
            let start = i;
            while i < n && bytes[i] != b'|' {
                i += 1;
            }
            fields.push(&line[start..i]);
            if i < n && bytes[i] == b'|' {
                i += 1;
                if i == n {
                    fields.push("");
                }
            }
        }
    }
    fields
}

// ---------------------------------------------------------------------------
// encode_agent_record_rust
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Inner encode: takes &[String] to avoid cloning meta_fields per record.
// Writes directly into a String buffer to eliminate per-field allocations.
// All hot-path callers (encode_agent_payload, chunk_payload) use this.
// ---------------------------------------------------------------------------

fn encode_agent_record_inner(rec: &Bound<'_, PyDict>, mf: &[String]) -> PyResult<String> {
    let rtype_obj = rec
        .get_item("type")?
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Record missing 'type' field"))?;
    let rtype: String = rtype_obj.extract()?;

    let si = schema_index(&rtype).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!("Unknown record type: {:?}", rtype))
    })?;
    let schema = &SCHEMAS[si];

    let n_fields = 4 + schema.fields.len() + mf.len();
    let mut out = String::with_capacity(n_fields * 20);

    out.push_str(&rtype);

    // Common fields: id (string), thread_id (string), step (int).
    // Use type-directed extraction: single downcast, no cascade.
    out.push('|');
    encode_field_typed_str(rec, "id", &mut out)?;
    out.push('|');
    encode_field_typed_str(rec, "thread_id", &mut out)?;
    out.push('|');
    encode_field_typed_int(rec, "step", &mut out)?;

    // Type-specific fields: use type_char to pick the right fast path.
    for fd in schema.fields {
        out.push('|');
        match fd.type_char {
            b's' => encode_field_typed_str(rec, fd.name, &mut out)?,
            b'd' => encode_field_typed_int(rec, fd.name, &mut out)?,
            b'f' => encode_field_typed_float(rec, fd.name, &mut out)?,
            b'b' => encode_field_typed_bool(rec, fd.name, &mut out)?,
            _ => encode_field_typed_str(rec, fd.name, &mut out)?,
        }
    }

    // Meta fields (all strings except priority which is int).
    for mf_name in mf {
        out.push('|');
        if mf_name == "priority" {
            encode_field_typed_int(rec, mf_name, &mut out)?;
        } else {
            encode_field_typed_str(rec, mf_name, &mut out)?;
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Type-directed field encoders: one downcast per field, no cascade.
// Each handles None and falls back to the generic path for edge cases.
// ---------------------------------------------------------------------------

#[inline]
fn encode_field_typed_str(rec: &Bound<'_, PyDict>, key: &str, out: &mut String) -> PyResult<()> {
    match rec.get_item(key)? {
        None => {
            out.push('N');
        }
        Some(val) => {
            if val.is_none() {
                out.push('N');
            } else if let Ok(s) = val.downcast::<PyString>() {
                agent_encode_str_into(
                    &s.to_cow().map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!("Invalid UTF-8: {}", e))
                    })?,
                    out,
                );
            } else {
                // Fallback for non-string values in string fields
                let s = val.str()?.to_string();
                agent_encode_str_into(&s, out);
            }
        }
    }
    Ok(())
}

#[inline]
fn encode_field_typed_int(rec: &Bound<'_, PyDict>, key: &str, out: &mut String) -> PyResult<()> {
    match rec.get_item(key)? {
        None => {
            out.push('N');
        }
        Some(val) => {
            if val.is_none() {
                out.push('N');
            } else if let Ok(i) = val.extract::<i64>() {
                use std::fmt::Write;
                let _ = write!(out, "{}", i);
            } else {
                // Fallback
                let s = val.str()?.to_string();
                out.push_str(&s);
            }
        }
    }
    Ok(())
}

#[inline]
fn encode_field_typed_float(rec: &Bound<'_, PyDict>, key: &str, out: &mut String) -> PyResult<()> {
    match rec.get_item(key)? {
        None => {
            out.push('N');
        }
        Some(val) => {
            if val.is_none() {
                out.push('N');
            } else if let Ok(f) = val.extract::<f64>() {
                if f.is_nan() {
                    out.push_str("nan");
                } else if f.is_infinite() {
                    out.push_str(if f > 0.0 { "inf" } else { "-inf" });
                } else {
                    use std::fmt::Write;
                    let _ = write!(out, "{:?}", f);
                }
            } else {
                let s = val.str()?.to_string();
                out.push_str(&s);
            }
        }
    }
    Ok(())
}

#[inline]
fn encode_field_typed_bool(rec: &Bound<'_, PyDict>, key: &str, out: &mut String) -> PyResult<()> {
    match rec.get_item(key)? {
        None => {
            out.push('N');
        }
        Some(val) => {
            if val.is_none() {
                out.push('N');
            } else if let Ok(b) = val.extract::<bool>() {
                out.push(if b { 'T' } else { 'F' });
            } else {
                let s = val.str()?.to_string();
                out.push_str(&s);
            }
        }
    }
    Ok(())
}

/// PyO3 wrapper: converts Option<Vec<String>> to &[String] and delegates.
#[pyfunction]
#[pyo3(signature = (rec, meta_fields=None))]
fn encode_agent_record_rust(
    _py: Python<'_>,
    rec: &Bound<'_, PyDict>,
    meta_fields: Option<Vec<String>>,
) -> PyResult<String> {
    let mf = meta_fields.unwrap_or_default();
    encode_agent_record_inner(rec, &mf)
}

// ---------------------------------------------------------------------------
// decode_agent_record_rust
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (line, meta_fields=None))]
fn decode_agent_record_rust<'py>(
    py: Python<'py>,
    line: &str,
    meta_fields: Option<Vec<String>>,
) -> PyResult<Bound<'py, PyDict>> {
    let mf = meta_fields.unwrap_or_default();

    let fields = agent_split_row(line);
    if fields.is_empty() || (fields.len() == 1 && fields[0].is_empty()) {
        return Err(pyo3::exceptions::PyValueError::new_err("Empty row"));
    }

    let rtype = fields[0];
    let si = schema_index(rtype).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!("Unknown record type: {:?}", rtype))
    })?;
    let schema = &SCHEMAS[si];

    let base_count = 4 + schema.fields.len();
    let meta_count = mf.len();
    let expected = base_count + meta_count;

    if fields.len() != expected {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Row type {:?} expects {} fields, got {}: {:?}",
            rtype,
            expected,
            fields.len(),
            line
        )));
    }

    let d = PyDict::new_bound(py);
    d.set_item("type", rtype)?;

    // Common fields: id(s), thread_id(s), step(d)
    let common_types: [u8; 3] = [b's', b's', b'd'];
    let common_names: [&str; 3] = ["id", "thread_id", "step"];
    for (i, (name, tc)) in common_names.iter().zip(common_types.iter()).enumerate() {
        let tok = fields[1 + i];
        let val = agent_decode_field(py, tok, *tc).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Common field error in row {:?}: {}",
                line, e
            ))
        })?;
        d.set_item(*name, val)?;
    }

    // Type-specific fields
    for (i, fd) in schema.fields.iter().enumerate() {
        let tok = fields[4 + i];
        if fd.required && tok == "N" {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Required field {:?} is null in row {:?}",
                fd.name, line
            )));
        }
        let val = agent_decode_field(py, tok, fd.type_char).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Field {:?} (type={}) error in row {:?}: {}",
                fd.name, fd.type_char as char, line, e
            ))
        })?;
        d.set_item(fd.name, val)?;
    }

    // Meta fields
    for (i, mf_name) in mf.iter().enumerate() {
        let tok = fields[base_count + i];
        let tc = if mf_name == "priority" { b'd' } else { b's' };
        let val = agent_decode_field(py, tok, tc)?;
        d.set_item(mf_name.as_str(), val)?;
    }

    Ok(d)
}

// ---------------------------------------------------------------------------
// Header builder -- produces header lines as a Vec<String>
// ---------------------------------------------------------------------------

struct AgentHeaderOpts<'a> {
    thread_id: Option<&'a str>,
    context_window: Option<i64>,
    context_used: Option<i64>,
    payload_id: Option<&'a str>,
    parent_payload_id: Option<&'a str>,
    agent_id: Option<&'a str>,
    session_id: Option<&'a str>,
    schema_version: Option<&'a str>,
    meta_fields: &'a [String],
    record_count: usize,
}

fn build_header_lines(opts: &AgentHeaderOpts<'_>) -> Vec<String> {
    let mut lines: Vec<String> = Vec::with_capacity(12);
    if let Some(v) = opts.thread_id {
        lines.push(format!("thread: {}", v));
    }
    if let Some(v) = opts.context_window {
        lines.push(format!("context_window: {}", v));
    }
    if let Some(v) = opts.context_used {
        lines.push(format!("context_used: {}", v));
    }
    if let Some(v) = opts.payload_id {
        lines.push(format!("payload_id: {}", v));
    }
    if let Some(v) = opts.parent_payload_id {
        lines.push(format!("parent_payload_id: {}", v));
    }
    if let Some(v) = opts.agent_id {
        lines.push(format!("agent_id: {}", v));
    }
    if let Some(v) = opts.session_id {
        lines.push(format!("session_id: {}", v));
    }
    if let Some(v) = opts.schema_version {
        lines.push(format!("schema_version: {}", v));
    }
    if !opts.meta_fields.is_empty() {
        lines.push(format!("meta: {}", opts.meta_fields.join(",")));
    }
    lines.push(format!("records: {}", opts.record_count));
    lines
}

// Rough token estimate: chars / 4, minimum 1
#[inline]
fn rough_tokens(s: &str) -> i64 {
    ((s.len() as i64) + 3) / 4
}

// ---------------------------------------------------------------------------
// encode_agent_payload_rust
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (
    records,
    thread_id=None,
    context_window=None,
    meta_fields=None,
    auto_context=true,
    enforce_budget=false,
    payload_id=None,
    parent_payload_id=None,
    agent_id=None,
    session_id=None,
    schema_version=None,
    auto_payload_id=false,
))]
fn encode_agent_payload_rust(
    py: Python<'_>,
    records: &Bound<'_, PyList>,
    thread_id: Option<String>,
    context_window: Option<i64>,
    meta_fields: Option<Vec<String>>,
    auto_context: bool,
    enforce_budget: bool,
    payload_id: Option<String>,
    parent_payload_id: Option<String>,
    agent_id: Option<String>,
    session_id: Option<String>,
    schema_version: Option<String>,
    auto_payload_id: bool,
) -> PyResult<String> {
    let mf = meta_fields.unwrap_or_default();

    // Encode data rows using the inner function (no per-record clone of mf).
    let mut data_lines: Vec<String> = Vec::with_capacity(records.len());
    for item in records.iter() {
        let rec: Bound<'_, PyDict> = item.downcast_into()?;
        data_lines.push(encode_agent_record_inner(&rec, &mf)?);
    }

    // Auto payload_id via uuid4
    let effective_payload_id: Option<String> = if auto_payload_id {
        // Generate UUID via Python's uuid module (no external Rust dep needed)
        let uuid_mod = py.import_bound("uuid")?;
        let uuid4 = uuid_mod.call_method0("uuid4")?;
        Some(uuid4.str()?.to_string())
    } else {
        payload_id
    };

    // context_used estimation (rough: chars / 4)
    let context_used: Option<i64> = if auto_context && context_window.is_some() {
        let body: String = data_lines.join("\n");
        Some(rough_tokens(&body).max(1))
    } else {
        None
    };

    // Budget enforcement
    if enforce_budget {
        if let (Some(cw), Some(cu)) = (context_window, context_used) {
            if cu > cw {
                // Raise ContextBudgetExceededError via Python
                let agent_mod = py.import_bound("ulmen.core._agent")?;
                let exc_cls = agent_mod.getattr("ContextBudgetExceededError")?;
                return Err(PyErr::from_value_bound(exc_cls.call1((cw, cu))?.into()));
            }
        }
    }

    let opts = AgentHeaderOpts {
        thread_id: thread_id.as_deref(),
        context_window,
        context_used,
        payload_id: effective_payload_id.as_deref(),
        parent_payload_id: parent_payload_id.as_deref(),
        agent_id: agent_id.as_deref(),
        session_id: session_id.as_deref(),
        schema_version: schema_version.as_deref(),
        meta_fields: &mf,
        record_count: data_lines.len(),
    };

    let header_lines = build_header_lines(&opts);

    // Total capacity: magic + header lines + data lines + newlines
    let total_cap = AGENT_MAGIC.len()
        + header_lines.iter().map(|l| l.len() + 1).sum::<usize>()
        + data_lines.iter().map(|l| l.len() + 1).sum::<usize>()
        + 1;

    let mut out = String::with_capacity(total_cap);
    out.push_str(AGENT_MAGIC);
    out.push('\n');
    for line in &header_lines {
        out.push_str(line);
        out.push('\n');
    }
    for line in &data_lines {
        out.push_str(line);
        out.push('\n');
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Header parser -- forward compatible, silently ignores unknown lines
// Returns (meta_fields, record_count, header_field_count, header_dict)
// ---------------------------------------------------------------------------

struct ParsedHeader {
    thread_id: Option<String>,
    context_window: Option<i64>,
    context_used: Option<i64>,
    payload_id: Option<String>,
    parent_payload_id: Option<String>,
    agent_id: Option<String>,
    session_id: Option<String>,
    schema_version: Option<String>,
    meta_fields: Vec<String>,
    record_count: usize,
    lines_consumed: usize,
}

fn parse_header(lines: &[&str]) -> Result<ParsedHeader, String> {
    let mut h = ParsedHeader {
        thread_id: None,
        context_window: None,
        context_used: None,
        payload_id: None,
        parent_payload_id: None,
        agent_id: None,
        session_id: None,
        schema_version: None,
        meta_fields: Vec::new(),
        record_count: 0,
        lines_consumed: 0,
    };

    let known_meta: [&str; 4] = ["parent_id", "from_agent", "to_agent", "priority"];

    for (idx, &line) in lines.iter().enumerate() {
        if line.starts_with("thread: ") {
            h.thread_id = Some(line[8..].trim().to_string());
        } else if line.starts_with("context_window: ") {
            h.context_window = Some(
                line[16..]
                    .trim()
                    .parse()
                    .map_err(|_| format!("Bad context_window line: {:?}", line))?,
            );
        } else if line.starts_with("context_used: ") {
            h.context_used = Some(
                line[14..]
                    .trim()
                    .parse()
                    .map_err(|_| format!("Bad context_used line: {:?}", line))?,
            );
        } else if line.starts_with("payload_id: ") {
            h.payload_id = Some(line[12..].trim().to_string());
        } else if line.starts_with("parent_payload_id: ") {
            h.parent_payload_id = Some(line[19..].trim().to_string());
        } else if line.starts_with("agent_id: ") {
            h.agent_id = Some(line[10..].trim().to_string());
        } else if line.starts_with("session_id: ") {
            h.session_id = Some(line[12..].trim().to_string());
        } else if line.starts_with("schema_version: ") {
            h.schema_version = Some(line[16..].trim().to_string());
        } else if line.starts_with("meta: ") {
            let raw = &line[6..];
            let fields: Vec<String> = raw
                .split(',')
                .map(|f| f.trim().to_string())
                .filter(|f| !f.is_empty())
                .collect();
            // Validate meta fields
            let unknown: Vec<&str> = fields
                .iter()
                .filter(|f| !known_meta.contains(&f.as_str()))
                .map(|f| f.as_str())
                .collect();
            if !unknown.is_empty() {
                return Err(format!("Unknown meta fields: {:?}", unknown));
            }
            h.meta_fields = fields;
        } else if line.starts_with("records: ") {
            h.record_count = line[9..]
                .trim()
                .parse()
                .map_err(|_| format!("Bad record count: {:?}", line))?;
            h.lines_consumed = idx + 1;
            return Ok(h);
        }
        // Unknown header line: silently ignore (forward compatibility)
    }

    Err("records: not found".to_string())
}

// ---------------------------------------------------------------------------
// decode_agent_payload_rust
// ---------------------------------------------------------------------------

#[pyfunction]
fn decode_agent_payload_rust<'py>(py: Python<'py>, text: &'py str) -> PyResult<Bound<'py, PyList>> {
    let (records, _header) = decode_agent_payload_full_inner(py, text)?;
    Ok(records)
}

// Internal: returns (records, header_dict) -- shared by decode and validate
fn decode_agent_payload_full_inner<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<(Bound<'py, PyList>, ParsedHeader)> {
    // Strip trailing newlines, split
    let trimmed = text.trim_end_matches('\n');
    let all_lines: Vec<&str> = trimmed.split('\n').collect();

    if all_lines.len() < 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Payload too short: missing header lines",
        ));
    }
    if all_lines[0] != AGENT_MAGIC {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Bad magic: expected {:?}, got {:?}",
            AGENT_MAGIC, all_lines[0]
        )));
    }

    let header =
        parse_header(&all_lines[1..]).map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

    let data_start = 1 + header.lines_consumed;
    let data_lines = &all_lines[data_start..];
    let declared_n = header.record_count;
    let actual_n = data_lines.len();

    if declared_n == 0 && actual_n == 0 {
        return Ok((PyList::empty_bound(py), header));
    }

    if actual_n != declared_n {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Record count mismatch: declared {}, found {}",
            declared_n, actual_n
        )));
    }

    let result = PyList::empty_bound(py);
    for (i, &line) in data_lines.iter().enumerate() {
        if line.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Blank line at data row {}",
                i + 1
            )));
        }
        let rec =
            decode_agent_record_rust(py, line, Some(header.meta_fields.clone())).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Row {}: {}", i + 1, e))
            })?;
        result.append(rec)?;
    }

    Ok((result, header))
}

// ---------------------------------------------------------------------------
// validate_agent_payload_rust
//
// Returns (bool, str|None) matching Python validate_agent_payload exactly.
// When structured=True we still return a string (not a ValidationError object)
// because ValidationError is a Python class. Python side wraps if needed.
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (text, structured=false))]
fn validate_agent_payload_rust(
    py: Python<'_>,
    text: &str,
    structured: bool,
) -> PyResult<(bool, PyObject)> {
    // Try to decode first
    let (records, header) = match decode_agent_payload_full_inner(py, text) {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            if structured {
                // Return a ValidationError Python object
                let agent_mod = py.import_bound("ulmen.core._agent")?;
                let ve = agent_mod.getattr("ValidationError")?.call1((
                    &msg,
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    "Check payload structure and header lines",
                ))?;
                return Ok((false, ve.into()));
            }
            return Ok((false, PyString::new_bound(py, &msg).into_any().unbind()));
        }
    };

    // Semantic validation
    // thread_steps: track last step per thread_id for monotonicity check
    let mut thread_steps: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut tool_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut res_ids: Vec<(String, String)> = Vec::new(); // (id, loc)

    let _ = header; // used for meta_fields already

    for i in 0..records.len() {
        let rec: Bound<'_, PyDict> = records.get_item(i)?.downcast_into()?;

        let rtype: String = rec
            .get_item("type")?
            .unwrap_or_else(|| py.None().into_bound(py))
            .extract()
            .unwrap_or_default();
        let rid: String = rec
            .get_item("id")?
            .unwrap_or_else(|| py.None().into_bound(py))
            .extract()
            .unwrap_or_default();
        let tid: String = rec
            .get_item("thread_id")?
            .unwrap_or_else(|| py.None().into_bound(py))
            .extract()
            .unwrap_or_default();
        let step_obj = rec
            .get_item("step")?
            .unwrap_or_else(|| py.None().into_bound(py));
        let step: i64 = step_obj.extract().unwrap_or(0);

        let row = i + 1;

        // thread_id must be non-empty
        if tid.is_empty() {
            let msg = format!("Row {}: thread_id is empty | row={} | field='thread_id' | expected='non-empty string' | got='empty string'", row, row);
            return val_err(
                py,
                &msg,
                structured,
                row,
                "thread_id",
                "non-empty string",
                "empty string",
                "Set thread_id to a unique thread identifier",
            );
        }
        // id must be non-empty
        if rid.is_empty() {
            let msg = format!("Row {}: id is empty | row={} | field='id'", row, row);
            return val_err(
                py,
                &msg,
                structured,
                row,
                "id",
                "non-empty string",
                "empty string",
                "Set id to a unique record identifier",
            );
        }
        // step must be >= 1
        if step < 1 {
            let msg = format!("Row {}: step must be positive integer, got {:?}", row, step);
            return val_err(
                py,
                &msg,
                structured,
                row,
                "step",
                "positive integer >= 1",
                &format!("{:?}", step),
                "step starts at 1 and increments monotonically",
            );
        }
        // step must be non-decreasing within thread
        let prev = *thread_steps.get(&tid).unwrap_or(&0);
        if step < prev {
            let msg = format!(
                "Row {}: step {} is less than previous step {} in thread {:?}",
                row, step, prev, tid
            );
            return val_err(
                py,
                &msg,
                structured,
                row,
                "step",
                &format!(">= {}", prev),
                &step.to_string(),
                "steps must be non-decreasing within a thread",
            );
        }
        thread_steps.insert(tid.clone(), step);

        // Track tool/res for cross-reference
        if rtype == "tool" {
            tool_ids.insert(rid.clone());
        }
        if rtype == "res" {
            res_ids.push((rid.clone(), format!("row {}", row)));
        }

        // Enum validation
        if let Some(err_msg) = validate_enums(py, &rec, &rtype, row, structured)? {
            return Ok(err_msg);
        }
    }

    // res rows must have matching tool rows
    for (res_id, loc) in &res_ids {
        if !tool_ids.contains(res_id) {
            let msg = format!("{}: res id {:?} has no matching tool row", loc, res_id);
            return val_err(
                py,
                &msg,
                structured,
                0,
                "id",
                &format!("tool row with id={:?}", res_id),
                "no matching tool row",
                &format!("Add a tool row with id={:?} before the res row", res_id),
            );
        }
    }

    // Success
    let success_second: PyObject = if structured {
        py.None()
    } else {
        PyString::new_bound(py, "").into_any().unbind()
    };
    Ok((true, success_second))
}

// Build a validation failure return value
fn val_err(
    py: Python<'_>,
    msg: &str,
    structured: bool,
    row: usize,
    field: &str,
    expected: &str,
    got: &str,
    hint: &str,
) -> PyResult<(bool, PyObject)> {
    if structured {
        let agent_mod = py.import_bound("ulmen.core._agent")?;
        let row_obj: PyObject = if row > 0 {
            row.into_py(py).into_bound(py).unbind()
        } else {
            py.None()
        };
        let ve = agent_mod
            .getattr("ValidationError")?
            .call1((msg, row_obj, field, expected, got, hint))?;
        Ok((false, ve.into()))
    } else {
        Ok((false, PyString::new_bound(py, &msg).into_any().unbind()))
    }
}

// Returns None if valid, Some((false, obj)) if invalid
fn validate_enums<'py>(
    py: Python<'py>,
    rec: &Bound<'py, PyDict>,
    rtype: &str,
    row: usize,
    structured: bool,
) -> PyResult<Option<(bool, PyObject)>> {
    // Helper: extract string or bool field as the token representation
    let get_tok = |fname: &str| -> String {
        match rec.get_item(fname) {
            Ok(Some(v)) => {
                if let Ok(b) = v.extract::<bool>() {
                    return if b { "T".to_string() } else { "F".to_string() };
                }
                v.extract::<String>().unwrap_or_default()
            }
            _ => String::new(),
        }
    };

    let check = |fname: &str, valid: &[&str], val: &str| -> Option<String> {
        if val.is_empty() || val == "N" {
            return None;
        } // null = absent, ok
        if !valid.contains(&val) {
            Some(format!(
                "Row {}: field {:?} value {:?} not in {:?}",
                row, fname, val, valid
            ))
        } else {
            None
        }
    };

    let err_msg = match rtype {
        "msg" => check("role", &ENUM_MSG_ROLE, &get_tok("role")),
        "tool" => check("status", &ENUM_TOOL_STATUS, &get_tok("status")),
        "res" => check("status", &ENUM_RES_STATUS, &get_tok("status")),
        "plan" => check("status", &ENUM_PLAN_STATUS, &get_tok("status")),
        "cot" => check("cot_type", &ENUM_COT_TYPE, &get_tok("cot_type")),
        _ => None,
    };

    if let Some(msg) = err_msg {
        let (fname, expected, got) = match rtype {
            "msg" => ("role", ENUM_MSG_ROLE.join("/"), get_tok("role")),
            "tool" => ("status", ENUM_TOOL_STATUS.join("/"), get_tok("status")),
            "res" => ("status", ENUM_RES_STATUS.join("/"), get_tok("status")),
            "plan" => ("status", ENUM_PLAN_STATUS.join("/"), get_tok("status")),
            "cot" => ("cot_type", ENUM_COT_TYPE.join("/"), get_tok("cot_type")),
            _ => ("", String::new(), String::new()),
        };
        let result = val_err(
            py,
            &msg,
            structured,
            row,
            fname,
            &expected,
            &got,
            &format!("Use one of: {}", expected),
        )?;
        return Ok(Some(result));
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Rust unit tests for the agent engine
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// M1c: decode_agent_stream_rust -- streaming line-by-line decoder
// ---------------------------------------------------------------------------

#[pyfunction]
fn decode_agent_stream_rust<'py>(
    py: Python<'py>,
    lines: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    // Collect lines, parse header, then yield records up to record_count.
    // This mirrors the Python generator but returns a list for simplicity
    // in the PyO3 bridge. The hot path (record decoding) is Rust-native.
    let result = PyList::empty_bound(py);
    let mut header: Option<ParsedHeader> = None;
    let mut seen: usize = 0;
    let mut expected: Option<usize> = None;
    let mut raw_header_lines: Vec<String> = Vec::new();

    for item in lines.iter() {
        let raw_line: String = item.extract()?;
        let line = raw_line.trim_end_matches('\n').trim_end_matches('\r');

        if header.is_none() {
            if raw_header_lines.is_empty() {
                if line != AGENT_MAGIC {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "Bad magic: expected {:?}, got {:?}",
                        AGENT_MAGIC, line
                    )));
                }
                raw_header_lines.push(line.to_string());
                continue;
            }
            raw_header_lines.push(line.to_string());
            // Try parsing header (skip magic at index 0)
            let hdr_strs: Vec<&str> = raw_header_lines[1..].iter().map(|s| s.as_str()).collect();
            match parse_header(&hdr_strs) {
                Ok(h) => {
                    expected = Some(h.record_count);
                    header = Some(h);
                }
                Err(e) => {
                    if e.contains("records: not found") {
                        continue; // still buffering header lines
                    }
                    return Err(pyo3::exceptions::PyValueError::new_err(e));
                }
            }
            continue;
        }

        if line.is_empty() {
            continue;
        }

        let h = header.as_ref().unwrap();
        let rec = decode_agent_record_rust(py, line, Some(h.meta_fields.clone())).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Row {}: {}", seen + 1, e))
        })?;
        seen += 1;
        result.append(rec)?;

        if let Some(exp) = expected {
            if seen >= exp {
                break;
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// M1d: compress_context_rust -- context window compression
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (
    records,
    strategy = "completed_sequences",
    keep_priority = 2,
    target_reduction = 0.5,
    keep_types = None,
    window_size = None,
    preserve_cot = false,
))]
fn compress_context_rust<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
    strategy: &str,
    keep_priority: i64,
    target_reduction: f64,
    keep_types: Option<Vec<String>>,
    window_size: Option<usize>,
    preserve_cot: bool,
) -> PyResult<Bound<'py, PyList>> {
    let _ = target_reduction; // informational only
    let n = records.len();
    if n == 0 {
        return Ok(PyList::empty_bound(py));
    }

    match strategy {
        "completed_sequences" => {
            compress_completed_sequences(py, records, keep_priority, preserve_cot)
        }
        "keep_types" => {
            let kt: std::collections::HashSet<String> = keep_types
                .unwrap_or_else(|| vec!["msg".into(), "err".into(), "mem".into()])
                .into_iter()
                .collect();
            let result = PyList::empty_bound(py);
            for item in records.iter() {
                let d: &Bound<'_, PyDict> = item.downcast()?;
                let rtype: String = d
                    .get_item("type")?
                    .map(|v| v.extract().unwrap_or_default())
                    .unwrap_or_default();
                let pri = get_priority(&d);
                if kt.contains(&rtype) || pri <= keep_priority {
                    result.append(item)?;
                }
            }
            Ok(result)
        }
        "sliding_window" => {
            let ws = window_size.unwrap_or_else(|| std::cmp::max(10, n / 4));
            if n <= ws {
                // Return a copy of all records
                let result = PyList::empty_bound(py);
                for item in records.iter() {
                    result.append(item)?;
                }
                return Ok(result);
            }
            // Summarize earlier, keep recent
            let earlier = PyList::empty_bound(py);
            let cutoff = n - ws;
            for i in 0..cutoff {
                earlier.append(records.get_item(i)?)?;
            }
            let summary = summarize_as_mem(py, &earlier)?;
            let result = PyList::empty_bound(py);
            for item in summary.iter() {
                result.append(item)?;
            }
            for i in cutoff..n {
                result.append(records.get_item(i)?)?;
            }
            Ok(result)
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown compression strategy: {:?}",
            strategy
        ))),
    }
}

fn get_priority(rec: &Bound<'_, PyDict>) -> i64 {
    match rec.get_item("priority") {
        Ok(Some(v)) => v.extract::<i64>().unwrap_or(3),
        _ => 3,
    }
}

fn compress_completed_sequences<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
    keep_priority: i64,
    preserve_cot: bool,
) -> PyResult<Bound<'py, PyList>> {
    // Phase 1: find completed tool+res pairs
    let mut tool_ids: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut res_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    for i in 0..records.len() {
        let item_ref = records.get_item(i)?;
        let d: &Bound<'_, PyDict> = item_ref.downcast()?;
        let rtype: String = d
            .get_item("type")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        let rid: String = d
            .get_item("id")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        if rtype == "tool" {
            tool_ids.entry(rid.clone()).or_insert(i);
        }
        if rtype == "res" {
            res_ids.insert(rid);
        }
    }
    let completed: std::collections::HashSet<String> = tool_ids
        .keys()
        .filter(|k| res_ids.contains(*k))
        .cloned()
        .collect();

    let result = PyList::empty_bound(py);
    let mut compressed_tools: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seq_counter: usize = 0;
    let mut cot_counter: usize = 0;
    let meta_names = ["parent_id", "from_agent", "to_agent", "priority"];

    for i in 0..records.len() {
        let item = records.get_item(i)?;
        let d: &Bound<'_, PyDict> = item.downcast()?;
        let rtype: String = d
            .get_item("type")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        let rid: String = d
            .get_item("id")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        let pri = get_priority(d);

        if pri <= keep_priority {
            result.append(item)?;
            continue;
        }

        match rtype.as_str() {
            "msg" | "plan" | "obs" | "err" | "mem" | "hyp" | "rag" => {
                result.append(item)?;
            }
            "tool" if completed.contains(&rid) => {
                if compressed_tools.contains(&rid) {
                    continue;
                }
                compressed_tools.insert(rid.clone());
                seq_counter += 1;
                // Build summary mem record
                let mem = build_compressed_mem(py, d, &rid, seq_counter, &meta_names)?;
                result.append(mem)?;
            }
            "res" if completed.contains(&rid) => {
                continue;
            }
            "cot" => {
                if preserve_cot {
                    cot_counter += 1;
                    let mem = build_cot_mem(py, d, cot_counter, &meta_names)?;
                    result.append(mem)?;
                }
                // else: drop cot
            }
            _ => {
                result.append(item)?;
            }
        }
    }
    Ok(result)
}

fn build_compressed_mem<'py>(
    py: Python<'py>,
    tool_rec: &Bound<'py, PyDict>,
    rid: &str,
    seq: usize,
    meta_names: &[&str],
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("type", "mem")?;
    d.set_item("id", format!("mem_cmp_{:03}", seq))?;
    let tid: String = tool_rec
        .get_item("thread_id")?
        .map(|v| v.extract().unwrap_or_default())
        .unwrap_or_default();
    d.set_item("thread_id", &tid)?;
    let step: i64 = tool_rec
        .get_item("step")?
        .map(|v| v.extract().unwrap_or(0))
        .unwrap_or(0);
    d.set_item("step", step)?;
    let name: String = tool_rec
        .get_item("name")?
        .map(|v| v.extract().unwrap_or_default())
        .unwrap_or_default();
    d.set_item(
        "key",
        format!("tool_result_{}", if name.is_empty() { rid } else { &name }),
    )?;
    d.set_item("value", "")?;
    d.set_item("confidence", 1.0)?;
    d.set_item("ttl", py.None())?;
    for mf in meta_names {
        if let Ok(Some(v)) = tool_rec.get_item(*mf) {
            if !v.is_none() {
                d.set_item(*mf, v)?;
            }
        }
    }
    Ok(d)
}

fn build_cot_mem<'py>(
    py: Python<'py>,
    cot_rec: &Bound<'py, PyDict>,
    counter: usize,
    meta_names: &[&str],
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("type", "mem")?;
    d.set_item("id", format!("mem_cot_{:03}", counter))?;
    let tid: String = cot_rec
        .get_item("thread_id")?
        .map(|v| v.extract().unwrap_or_default())
        .unwrap_or_default();
    d.set_item("thread_id", &tid)?;
    let step: i64 = cot_rec
        .get_item("step")?
        .map(|v| v.extract().unwrap_or(0))
        .unwrap_or(0);
    d.set_item("step", step)?;
    let cot_type: String = cot_rec
        .get_item("cot_type")?
        .map(|v| v.extract().unwrap_or("step".into()))
        .unwrap_or("step".into());
    let index: i64 = cot_rec
        .get_item("index")?
        .map(|v| v.extract().unwrap_or(counter as i64))
        .unwrap_or(counter as i64);
    d.set_item("key", format!("cot_{}_{}", cot_type, index))?;
    let text: String = cot_rec
        .get_item("text")?
        .map(|v| v.extract().unwrap_or_default())
        .unwrap_or_default();
    let truncated = if text.len() > 500 {
        &text[..500]
    } else {
        &text
    };
    d.set_item("value", truncated)?;
    let conf: f64 = cot_rec
        .get_item("confidence")?
        .map(|v| v.extract().unwrap_or(1.0))
        .unwrap_or(1.0);
    d.set_item("confidence", conf)?;
    d.set_item("ttl", py.None())?;
    for mf in meta_names {
        if let Ok(Some(v)) = cot_rec.get_item(*mf) {
            if !v.is_none() {
                d.set_item(*mf, v)?;
            }
        }
    }
    Ok(d)
}

fn summarize_as_mem<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    // Group by thread_id, produce one mem summary per thread
    let mut by_thread: std::collections::HashMap<
        String,
        (usize, std::collections::HashSet<String>, i64),
    > = std::collections::HashMap::new();

    for item in records.iter() {
        let d: &Bound<'_, PyDict> = item.downcast()?;
        let tid: String = d
            .get_item("thread_id")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        let rtype: String = d
            .get_item("type")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        let step: i64 = d
            .get_item("step")?
            .map(|v| v.extract().unwrap_or(0))
            .unwrap_or(0);
        let entry = by_thread
            .entry(tid)
            .or_insert((0, std::collections::HashSet::new(), 0));
        entry.0 += 1;
        entry.1.insert(rtype);
        if step > entry.2 {
            entry.2 = step;
        }
    }

    let result = PyList::empty_bound(py);
    for (tid, (count, types, max_step)) in &by_thread {
        let d = PyDict::new_bound(py);
        d.set_item("type", "mem")?;
        d.set_item("id", format!("mem_summary_{}", tid))?;
        d.set_item("thread_id", tid.as_str())?;
        d.set_item("step", *max_step)?;
        d.set_item("key", format!("context_summary_{}", tid))?;
        let mut sorted_types: Vec<&String> = types.iter().collect();
        sorted_types.sort();
        let types_str = sorted_types
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(",");
        d.set_item(
            "value",
            format!(
                "Compressed {} records (types: {}) up to step {}",
                count, types_str, max_step
            ),
        )?;
        d.set_item("confidence", 0.9)?;
        d.set_item("ttl", py.None())?;
        result.append(d)?;
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// M1e: chunk_payload_rust / merge_chunks_rust
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (
    records,
    token_budget,
    thread_id = None,
    meta_fields = None,
    overlap = 0,
    parent_payload_id = None,
    session_id = None,
))]
fn chunk_payload_rust<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
    token_budget: i64,
    thread_id: Option<String>,
    meta_fields: Option<Vec<String>>,
    overlap: usize,
    parent_payload_id: Option<String>,
    session_id: Option<String>,
) -> PyResult<Bound<'py, PyList>> {
    let mf = meta_fields.unwrap_or_default();
    let n = records.len();

    if n == 0 {
        let empty_payload = encode_agent_payload_rust(
            py,
            &PyList::empty_bound(py),
            thread_id.clone(),
            Some(token_budget),
            Some(mf.clone()),
            true,
            false,
            None,
            None,
            None,
            session_id.clone(),
            None,
            true,
        )?;
        let result = PyList::empty_bound(py);
        result.append(empty_payload)?;
        return Ok(result);
    }

    // Group records into atomic units (tool+res pairs kept together)
    let mut units: Vec<Vec<usize>> = Vec::new(); // indices into records
    let mut pending_tools: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();

    for i in 0..n {
        let _item_tmp = records.get_item(i)?;
        let d: &Bound<'_, PyDict> = _item_tmp.downcast()?;
        let rtype: String = d
            .get_item("type")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();
        let rid: String = d
            .get_item("id")?
            .map(|v| v.extract().unwrap_or_default())
            .unwrap_or_default();

        if rtype == "tool" {
            let unit_idx = units.len();
            units.push(vec![i]);
            pending_tools.entry(rid).or_default().push(unit_idx);
        } else if rtype == "res" {
            if let Some(tool_units) = pending_tools.get_mut(&rid) {
                if let Some(unit_idx) = tool_units.first().copied() {
                    units[unit_idx].push(i);
                    tool_units.remove(0);
                    if tool_units.is_empty() {
                        pending_tools.remove(&rid);
                    }
                    continue;
                }
            }
            units.push(vec![i]);
        } else {
            units.push(vec![i]);
        }
    }

    // Estimate token cost per unit (no per-record clone of mf).
    let unit_costs: Vec<i64> = units
        .iter()
        .map(|unit| {
            unit.iter()
                .map(|&idx| {
                    let _item_tmp = records.get_item(idx).unwrap();
                    let d: &Bound<'_, PyDict> = _item_tmp.downcast().unwrap();
                    let row = encode_agent_record_inner(d, &mf).unwrap_or_default();
                    rough_tokens(&row).max(1)
                })
                .sum()
        })
        .collect();

    // Header overhead estimate
    let header_sample = format!(
        "{}\nthread: {}\ncontext_window: {}\nrecords: 9999\n",
        AGENT_MAGIC,
        thread_id.as_deref().unwrap_or(""),
        token_budget
    );
    let header_overhead = rough_tokens(&header_sample);
    let effective_budget = std::cmp::max(10, token_budget - header_overhead);

    // Pack units into chunks
    let uuid_mod = py.import_bound("uuid")?;
    let result = PyList::empty_bound(py);
    let mut current_indices: Vec<usize> = Vec::new(); // unit indices
    let mut current_tokens: i64 = 0;
    let mut prev_payload_id = parent_payload_id;

    let flush = |py: Python<'_>,
                 unit_indices: &[usize],
                 prev_id: Option<String>,
                 units: &[Vec<usize>],
                 records: &Bound<'_, PyList>,
                 mf: &[String],
                 thread_id: &Option<String>,
                 token_budget: i64,
                 session_id: &Option<String>,
                 uuid_mod: &Bound<'_, PyAny>|
     -> PyResult<(String, String)> {
        let flat = PyList::empty_bound(py);
        for &ui in unit_indices {
            for &ri in &units[ui] {
                flat.append(records.get_item(ri)?)?;
            }
        }
        let pid = uuid_mod.call_method0("uuid4")?.str()?.to_string();
        let payload = encode_agent_payload_rust(
            py,
            &flat,
            thread_id.clone(),
            Some(token_budget),
            Some(mf.to_vec()),
            true,
            false,
            Some(pid.clone()),
            prev_id,
            None,
            session_id.clone(),
            None,
            false,
        )?;
        Ok((payload, pid))
    };

    for (ui, cost) in unit_costs.iter().enumerate() {
        if !current_indices.is_empty() && current_tokens + cost > effective_budget {
            let (payload, pid) = flush(
                py,
                &current_indices,
                prev_payload_id.clone(),
                &units,
                records,
                &mf,
                &thread_id,
                token_budget,
                &session_id,
                &uuid_mod,
            )?;
            result.append(payload)?;
            prev_payload_id = Some(pid);
            if overlap > 0 && current_indices.len() > overlap {
                let start = current_indices.len() - overlap;
                current_indices = current_indices[start..].to_vec();
                current_tokens = current_indices.iter().map(|&idx| unit_costs[idx]).sum();
            } else {
                current_indices.clear();
                current_tokens = 0;
            }
        }
        current_indices.push(ui);
        current_tokens += cost;
    }

    if !current_indices.is_empty() {
        let (payload, _) = flush(
            py,
            &current_indices,
            prev_payload_id,
            &units,
            records,
            &mf,
            &thread_id,
            token_budget,
            &session_id,
            &uuid_mod,
        )?;
        result.append(payload)?;
    }

    Ok(result)
}

#[pyfunction]
fn merge_chunks_rust<'py>(
    py: Python<'py>,
    payloads: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let mut seen: std::collections::HashSet<(String, String, i64)> =
        std::collections::HashSet::new();
    let result = PyList::empty_bound(py);

    for payload_item in payloads.iter() {
        let text: String = payload_item.extract()?;
        let records = decode_agent_payload_rust(py, &text)?;
        for item in records.iter() {
            let d: &Bound<'_, PyDict> = item.downcast()?;
            let rid: String = d
                .get_item("id")?
                .map(|v| v.extract().unwrap_or_default())
                .unwrap_or_default();
            let tid: String = d
                .get_item("thread_id")?
                .map(|v| v.extract().unwrap_or_default())
                .unwrap_or_default();
            let step: i64 = d
                .get_item("step")?
                .map(|v| v.extract().unwrap_or(0))
                .unwrap_or(0);
            let key = (rid, tid, step);
            if seen.insert(key) {
                result.append(item)?;
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// M1f: parse_llm_output_rust -- repair malformed LLM output
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (raw_text, thread_id = None, strict = false))]
fn parse_llm_output_rust(
    py: Python<'_>,
    raw_text: &str,
    thread_id: Option<String>,
    strict: bool,
) -> PyResult<String> {
    // Strip markdown fences
    let lines: Vec<&str> = raw_text
        .lines()
        .filter(|l| !l.trim().starts_with("```"))
        .collect();
    let cleaned = lines.join("\n");
    let cleaned = cleaned.trim();

    // Find magic line
    let all_lines: Vec<&str> = cleaned.lines().collect();
    let magic_idx = all_lines.iter().position(|l| l.trim() == AGENT_MAGIC);

    let repair_tid = thread_id.as_deref().unwrap_or("REPAIR");

    if magic_idx.is_none() {
        let msg = format!("No '{}' magic line found in LLM output", AGENT_MAGIC);
        if strict {
            return Err(pyo3::exceptions::PyValueError::new_err(msg));
        }
        return make_error_payload(py, &msg, repair_tid);
    }

    let mi = magic_idx.unwrap();
    let work_lines = &all_lines[mi..];

    // Separate header from data
    let mut header_lines: Vec<String> = Vec::new();
    let mut data_lines: Vec<String> = Vec::new();
    let mut past_records = false;
    let is_data_line = |s: &str| -> bool {
        if s.is_empty() {
            return false;
        }
        let first = s.split('|').next().unwrap_or("").trim();
        schema_index(first).is_some()
    };
    let header_prefixes = [
        "thread: ",
        "context_window: ",
        "context_used: ",
        "payload_id: ",
        "parent_payload_id: ",
        "agent_id: ",
        "session_id: ",
        "schema_version: ",
        "meta: ",
        "records: ",
    ];
    let is_header_line = |s: &str| -> bool { header_prefixes.iter().any(|p| s.starts_with(p)) };

    for &line in &work_lines[1..] {
        let stripped = line.trim();
        if stripped.is_empty() {
            continue;
        }
        if !past_records && is_header_line(stripped) {
            header_lines.push(stripped.to_string());
            if stripped.starts_with("records: ") {
                past_records = true;
            }
        } else if is_data_line(stripped) {
            data_lines.push(stripped.to_string());
        } else if !past_records {
            header_lines.push(stripped.to_string());
        }
    }

    // Fix record count
    let actual_count = data_lines.len();
    let mut found_records = false;
    for h in header_lines.iter_mut() {
        if h.starts_with("records: ") {
            *h = format!("records: {}", actual_count);
            found_records = true;
        }
    }
    if !found_records {
        header_lines.push(format!("records: {}", actual_count));
    }

    // Reassemble
    let mut repaired = String::with_capacity(256);
    repaired.push_str(AGENT_MAGIC);
    repaired.push('\n');
    for h in &header_lines {
        repaired.push_str(h);
        repaired.push('\n');
    }
    for d in &data_lines {
        repaired.push_str(d);
        repaired.push('\n');
    }

    // Validate
    let (ok, _) = validate_agent_payload_rust(py, &repaired, false)?;
    if ok {
        return Ok(repaired);
    }

    // Last resort: decode individual rows and re-encode good ones
    let mut meta_fields: Vec<String> = Vec::new();
    for h in &header_lines {
        if h.starts_with("meta: ") {
            meta_fields = h[6..]
                .split(',')
                .map(|f| f.trim().to_string())
                .filter(|f| !f.is_empty())
                .collect();
        }
    }

    let mut good_records: Vec<Bound<'_, PyDict>> = Vec::new();
    for row in &data_lines {
        match decode_agent_record_rust(py, row, Some(meta_fields.clone())) {
            Ok(rec) => good_records.push(rec),
            Err(_) => continue,
        }
    }

    if good_records.is_empty() {
        let msg = "Could not repair LLM output";
        if strict {
            return Err(pyo3::exceptions::PyValueError::new_err(msg));
        }
        return make_error_payload(py, msg, repair_tid);
    }

    let good_list = PyList::empty_bound(py);
    for rec in &good_records {
        good_list.append(rec)?;
    }

    match encode_agent_payload_rust(
        py,
        &good_list,
        thread_id.clone(),
        None,
        Some(meta_fields),
        true,
        false,
        None,
        None,
        None,
        None,
        None,
        false,
    ) {
        Ok(result) => {
            let (ok2, _) = validate_agent_payload_rust(py, &result, false)?;
            if ok2 {
                return Ok(result);
            }
            let msg = "Repair produced invalid payload";
            if strict {
                return Err(pyo3::exceptions::PyValueError::new_err(msg));
            }
            make_error_payload(py, msg, repair_tid)
        }
        Err(e) => {
            let msg = format!("Repair failed: {}", e);
            if strict {
                return Err(pyo3::exceptions::PyValueError::new_err(msg));
            }
            make_error_payload(py, &msg, repair_tid)
        }
    }
}

fn make_error_payload(py: Python<'_>, msg: &str, thread_id: &str) -> PyResult<String> {
    let rec = PyDict::new_bound(py);
    rec.set_item("type", "err")?;
    rec.set_item("id", "er_val_001")?;
    rec.set_item("thread_id", thread_id)?;
    rec.set_item("step", 1)?;
    rec.set_item("code", "VALIDATION_FAILED")?;
    rec.set_item("message", msg)?;
    rec.set_item("source", "validator")?;
    rec.set_item("recoverable", false)?;
    let records = PyList::empty_bound(py);
    records.append(rec)?;
    encode_agent_payload_rust(
        py, &records, None, None, None, true, false, None, None, None, None, None, false,
    )
}

// ---------------------------------------------------------------------------
// M1g: count_tokens_exact_rust / count_tokens_exact_records_rust
//
// Uses the existing bpe_split + bpe_chunk_tokens already in this file.
// ---------------------------------------------------------------------------

#[pyfunction]
fn count_tokens_exact_rust(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    let chunks = bpe_split(text);
    if chunks.is_empty() {
        return std::cmp::max(1, (text.len() + 3) / 4);
    }
    chunks.iter().map(|c| bpe_chunk_tokens(c)).sum()
}

#[pyfunction]
#[pyo3(signature = (text, per_record_overhead = 3))]
fn count_tokens_exact_records_rust(text: &str, per_record_overhead: usize) -> usize {
    let base = count_tokens_exact_rust(text);
    let n_rows = text.matches('\n').count().saturating_sub(3);
    base + n_rows * per_record_overhead
}

#[cfg(test)]
mod agent_tests {
    use super::*;

    #[test]
    fn schema_index_all_types() {
        assert_eq!(schema_index("msg"), Some(RT_MSG));
        assert_eq!(schema_index("tool"), Some(RT_TOOL));
        assert_eq!(schema_index("res"), Some(RT_RES));
        assert_eq!(schema_index("plan"), Some(RT_PLAN));
        assert_eq!(schema_index("obs"), Some(RT_OBS));
        assert_eq!(schema_index("err"), Some(RT_ERR));
        assert_eq!(schema_index("mem"), Some(RT_MEM));
        assert_eq!(schema_index("rag"), Some(RT_RAG));
        assert_eq!(schema_index("hyp"), Some(RT_HYP));
        assert_eq!(schema_index("cot"), Some(RT_COT));
        assert_eq!(schema_index("bad"), None);
    }

    #[test]
    fn field_counts_match_python() {
        // Python FIELD_COUNTS = {t: 4 + len(s) for t, s in _SCHEMAS.items()}
        assert_eq!(4 + SCHEMAS[RT_MSG].fields.len(), 9);
        assert_eq!(4 + SCHEMAS[RT_TOOL].fields.len(), 7);
        assert_eq!(4 + SCHEMAS[RT_RES].fields.len(), 8);
        assert_eq!(4 + SCHEMAS[RT_PLAN].fields.len(), 7);
        assert_eq!(4 + SCHEMAS[RT_OBS].fields.len(), 7);
        assert_eq!(4 + SCHEMAS[RT_ERR].fields.len(), 8);
        assert_eq!(4 + SCHEMAS[RT_MEM].fields.len(), 8);
        assert_eq!(4 + SCHEMAS[RT_RAG].fields.len(), 9);
        assert_eq!(4 + SCHEMAS[RT_HYP].fields.len(), 8);
        assert_eq!(4 + SCHEMAS[RT_COT].fields.len(), 8);
    }

    #[test]
    fn split_row_simple() {
        let r = agent_split_row("a|b|c");
        assert_eq!(r, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_row_no_pipe() {
        let r = agent_split_row("abc");
        assert_eq!(r, vec!["abc"]);
    }

    #[test]
    fn split_row_quoted_pipe() {
        let r = agent_split_row(r#"a|"b|c"|d"#);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0], "a");
        assert_eq!(r[1], "\"b|c\"");
        assert_eq!(r[2], "d");
    }

    #[test]
    fn split_row_trailing_pipe() {
        let r = agent_split_row("a|b|");
        assert_eq!(r, vec!["a", "b", ""]);
    }

    #[test]
    fn split_row_empty() {
        let r = agent_split_row("");
        assert_eq!(r, vec![""]);
    }

    #[test]
    fn encode_str_empty() {
        assert_eq!(agent_encode_str(""), "$0=");
    }

    #[test]
    fn encode_str_safe() {
        assert_eq!(agent_encode_str("hello"), "hello");
    }

    #[test]
    fn encode_str_with_pipe() {
        assert_eq!(agent_encode_str("a|b"), "\"a|b\"");
    }

    #[test]
    fn encode_str_with_quote() {
        assert_eq!(agent_encode_str("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn encode_str_with_newline() {
        assert_eq!(agent_encode_str("a\nb"), "\"a\\nb\"");
    }

    #[test]
    fn unescape_double_quote() {
        assert_eq!(agent_unescape("say \"\"hi\"\""), "say \"hi\"");
    }

    #[test]
    fn unescape_backslash_n() {
        assert_eq!(agent_unescape("a\\nb"), "a\nb");
    }

    #[test]
    fn parse_header_basic() {
        let lines = ["records: 3"];
        let h = parse_header(&lines).unwrap();
        assert_eq!(h.record_count, 3);
        assert_eq!(h.lines_consumed, 1);
    }

    #[test]
    fn parse_header_all_fields() {
        let lines = [
            "thread: t1",
            "context_window: 8000",
            "context_used: 42",
            "payload_id: p1",
            "parent_payload_id: p0",
            "agent_id: ag1",
            "session_id: s1",
            "schema_version: 1.0.0",
            "meta: from_agent,to_agent",
            "records: 5",
        ];
        let h = parse_header(&lines).unwrap();
        assert_eq!(h.thread_id.as_deref(), Some("t1"));
        assert_eq!(h.context_window, Some(8000));
        assert_eq!(h.context_used, Some(42));
        assert_eq!(h.payload_id.as_deref(), Some("p1"));
        assert_eq!(h.parent_payload_id.as_deref(), Some("p0"));
        assert_eq!(h.agent_id.as_deref(), Some("ag1"));
        assert_eq!(h.session_id.as_deref(), Some("s1"));
        assert_eq!(h.schema_version.as_deref(), Some("1.0.0"));
        assert_eq!(h.meta_fields, vec!["from_agent", "to_agent"]);
        assert_eq!(h.record_count, 5);
        assert_eq!(h.lines_consumed, 10);
    }

    #[test]
    fn parse_header_forward_compat() {
        let lines = ["future_field: xyz", "records: 2"];
        let h = parse_header(&lines).unwrap();
        assert_eq!(h.record_count, 2);
    }

    #[test]
    fn parse_header_bad_records_line() {
        let lines = ["records: abc"];
        assert!(parse_header(&lines).is_err());
    }

    #[test]
    fn parse_header_bad_context_window() {
        let lines = ["context_window: abc", "records: 0"];
        assert!(parse_header(&lines).is_err());
    }

    #[test]
    fn parse_header_unknown_meta_field() {
        let lines = ["meta: not_real", "records: 0"];
        assert!(parse_header(&lines).is_err());
    }

    #[test]
    fn enum_sets_sorted() {
        // Binary search requires sorted order
        assert!(ENUM_MSG_ROLE.windows(2).all(|w| w[0] <= w[1]));
        assert!(ENUM_TOOL_STATUS.windows(2).all(|w| w[0] <= w[1]));
        assert!(ENUM_RES_STATUS.windows(2).all(|w| w[0] <= w[1]));
        assert!(ENUM_PLAN_STATUS.windows(2).all(|w| w[0] <= w[1]));
        assert!(ENUM_COT_TYPE.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn enum_contains_valid() {
        assert!(enum_contains(&ENUM_MSG_ROLE, "user"));
        assert!(enum_contains(&ENUM_MSG_ROLE, "assistant"));
        assert!(enum_contains(&ENUM_MSG_ROLE, "system"));
        assert!(!enum_contains(&ENUM_MSG_ROLE, "robot"));
    }

    #[test]
    fn magic_constant() {
        assert_eq!(AGENT_MAGIC, "ULMEN-AGENT v1");
    }

    #[test]
    fn count_tokens_exact_empty() {
        assert_eq!(count_tokens_exact_rust(""), 0);
    }

    #[test]
    fn count_tokens_exact_short() {
        assert!(count_tokens_exact_rust("hello") >= 1);
    }

    #[test]
    fn count_tokens_exact_records_overhead() {
        let text = "ULMEN-AGENT v1\nrecords: 2\nrow1\nrow2\n";
        let base = count_tokens_exact_rust(text);
        let with_oh = count_tokens_exact_records_rust(text, 5);
        assert!(with_oh >= base);
    }
}

#[pymodule]
fn _ulmen_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<UlmenDictRust>()?;
    m.add_class::<UlmenDictFullRust>()?;
    m.add_class::<UlmenStreamEncoder>()?;
    m.add("VERSION", "1.0.0")?;
    m.add("EDITION", "ULMEN V1")?;
    m.add_function(wrap_pyfunction!(decode_binary_records_rust, m)?)?;
    m.add_function(wrap_pyfunction!(encode_ulmen_llm_rust, m)?)?;
    m.add_function(wrap_pyfunction!(decode_ulmen_llm_rust, m)?)?;
    m.add_function(wrap_pyfunction!(encode_binary_stream_chunked, m)?)?;
    m.add_function(wrap_pyfunction!(count_tokens, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_tokens, m)?)?;
    m.add_function(wrap_pyfunction!(from_json, m)?)?;
    m.add_function(wrap_pyfunction!(to_json, m)?)?;
    m.add_function(wrap_pyfunction!(compare_sizes, m)?)?;
    m.add_function(wrap_pyfunction!(encode_agent_field, m)?)?;
    m.add_function(wrap_pyfunction!(decode_agent_field, m)?)?;
    m.add_function(wrap_pyfunction!(encode_agent_record_rust, m)?)?;
    m.add_function(wrap_pyfunction!(decode_agent_record_rust, m)?)?;
    m.add_function(wrap_pyfunction!(encode_agent_payload_rust, m)?)?;
    m.add_function(wrap_pyfunction!(decode_agent_payload_rust, m)?)?;
    m.add_function(wrap_pyfunction!(validate_agent_payload_rust, m)?)?;
    m.add_function(wrap_pyfunction!(decode_agent_stream_rust, m)?)?;
    m.add_function(wrap_pyfunction!(compress_context_rust, m)?)?;
    m.add_function(wrap_pyfunction!(chunk_payload_rust, m)?)?;
    m.add_function(wrap_pyfunction!(merge_chunks_rust, m)?)?;
    m.add_function(wrap_pyfunction!(parse_llm_output_rust, m)?)?;
    m.add_function(wrap_pyfunction!(count_tokens_exact_rust, m)?)?;
    m.add_function(wrap_pyfunction!(count_tokens_exact_records_rust, m)?)?;
    Ok(())
}
