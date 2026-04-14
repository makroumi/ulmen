//! LUMEN V1 — Lightweight Universal Minimal Encoding Notation
//! Rust acceleration layer (PyO3)
//!
//! Copyright (c) El Mehdi Makroumi. All rights reserved.
//! Proprietary and confidential.

#![allow(dead_code)]
#![allow(unused_imports)]

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
// Wire-format constants — byte-identical to lumen/core/_constants.py
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
                for j in 0..u {
                    if small[j] == si {
                        continue 'outer;
                    }
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
// LumenCore — all precomputed encodings
// ---------------------------------------------------------------------------

struct LumenCore {
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

impl LumenCore {
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
        for ci in 0..nc {
            col_types.push(col_type_char(&cd[ci]));
            col_strats.push(detect_strat(&cd[ci]));
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
struct LumenDictRust {
    core: LumenCore,
    opt: bool,
}

#[pymethods]
impl LumenDictRust {
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
            core: LumenCore::build(lst, pool_size_limit, 6)?,
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

    fn encode_lumen_llm(&self, py: Python<'_>) -> PyResult<String> {
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
        encode_lumen_llm_rust(py, &list)
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
            "LumenDictRust(records={}, pool={}, optimizations={})",
            self.core.n_rows,
            self.core.pool.len(),
            self.opt,
        )
    }
}

#[pyclass]
struct LumenDictFullRust {
    core: LumenCore,
    lim: usize,
}

#[pymethods]
impl LumenDictFullRust {
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
            core: LumenCore::build(lst, pool_size_limit, 6)?,
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

    fn encode_lumen_llm(&self, py: Python<'_>) -> PyResult<String> {
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
        encode_lumen_llm_rust(py, &list)
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
            "LumenDictFullRust(records={}, pool={}, pool_limit={})",
            self.core.n_rows,
            self.core.pool.len(),
            self.lim,
        )
    }
}

// ---------------------------------------------------------------------------
// LumenStreamEncoder — streaming binary encode, no full materialisation
// ---------------------------------------------------------------------------
//
// Usage (Python):
//   enc = LumenStreamEncoder(pool_size_limit=64)
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
struct LumenStreamEncoder {
    rows: Vec<Vec<ColVal>>, // rows[row_idx][col_idx]
    keys: Vec<u32>,
    st: StringTable,
    max_pool: usize,
    chunk_size: usize,
    has_schema: bool,
}

#[pymethods]
impl LumenStreamEncoder {
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

        for ci in 0..nc {
            let key = self.st.get(self.keys[ci]);
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
        let payload = LumenCore::assemble_binary(
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
            "LumenStreamEncoder(records_buffered={}, cols={}, pool_limit={})",
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
// window as a complete self-contained LUMEN binary payload, and returns
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
        let core = LumenCore::build(&slice, pool_size_limit, 6)?;
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
                for ci in 0..n_cols {
                    col_data.push(read_column(&mut cur, py, &pool, col_strats[ci], n_rows)?);
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
// LUMIA encoder / decoder (unchanged, correct and complete)
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
fn encode_lumen_llm_rust(_py: Python<'_>, data: &Bound<'_, PyList>) -> PyResult<String> {
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
// LUMIA decoder helpers
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
fn decode_lumen_llm_rust<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyList>> {
    if text.len() < 2 || !text.starts_with("L|") {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Not a LUMEN LLM payload: must start with 'L|'",
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
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn _lumen_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LumenDictRust>()?;
    m.add_class::<LumenDictFullRust>()?;
    m.add_class::<LumenStreamEncoder>()?;
    m.add("VERSION", "1.0.0")?;
    m.add("EDITION", "LUMEN V1")?;
    m.add_function(wrap_pyfunction!(decode_binary_records_rust, m)?)?;
    m.add_function(wrap_pyfunction!(encode_lumen_llm_rust, m)?)?;
    m.add_function(wrap_pyfunction!(decode_lumen_llm_rust, m)?)?;
    m.add_function(wrap_pyfunction!(encode_binary_stream_chunked, m)?)?;
    Ok(())
}
