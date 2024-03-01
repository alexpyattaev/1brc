use std::collections::HashMap;

use std::{fmt::Display, fs::File};

#[derive(Debug)]
struct State {
    min: f32,
    max: f32,
    count: u64,
    sum: f32,
}

impl Default for State {
    #[inline(always)]
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            count: 0,
            sum: 0.0,
        }
    }
}

impl Display for State {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avg = self.sum / (self.count as f32);
        write!(f, "{:.1}/{avg:.1}/{:.1}", self.min, self.max)
    }
}

impl State {
    #[inline(always)]
    fn update(&mut self, v: f32) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.count += 1;
        self.sum += v;
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.count += other.count;
        self.sum += other.sum;
    }
}

struct MyString {
    buf: [u8; 100],
    len: usize,
}

impl MyString {
    fn new(src: &[u8]) -> Self {
        let mut b = [0; 100];
        b[0..src.len()].copy_from_slice(src);
        Self {
            buf: b,
            len: src.len(),
        }
    }
}

impl std::ops::Deref for MyString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        unsafe {
            let s = std::str::from_utf8_unchecked(&self.buf[0..self.len]);
            s
        }
    }
}

impl std::hash::Hash for MyString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&self.buf[0..self.len]);
    }
}

///Makes str from u8 through unsafe cast
#[inline(always)]
fn make_str(x: &[u8]) -> &str {
    unsafe {
        let s = std::str::from_utf8_unchecked(x);
        s
    }
}

///Interns an str in a vector for later use. Cheap-o-bump-alloc, essentially.
#[inline(always)]
fn intern_str(stringstore: &mut Vec<u8>, x: &str) -> &'static str {
    let start = stringstore.len();
    let len = x.len();
    // let spare_cap = stringstore.spare_capacity_mut();
    // if spare_cap.len() < len{
    //     println!("Stringstore len is {}", stringstore.len());
    //     panic!("Not enuf string store!");
    // }
    stringstore.extend_from_slice(x.as_bytes());    
    unsafe {
        let x = std::str::from_utf8_unchecked(&stringstore[start..start + len]);
        (x as *const str).as_ref().unwrap_unchecked()
    }
}

#[inline(always)]
fn unintern_str(stringstore: &mut Vec<u8>, x: &str) {
    unsafe {
        stringstore.set_len(stringstore.len() - x.len());
    }
}
#[inline(always)]
fn parse_stuff(
    stringstore: &mut Vec<u8>,
    line: &[u8],
) -> Result<(&'static str, f32), &'static str> {
    let mut parts = line.split(|&e| e == b';');

    let name = make_str(parts.next().ok_or("there must be a city name")?);
    let name = intern_str(stringstore, name);

    let value = make_str(parts.next().ok_or("there must be a value")?);
    let value: f32 = value.parse().map_err(|_| "Value must be a float")?;
    Ok((name, value))
}

//#[inline(always)]
fn parse_stuff_fast(
    stringstore: &mut Vec<u8>,
    line: &[u8],
) -> (&'static str, f32){
    let mut parts = line.split(|&e| e == b';');
    unsafe{
    let name = make_str(parts.next().unwrap_unchecked());
    let name = intern_str(stringstore, name);

    let value = make_str(parts.next().unwrap_unchecked());
    let mut vparts = value.split(|e| e== '.');
    let vint:i32 = vparts.next().unwrap_unchecked().parse().unwrap_unchecked();
    let vfrac:u32 = vparts.next().unwrap_unchecked().parse().unwrap_unchecked();
    let value:f32 = vint as f32 + (vfrac as f32 ) / 10.0;
    (name, value)
    }
}


fn make_map<'a>(
    stringstore: &mut Vec<u8>,
    i: impl Iterator<Item = &'a [u8]>,
) -> HashMap<&'static str, State> {
    let mut state: HashMap<&'static str, State> = HashMap::with_capacity(1024);
    for line in i {
        if line.len() == 0 {
            continue;
        }
        /*let (name, value) = match parse_stuff(stringstore, line) {
            Ok(v) => v,
            Err(e) => {
                println!("Got error {e}");
                println!("while parsing line {:?}", line);
                panic!("Cant work");
            }
        };*/
        let (name, value) = parse_stuff_fast(stringstore, line);
        state
            .entry(name)
            .and_modify(|v| {
                v.update(value);
                unintern_str(stringstore, name);
            })
            .or_insert({
                let mut s = State::default();
                s.update(value);
                s
            });
    }
    state
}


fn solve_for_part(stringstore: &mut Vec<u8>, mem: &[u8]) -> HashMap<&'static str, State> {    
    let iter = mem.split(|&e| e == b'\n');
    make_map(stringstore, iter)
}


fn merge(a: &mut HashMap<&'static str, State>, b: &HashMap<&'static str, State>) {
    for (k, v) in b {
        a.entry(k).or_default().merge(v);
    }
}

fn main() {
    let avail_cores: usize = std::thread::available_parallelism().unwrap().into();
    let cores = avail_cores;
    
    // malloc is for the weak, we will allocate a string storage bump allocator per thread.
    // This allows us to pretend that everything is 'static str.
    let mut stringstores: Vec<_> = (0..cores)
        .map(|_| Vec::with_capacity(1024 * 1024 * 128))
        .collect();

    let path = match std::env::args().skip(1).next() {
        Some(path) => path,
        None => "measurements.txt".to_owned(),
    };
    let file = File::open(path).unwrap();
    let metadata = file.metadata().unwrap();
    let total_len = metadata.len() as usize;
    
    let chunk_size = total_len / cores;

    let bias = 1000 * 1000;
    let mut chunk_size = chunk_size - bias * cores;
    //let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file).unwrap() };
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
    

    let mut chunks: Vec<_> = Vec::with_capacity(cores);
    let search_area = 256;
    let mut start = 0;
    for _ in 0..cores - 1 {
        let end = start + chunk_size;
        chunk_size += bias;
        let slice = &mmap[end..end + search_area];
        let mut iter = slice.into_iter().enumerate();
        let next_new_line = loop {
            let (idx, &c) = iter.next().unwrap();
            if c == b'\n' {
                break idx;
            }
        };

        let end = end + next_new_line;
        chunks.push(&mmap[start..end]);
        start = end + 1;
    }
    chunks.push(&mmap[start..mmap.len()]);

    let state = std::thread::scope(|s| {
        let join_handles: Vec<_> = chunks
            .into_iter()
            .zip(stringstores.iter_mut())
            .map(|(ch, ss)| s.spawn(|| solve_for_part(ss, ch)))
            .collect();

        let mut state = HashMap::<&'static str, State>::with_capacity(1024*8);

        for jh in join_handles {
            let res = jh.join().unwrap();
            //dbg!("Merging data...", res.len());
            merge(&mut state, &res);
            //dbg!("Merged!");
        }
        state
    });

    
    let mut all: Vec<_> = state.into_iter().collect();
    all.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    print!("{{");
    for (i, (name, state)) in all.into_iter().enumerate() {
        if i == 0 {
            print!("{name}={state}");
        } else {
            print!(", {name}={state}");
        }
    }
    println!("}}");
}
