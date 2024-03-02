use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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

fn next_newline(input: &[u8]) -> Option<usize> {
    input.iter().position(|&b| b == b'\n')
}

const STR_INTERNED:usize=63;

struct MyString {
    buf: [u8; STR_INTERNED],
    len: u8,
}

impl MyString {
    fn new(src: &[u8]) -> Self {
        debug_assert!(src.len() < 256);
        if src.len() > STR_INTERNED{
            todo!("Heap allocations not done");
        }
        let mut b = [0; STR_INTERNED];
        b[0..src.len()].copy_from_slice(src);
        Self {
            buf: b,
            len: src.len() as u8,
        }
    }
}

impl std::ops::Deref for MyString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        
        unsafe {
            let s = std::str::from_utf8_unchecked(&self.buf[0..self.len as usize]);
            s
        }
    }
}

impl std::hash::Hash for MyString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&self.buf[0..self.len as usize]);
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

type MapType = HashMap<&'static str, State>;


//#[inline(always)]
fn parse_stuff_fast(
    stringstore: &mut Vec<u8>,
    line: &str,
) -> (&'static str, f32){
    unsafe{    
    let (name,value) = line.split_once(';').unwrap();
    let name = intern_str(stringstore, name);
    let (vint, vfrac) = value.split_once( '.').unwrap_unchecked();
    let vint:i32 = vint.parse().unwrap_unchecked();
    let vfrac:u32 = vfrac.parse().unwrap_unchecked();
    let value:f32 = vint as f32 + (vfrac as f32 ) / 10.0;
    (name, value)
    }
}


       /*let (name, value) = match parse_stuff(stringstore, line) {
            Ok(v) => v,
            Err(e) => {
                println!("Got error {e}");
                println!("while parsing line {:?}", line);
                panic!("Cant work");
            }
        };*/
 

fn update_map<'a, 'b>(stringstore: &mut Vec<u8>,state: &'a mut MapType, mem: &'b str)
where 'b: 'a {
    for line in mem.lines() {
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
        //state.entry(name.into()).or_default().update(value);
    }
}

fn solve_for_part<'a, 'b>(stringstore: &mut Vec<u8>, mem: &'a [u8],  next_chunk: &'b AtomicUsize) -> MapType {
    let mut state: MapType = MapType::with_capacity(1024);
 
    loop {
        // bump the start to the start of the next line
        let start = next_chunk.fetch_add(CHUNK_SIZE, Ordering::Relaxed);
 
        if start > mem.len() {
            break;
        }
        let actual_start = match start {
            0 => start,
            _ => {
                let Some(actual_start) = next_newline(&mem[start..]) else {break};
                start + actual_start + 1
            }
        };
        if start > mem.len() {
            break;
        }
 
        // bump the end until the end of the last line
        let end = mem.len().min(start + CHUNK_SIZE);
        let next_new_line = match next_newline(&mem[end..]){
            Some(v) => v,
            None => {
                assert_eq!(end, mem.len());
                0
            }
        };
        let actual_end = end + next_new_line;
 
        update_map(stringstore, &mut state, unsafe { std::str::from_utf8_unchecked(&mem[actual_start..actual_end])});
    };
 
    state
}


fn merge(a: &mut MapType, b: &MapType) {
    for (k, v) in b {
        a.entry(k).or_default().merge(v);
    }
}

const CHUNK_SIZE:usize = 1024*1024*32; //Operate on chunks of several MB at a time

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
    println!("Total file size is {} MB", total_len/1024);
    let next_chunk = AtomicUsize::new(0);

    
    //let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file).unwrap() };
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
    

    let state = std::thread::scope(|s| {
        let join_handles: Vec<_> = stringstores.iter_mut()
            .map(|ss| s.spawn(|| solve_for_part(ss,&mmap, &next_chunk)))
            .collect();

        let mut state = MapType::with_capacity(1024*8);

        for jh in join_handles {
            let res = jh.join().unwrap();
            dbg!("Merging data...", res.len());
            merge(&mut state, &res);
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
