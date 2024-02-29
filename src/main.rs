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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avg = self.sum / (self.count as f32);
        write!(f, "{:.1}/{avg:.1}/{:.1}", self.min, self.max)
    }
}

impl State {
    fn update(&mut self, v: f32) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.count += 1;
        self.sum += v;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.count += other.count;
        self.sum += other.sum;
    }
}

///Makes str from u8 through unsafe cast
fn make_str(x: &[u8]) -> &str {
    unsafe {
        let s = std::str::from_utf8_unchecked(x);
        s
    }
}
///Interns an str in a vector for later use. Cheap-o-bump-alloc, essentially.
fn intern_str(stringstore: &mut Vec<u8>, x: &str) -> &'static str {
    let start = stringstore.len();
    stringstore.extend_from_slice(x.as_bytes());
    let len = x.len();
    unsafe {
        let x = std::str::from_utf8_unchecked(&stringstore[start..start + len]);
        (x as *const str).as_ref().unwrap_unchecked()
    }
}

fn unintern_str(stringstore: &mut Vec<u8>, x: &str) {
    unsafe {
        stringstore.set_len(stringstore.len() - x.len());
    }
}
fn parse_stuff(stringstore: &mut Vec<u8>, line:&[u8])->Result<(&'static str, f32), &'static str>
{
    let mut parts = line.split(|&e| e == b';');

        let name = make_str(parts.next().ok_or("there must be a city name")?);
        let name = intern_str(stringstore, name);

        let value = make_str(parts.next().ok_or("there must be a value")?);
        let value: f32 = value.parse().map_err(|e| "Value must be a float")?;
    Ok((name,value))
}

fn make_map<'a>(
    stringstore: &mut Vec<u8>,
    i: impl Iterator<Item = &'a [u8]>,
) -> HashMap<&'static str, State> {
    let mut state: HashMap<&'static str, State> = Default::default();
    for line in i {
        if line.len()==0{
            break;
        }
        let (name,value) = match parse_stuff(stringstore, line){
            Ok(v)=>v,
            Err(e)=>{
                println!("Got error {e}");
                println!("while parsing line {:?}",line);
                panic!("Cant work");
            }
        }; 
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
    dbg!(mem.len());
    let iter = mem.split(|&e| e == b'\n');
    make_map(stringstore, iter)
}

fn merge(a: &mut HashMap<&'static str, State>, b: &HashMap<&'static str, State>) {
    for (k, v) in b {
        a.entry(k).or_default().merge(v);
    }
}

fn main() {
    let cores: usize = std::thread::available_parallelism().unwrap().into();
    println!("Working with {cores} cores");
    // malloc is for the weak, we will allocate a string storage bump allocator per thread and just never free it.
    // This allows us to pretend that everything is 'static str.
    let mut stringstores: Vec<_> = (0..cores)
        .map(|_| Vec::with_capacity(1024 * 1024 * 16))
        .collect();

    let path = match std::env::args().skip(1).next() {
        Some(path) => path,
        None => "measurements.txt".to_owned(),
    };
    let file = File::open(path).unwrap();
    let metadata = file.metadata().unwrap();
    let total_len = metadata.len() as usize;

    let chunk_size = total_len / cores;

    let mmap = unsafe { memmap2::MmapOptions::new().populate().map(&file).unwrap() };

    /*
    let block = 1024 * 1024 * 16;
    let mut buf: Vec<u8> =  std::iter::once(0u8).cycle().take(block).collect();

    loop {
        let n = file.read(&mut buf).unwrap();
        for &e in &buf{
            s += e as u64;
        }
        //let sln = solve_for_part((0, n), &buf);
        //dbg!(sln);
        if n ==0{
            break;
        }
    }*/
    //let mut chunks: Vec<(usize, usize)> = vec![];
    let mut chunks: Vec<_> = vec![];
    let search_area = 256;
    let mut start = 0;
    for _ in 0..cores - 1 {
        let end = start + chunk_size;
        let slice = &mmap[end..end + search_area];
        let mut iter = slice.into_iter().enumerate();
        let next_new_line = loop {
            let (idx, &c) = iter.next().unwrap();
            if c == b'\n' {
                break idx;
            }
        };
        // let next_new_line = match memchr::memchr(b'\n', &mmap[end..]) {
        //     Some(v) => v,
        //     None => {
        //         assert_eq!(end, mmap.len());
        //         0
        //     }
        // };
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

        let mut state = HashMap::<&'static str, State>::new();
        for jh in join_handles {
            let res = jh.join().unwrap();
            merge(&mut state, &res);
        }
        state
    });

    /*let parts: Vec<_> = chunks
        .par_iter()
        .map(|r| solve_for_part(*r, &mmap))
        .collect();
    */
    /*let state: HashMap<&str, State> = parts.into_iter().fold(Default::default(), |mut a, b| {
        merge(&mut a, &b);
        a
    });*/
    //dbg!(&state.keys());
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
