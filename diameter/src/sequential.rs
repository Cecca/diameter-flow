/// Compute an approximation to the diameter by means of two runs of Dijkstra algorithm
/// Returns the diameter and a pair of nodes realizing it.
/// Assumes that all vertices are in the range [0,n)
pub fn approx_diameter<I: IntoIterator<Item = ((u32, u32), u32)>>(
    edges: I,
    n: u32,
) -> (u32, (u32, u32)) {
    use std::time::Instant;

    let neighbourhoods = init_neighbourhoods(edges, n);
    let mut reachable = vec![false; n as usize];

    let mut distant_pairs = Vec::new();

    let timer = Instant::now();
    for i in 0..n {
        if !reachable[i as usize] {
            println!("starting sssp from {}", i);
            let pair = sssp(&neighbourhoods, i, &mut reachable);
            let v1 = (pair.1).1;
            distant_pairs.push(pair);
            distant_pairs.push(sssp(&neighbourhoods, v1, &mut reachable));
        }
    }
    println!("diameter computation: elapsed {:?}", timer.elapsed());

    distant_pairs.into_iter().max_by_key(|pair| pair.0).unwrap()
}

/// Build neighbourhoods, as vectors of (weight, id) pairs
fn init_neighbourhoods<I: IntoIterator<Item = ((u32, u32), u32)>>(
    edges: I,
    n: u32,
) -> Vec<Vec<(u32, u32)>> {
    let mut neighbourhoods = vec![Vec::new(); n as usize];
    for ((u, v), w) in edges {
        neighbourhoods[u as usize].push((w, v));
        neighbourhoods[v as usize].push((w, u));
    }
    neighbourhoods
}

fn sssp(adjs: &Vec<Vec<(u32, u32)>>, source: u32, reachable: &mut Vec<bool>) -> (u32, (u32, u32)) {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let n = adjs.len();
    let mut distances: Vec<Option<u32>> = vec![None; n];
    let mut pqueue = BinaryHeap::new();

    pqueue.push(Reverse((0, source)));
    distances[source as usize] = Some(0);
    reachable[source as usize] = true;

    while let Some(Reverse((dist, node))) = pqueue.pop() {
        reachable[node as usize] = true;
        for &(weight, neigh) in adjs[node as usize].iter() {
            let d = dist + weight;
            if distances[neigh as usize].is_none() || d < distances[neigh as usize].unwrap() {
                distances[neigh as usize] = Some(d);
                pqueue.push(Reverse((d, neigh)));
            }
        }
    }

    let mut max_i = 0;
    let mut max_dist = 0;
    for (i, dist) in distances.into_iter().enumerate() {
        if let Some(dist) = dist {
            if dist > max_dist {
                max_dist = dist;
                max_i = i;
            }
        }
    }
    (max_dist, (source, max_i as u32))
}

// Floyd-Warshall algorithm
// pub fn apsp(adjacency: &Vec<Vec<u32>>) -> Vec<Vec<u32>> {
//     let n = adjacency.len();
//     let mut cur = vec![vec![std::u32::MAX; n]; n];
//     let mut prev = vec![vec![std::u32::MAX; n]; n];
//     for i in 0..n {
//         for j in 0..n {
//             cur[i][j] = adjacency[i][j];
//         }
//     }

//     for k in 0..n {
//         std::mem::swap(&mut cur, &mut prev);
//         for i in 0..n {
//             for j in 0..n {
//                 let d = if prev[i][k] == std::u32::MAX || prev[k][j] == std::u32::MAX {
//                     std::u32::MAX
//                 } else {
//                     prev[i][k] + prev[k][j]
//                 };
//                 cur[i][j] = std::cmp::min(prev[i][j], d);
//             }
//         }
//     }

//     cur
// }
