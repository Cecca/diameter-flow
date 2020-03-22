// interleave the bits
pub fn pair_to_zorder((mut x, mut y): (u32, u32)) -> u64 {
    let mut z = 0;
    let msb_mask = 1_u32 << 31;
    for _ in 0..32 {
        if x & msb_mask == 0 {
            z = z << 1;
        } else {
            z = (z << 1) | 1;
        }
        if y & msb_mask == 0 {
            z = z << 1;
        } else {
            z = (z << 1) | 1;
        }
        x = x << 1;
        y = y << 1;
    }
    z
}

pub fn zorder_to_pair(mut z: u64) -> (u32, u32) {
    let mut x = 0;
    let mut y = 0;

    let x_mask = 1_u64 << 63;
    let y_mask = 1_u64 << 62;

    for i in 0..32 {
        if z & x_mask == 0 {
            x = x << 1;
        } else {
            x = (x << 1) | 1;
        }
        if z & y_mask == 0 {
            y = y << 1;
        } else {
            y = (y << 1) | 1;
        }
        z = z << 2;
    }

    (x, y)
}

#[test]
fn test_zorder() {
    for x in 0..100 {
        for y in 0..100 {
            assert_eq!((x, y), zorder_to_pair(pair_to_zorder((x, y))));
        }
    }
}
