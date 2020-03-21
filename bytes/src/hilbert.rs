// Implementation adapted from
// https://github.com/frankmcsherry/COST/blob/master/src/hilbert_curve.rs

pub struct BytewiseCached {
    hilbert: BytewiseHilbert,
    prev_hi: u64,
    prev_out: (u32, u32),
    prev_rot: (bool, bool),
}

impl BytewiseCached {
    #[inline(always)]
    pub fn detangle(&mut self, tangle: u64) -> (u32, u32) {
        let (mut x_byte, mut y_byte) =
            unsafe { *self.hilbert.detangle.get_unchecked(tangle as u16 as usize) };

        // validate self.prev_rot, self.prev_out
        if self.prev_hi != (tangle >> 16) {
            self.prev_hi = tangle >> 16;

            // detangle with a bit set to see what happens to it
            let low = 255; //self.hilbert.entangle((0xF, 0)) as u16;
            let (x, y) = self.hilbert.detangle((self.prev_hi << 16) + low as u64);

            let value = (x as u8, y as u8);
            self.prev_rot = match value {
                (0x0F, 0x00) => (false, false), // nothing
                (0x00, 0x0F) => (true, false),  // swapped
                (0xF0, 0xFF) => (false, true),  // flipped
                (0xFF, 0xF0) => (true, true),   // flipped & swapped
                val => panic!(format!("Found : ({:x}, {:x})", val.0, val.1)),
            };
            self.prev_out = (x & 0xFFFFFF00, y & 0xFFFFFF00);
        }

        if self.prev_rot.1 {
            x_byte = 255 - x_byte;
            y_byte = 255 - y_byte;
        }
        if self.prev_rot.0 {
            let temp = x_byte;
            x_byte = y_byte;
            y_byte = temp;
        }

        return (
            self.prev_out.0 + x_byte as u32,
            self.prev_out.1 + y_byte as u32,
        );
    }
    pub fn new() -> BytewiseCached {
        let mut result = BytewiseCached {
            hilbert: BytewiseHilbert::new(),
            prev_hi: 0xFFFFFFFFFFFFFFFF,
            prev_out: (0, 0),
            prev_rot: (false, false),
        };

        result.detangle(0); // ensures that we set the cached stuff correctly
        return result;
    }
}

pub struct BytewiseHilbert {
    entangle: Vec<u16>,      // entangle[x_byte << 16 + y_byte] -> tangle
    detangle: Vec<(u8, u8)>, // detangle[tangle] -> (x_byte, y_byte)
    rotation: Vec<u8>,       // info on rotation, keyed per self.entangle
}

impl BytewiseHilbert {
    pub fn new() -> BytewiseHilbert {
        let mut entangle = Vec::new();
        let mut detangle: Vec<_> = (0..65536).map(|_| (0u8, 0u8)).collect();
        let mut rotation = Vec::new();
        for x in 0u32..256 {
            for y in 0u32..256 {
                let entangled = bit_entangle(((x << 24), (y << 24) + (1 << 23)));
                entangle.push((entangled >> 48) as u16);
                detangle[(entangled >> 48) as usize] = (x as u8, y as u8);
                rotation.push(((entangled >> 44) & 0x0F) as u8);

                //  note to self: math is hard.
                //  rotation decode:    lsbs
                //  0100 -N--> 0100 --> 0100
                //  0100 -S--> 1000 --> 1110
                //  0100 -F--> 1011 --> 1100
                //  0100 -FS-> 0111 --> 0110
            }
        }

        return BytewiseHilbert {
            entangle: entangle,
            detangle: detangle,
            rotation: rotation,
        };
    }

    pub fn entangle(&self, (mut x, mut y): (u32, u32)) -> u64 {
        let init_x = x;
        let init_y = y;
        let mut result = 0u64;
        for i in 0..4 {
            let x_byte = (x >> (24 - (8 * i))) as u8;
            let y_byte = (y >> (24 - (8 * i))) as u8;
            result = (result << 16)
                + self.entangle[(((x_byte as u16) << 8) + y_byte as u16) as usize] as u64;
            let rotation = self.rotation[(((x_byte as u16) << 8) + y_byte as u16) as usize];
            if (rotation & 0x2) > 0 {
                let temp = x;
                x = y;
                y = temp;
            }
            if rotation == 12 || rotation == 6 {
                x = 0xFFFFFFFF - x;
                y = 0xFFFFFFFF - y
            }
        }

        debug_assert!(bit_entangle((init_x, init_y)) == result);
        return result;
    }

    #[inline(always)]
    pub fn detangle(&self, tangle: u64) -> (u32, u32) {
        let init_tangle = tangle;
        let mut result = (0u32, 0u32);
        for log_s in 0u32..4 {
            let shifted = (tangle >> (16 * log_s)) as u16;
            let (x_byte, y_byte) = self.detangle[shifted as usize];
            let rotation = self.rotation[(((x_byte as u16) << 8) + y_byte as u16) as usize];
            if rotation == 12 || rotation == 6 {
                result.0 = (1 << 8 * log_s) - result.0 - 1;
                result.1 = (1 << 8 * log_s) - result.1 - 1;
            }
            if (rotation & 0x2) > 0 {
                let temp = result.0;
                result.0 = result.1;
                result.1 = temp;
            }

            result.0 += (x_byte as u32) << (8 * log_s);
            result.1 += (y_byte as u32) << (8 * log_s);
        }

        debug_assert!(bit_detangle(init_tangle) == result);
        return result;
    }
}

fn bit_entangle(mut pair: (u32, u32)) -> u64 {
    let mut result = 0u64;
    for log_s_rev in 0..32 {
        let log_s = 31 - log_s_rev;
        let rx = (pair.0 >> log_s) & 1u32;
        let ry = (pair.1 >> log_s) & 1u32;
        result += (((3 * rx) ^ ry) as u64) << (2 * log_s);
        pair = bit_rotate(log_s, pair, rx, ry);
    }

    return result;
}

fn bit_detangle(tangle: u64) -> (u32, u32) {
    let mut result = (0u32, 0u32);
    for log_s in 0..32 {
        let shifted = ((tangle >> (2 * log_s)) & 3u64) as u32;

        let rx = (shifted >> 1) & 1u32;
        let ry = (shifted ^ rx) & 1u32;
        result = bit_rotate(log_s, result, rx, ry);
        result = (result.0 + (rx << log_s), result.1 + (ry << log_s));
    }

    return result;
}

fn bit_rotate(logn: usize, pair: (u32, u32), rx: u32, ry: u32) -> (u32, u32) {
    if ry == 0 {
        if rx != 0 {
            ((1 << logn) - pair.1 - 1, (1 << logn) - pair.0 - 1)
        } else {
            (pair.1, pair.0)
        }
    } else {
        pair
    }
}
