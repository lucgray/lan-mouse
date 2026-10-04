#[cfg(any(windows, x11, test))]
use std::collections::HashMap;

#[cfg(any(windows, x11, test))]
use crate::EmulationHandle;

/// Fractional relative motion belongs to one native handle lifecycle.
#[cfg(any(windows, x11, test))]
#[derive(Default)]
pub(crate) struct MotionRemainders(HashMap<EmulationHandle, (f64, f64)>);

#[cfg(any(windows, x11, test))]
impl MotionRemainders {
    pub(crate) fn deliver<E>(
        &mut self,
        handle: EmulationHandle,
        delta: (f64, f64),
        emit: impl FnOnce(i32, i32) -> Result<(), E>,
    ) -> Result<(), E> {
        let residual = self.0.get(&handle).copied().unwrap_or_default();
        let ((x, y), next) = quantize_motion(delta, residual);
        if x != 0 || y != 0 {
            emit(x, y)?;
        }
        // Commit only after successful delivery; zero movement needs no native call.
        self.0.insert(handle, next);
        Ok(())
    }

    pub(crate) fn remove(&mut self, handle: EmulationHandle) {
        self.0.remove(&handle);
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

pub(crate) fn quantize_motion(
    (dx, dy): (f64, f64),
    (rx, ry): (f64, f64),
) -> ((i32, i32), (f64, f64)) {
    let (x, rx) = quantize_axis(dx, rx);
    let (y, ry) = quantize_axis(dy, ry);
    ((x, y), (rx, ry))
}

fn quantize_axis(delta: f64, residual: f64) -> (i32, f64) {
    // Residuals contain fractions only, never unrepresentable displacement.
    let residual = if residual.is_finite() && residual.abs() <= 0.5 {
        residual
    } else {
        0.0
    };
    if !delta.is_finite() {
        return (0, residual);
    }
    let sum = delta + residual;
    let rounded = sum.round();
    let integer = rounded as i32;
    if rounded < f64::from(i32::MIN) || rounded > f64::from(i32::MAX) {
        return (integer, 0.0);
    }
    (integer, sum - f64::from(integer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_motion_preserves_distance_and_reversals_without_zero_injection() {
        let mut motion = MotionRemainders::default();
        let mut total = (0, 0);
        let mut calls = 0;
        for sign in [1.0, -1.0] {
            for _ in 0..1000 {
                motion
                    .deliver(7, (sign * 0.4, sign * -0.4), |x, y| {
                        assert_ne!((x, y), (0, 0));
                        total.0 += x;
                        total.1 += y;
                        calls += 1;
                        Ok::<_, ()>(())
                    })
                    .unwrap();
            }
            assert_eq!(total, if sign > 0.0 { (400, -400) } else { (0, 0) });
        }
        assert_eq!(calls, 800);
        assert!(motion.0[&7].0.abs() <= 0.5);
    }

    #[test]
    fn peers_axes_and_recreated_handles_do_not_share_fractions() {
        let mut motion = MotionRemainders::default();
        motion.deliver(1, (0.4, 0.0), |_, _| Err(())).unwrap();
        motion.deliver(2, (0.4, 0.0), |_, _| Err(())).unwrap();
        let mut output = Vec::new();
        motion
            .deliver(1, (0.4, 0.4), |x, y| {
                output.push((x, y));
                Ok::<_, ()>(())
            })
            .unwrap();
        assert_eq!(output, [(1, 0)]);
        assert_eq!(motion.0[&2], (0.4, 0.0));
        motion.remove(2);
        motion.deliver(2, (0.4, 0.0), |_, _| Err(())).unwrap();
        assert_eq!(motion.0[&2], (0.4, 0.0));
        motion.clear();
        assert!(motion.0.is_empty());
    }

    #[test]
    fn failed_delivery_does_not_commit_candidate_remainder() {
        let mut motion = MotionRemainders::default();
        motion.deliver(1, (0.4, -0.4), |_, _| Err(())).unwrap();
        assert_eq!(
            motion.deliver(1, (0.4, -0.4), |x, y| {
                assert_eq!((x, y), (1, -1));
                Err("native refused")
            }),
            Err("native refused")
        );
        assert_eq!(motion.0[&1], (0.4, -0.4));
        motion
            .deliver(1, (0.4, -0.4), |x, y| {
                assert_eq!((x, y), (1, -1));
                Ok::<_, ()>(())
            })
            .unwrap();
        assert!((motion.0[&1].0 + 0.2).abs() < 1e-12);
    }

    #[test]
    fn huge_or_invalid_motion_keeps_bounded_fractions_and_recovers() {
        let mut motion = MotionRemainders::default();
        for delta in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            -f64::MAX,
        ] {
            motion
                .deliver(1, (delta, delta), |_, _| Ok::<_, ()>(()))
                .unwrap();
            let (x, y) = motion.0[&1];
            assert!(x.is_finite() && x.abs() <= 0.5);
            assert!(y.is_finite() && y.abs() <= 0.5);
            motion
                .deliver(1, (1.0, -1.0), |x, y| {
                    assert_eq!((x, y), (1, -1));
                    Ok::<_, ()>(())
                })
                .unwrap();
        }
    }
}
