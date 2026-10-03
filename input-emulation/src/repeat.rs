use input_event::scancode::Linux;

#[derive(Debug, PartialEq)]
pub(super) enum RepeatAction {
    Start(u32),
    Stop,
    Unchanged,
}

#[derive(Default)]
pub(super) struct RepeatTarget {
    key: Option<u32>,
}

impl RepeatTarget {
    pub(super) fn update(&mut self, key: u32, state: u8) -> RepeatAction {
        if state == 0 && self.key == Some(key) {
            self.key = None;
            return RepeatAction::Stop;
        }
        if state == 1 && self.key != Some(key) && repeatable(key) {
            self.key = Some(key);
            return RepeatAction::Start(key);
        }
        RepeatAction::Unchanged
    }
}

fn repeatable(key: u32) -> bool {
    Linux::try_from(key).is_ok_and(|key| {
        !matches!(
            key,
            Linux::KeyLeftShift
                | Linux::KeyRightShift
                | Linux::KeyLeftCtrl
                | Linux::KeyRightCtrl
                | Linux::KeyLeftAlt
                | Linux::KeyRightalt
                | Linux::KeyLeftMeta
                | Linux::KeyRightmeta
                | Linux::KeyCapsLock
                | Linux::KeyNumlock
                | Linux::KeyScrollLock
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrelated_releases_and_modifiers_preserve_repeat() {
        let mut repeat = RepeatTarget::default();
        let a = Linux::KeyA as u32;
        let b = Linux::KeyB as u32;
        assert_eq!(repeat.update(a, 1), RepeatAction::Start(a));
        assert_eq!(repeat.update(b, 1), RepeatAction::Start(b));
        assert_eq!(repeat.update(a, 0), RepeatAction::Unchanged);
        for key in [Linux::KeyLeftShift, Linux::KeyRightCtrl, Linux::KeyNumlock] {
            assert_eq!(repeat.update(key as u32, 1), RepeatAction::Unchanged);
            assert_eq!(repeat.update(key as u32, 0), RepeatAction::Unchanged);
        }
        assert_eq!(repeat.update(b, 1), RepeatAction::Unchanged);
        assert_eq!(repeat.update(b, 2), RepeatAction::Unchanged);
        assert_eq!(repeat.update(b, 0), RepeatAction::Stop);
        assert_eq!(repeat.update(b, 0), RepeatAction::Unchanged);
    }
}
