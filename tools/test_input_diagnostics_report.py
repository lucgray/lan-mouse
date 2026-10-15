import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("report", pathlib.Path(__file__).with_name("input-diagnostics-report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class ReportTests(unittest.TestCase):
    def test_rotation_header_keeps_pair_and_uses_monotonic_clock(self):
        header = "schema=2 utc_us=1 process=2 executable_sha256=abc"
        result = report.summarize([
            header,
            "utc_us=2000000 elapsed_us=10 stage=Applied peer=aa key token=bb state=1 modifier=false",
            header,
            "utc_us=1000000 elapsed_us=100010 stage=Applied peer=aa key token=bb state=0 modifier=false",
        ], since_us=1000000)
        self.assertEqual(result["max_non_modifier_held_ms"]["Applied:key"], 100)
        self.assertEqual(len(result["builds"]), 1)

    def test_injected_keys_do_not_change_physical_hold_report(self):
        result = report.summarize([
            "utc_us=1 stage=Native peer=aa native_key token=bb state=1 message_ms=100 observed_ms=100 wait_ms=0 injected=true modifier=false",
            "utc_us=900001 stage=Native peer=aa native_key token=bb state=0 message_ms=200 observed_ms=1000 wait_ms=800 injected=true modifier=false",
        ])
        self.assertIsNone(result["native_non_modifier_hold_max_ms"])
        self.assertIsNone(result["native_key_wait_max_ms"])

    def test_capture_release_and_suppression_are_not_stuck_keys(self):
        result = report.summarize([
            "utc_us=1 elapsed_us=1 stage=Native peer=aa key token=bb state=1 modifier=false",
            "utc_us=2 elapsed_us=2 stage=Suppressed peer=aa key token=bb state=1 modifier=false",
            "utc_us=3 stage=Capture peer=cc control kind=Release value=-1",
        ])
        self.assertNotIn("open_at_end", result["stages"]["Native:key"])
        self.assertNotIn("open_at_end", result["stages"]["Suppressed:key"])

    def test_message_time_distinguishes_hold_from_dispatch_delay(self):
        result = report.summarize([
            "utc_us=1 elapsed_us=1 stage=Native peer=aa native_key token=bb state=1 message_ms=100 observed_ms=100 wait_ms=0 injected=false modifier=false",
            "utc_us=900001 stage=Native peer=aa native_key token=bb state=0 message_ms=200 observed_ms=1000 wait_ms=800 injected=false modifier=false",
        ])
        self.assertEqual(result["native_non_modifier_hold_max_ms"], 100)
        self.assertEqual(result["native_key_wait_max_ms"], 800)

    def test_modifier_hold_is_not_reported_as_typing_repeat(self):
        result = report.summarize([
            "utc_us=1 elapsed_us=1 stage=Applied peer=aa key token=bb state=1 modifier=true",
            "utc_us=9000001 elapsed_us=9000001 stage=Applied peer=aa key token=bb state=0 modifier=true",
        ])
        self.assertEqual(result["max_non_modifier_held_ms"], {})


if __name__ == "__main__":
    unittest.main()
