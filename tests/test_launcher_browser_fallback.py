"""Tests for ``launcher`` browser fallback (Windows dedicated Chromium window)."""

from __future__ import annotations

import sys
import unittest
from unittest import mock

import launcher


class TestWindowsDedicatedBrowser(unittest.TestCase):
    def test_try_open_skips_non_windows(self) -> None:
        if sys.platform == "win32":
            self.skipTest("non-Windows assertion")
        self.assertFalse(launcher._try_open_windows_dedicated_browser("http://127.0.0.1:5001"))

    @unittest.skipUnless(sys.platform == "win32", "Windows-only")
    @mock.patch.object(launcher.subprocess, "Popen", autospec=True)
    @mock.patch.object(launcher.os.path, "isfile", return_value=True)
    def test_try_open_prefers_first_candidate(
        self, _isfile: mock.MagicMock, popen: mock.MagicMock
    ) -> None:
        launcher._try_open_windows_dedicated_browser("http://127.0.0.1:9/")
        popen.assert_called_once()
        cmd = popen.call_args[0][0]
        self.assertTrue(cmd[0].lower().endswith("msedge.exe") or "msedge" in cmd[0].lower())
        self.assertIn("--user-data-dir=", cmd[1])
        self.assertEqual(cmd[-1], "http://127.0.0.1:9/")
