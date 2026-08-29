#
# BitBake Tests for the cgroup helpers used by the runqueue scheduler
#
# SPDX-License-Identifier: GPL-2.0-only
#

import os
import tempfile
import unittest

import bb.runqueue

class CgroupMemoryLimitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.leaf = os.path.join(self.root, "kubepods.slice", "pod.slice", "cri.scope")
        os.makedirs(self.leaf)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, rel, name, value):
        with open(os.path.join(self.root, rel, name), "w") as f:
            f.write(value + "\n")

    def test_nearest_finite_limit_wins(self):
        self.write("kubepods.slice", "memory.max", "1000")
        self.write("kubepods.slice/pod.slice", "memory.max", "500")
        self.write("kubepods.slice/pod.slice/cri.scope", "memory.max", "max")
        current, limit = bb.runqueue.cgroup_memory_limit(self.leaf, self.root)
        self.assertEqual(limit, 500)
        self.assertEqual(current, os.path.join(self.root, "kubepods.slice", "pod.slice", "memory.current"))

    def test_root_limit_is_found(self):
        self.write("", "memory.max", "42")
        current, limit = bb.runqueue.cgroup_memory_limit(self.leaf, self.root)
        self.assertEqual(limit, 42)
        self.assertEqual(current, os.path.join(self.root, "memory.current"))

    def test_unbounded_returns_none(self):
        self.write("kubepods.slice", "memory.max", "max")
        self.assertIsNone(bb.runqueue.cgroup_memory_limit(self.leaf, self.root))

    def test_never_leaves_the_root(self):
        outside = os.path.join(self.root, "..", "memory.max")
        self.assertIsNone(bb.runqueue.cgroup_memory_limit(self.root, os.path.join(self.root, "sub")))

    def test_pressure_files_fall_back_to_proc(self):
        files = bb.runqueue.pressure_files()
        self.assertEqual(len(files), 3)
        for f in files:
            self.assertTrue(f.startswith(bb.runqueue.CGROUP_ROOT) or f.startswith("/proc/pressure/"))
