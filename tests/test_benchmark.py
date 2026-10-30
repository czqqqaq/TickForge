import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/benchmark_core.py"
spec = importlib.util.spec_from_file_location("benchmark_core", SCRIPT)
benchmark_core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark_core)


class BenchmarkTests(unittest.TestCase):
    def test_evidence_copies_and_uses_the_fixed_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deps = root / "target/release/deps"
            deps.mkdir(parents=True)
            for name in ("anyhow", "serde", "serde_json", "crc32fast", "sha2", "tracing"):
                (deps / f"lib{name}-test.rlib").touch()
            (root / "scripts").mkdir()
            (root / "scripts/core_benchmark.rs").write_text("ENGINE WAL")
            source = root / "source"
            source.mkdir()
            for name in ("engine.rs", "wal.rs"):
                (source / name).write_text(name)
            original = root / "original.wal"
            original.write_bytes(b"immutable input")
            output = root / "evidence"
            args = argparse.Namespace(before=source, after=source, input=original, out=output)
            inputs = []

            def run(command, **kwargs):
                if command[0] == "rustc":
                    return subprocess.CompletedProcess(command, 0)
                inputs.append(Path(command[1]))
                result = {"digest": "same", "engine_us": [10], "wal_read_us": [20], "wal_write_us": [30]}
                return subprocess.CompletedProcess(command, 0, stdout=json.dumps(result))

            with patch.object(benchmark_core.subprocess, "run", side_effect=run):
                report = benchmark_core.benchmark(args, root)
            self.assertEqual(inputs, [(output / "input.wal").resolve()] * 2)
            self.assertEqual(report["input"], "input.wal")
            self.assertEqual((output / report["input"]).read_bytes(), original.read_bytes())
            self.assertEqual(report["wal_sha256"], hashlib.sha256(original.read_bytes()).hexdigest())
            original.unlink()
            self.assertEqual((output / report["input"]).read_bytes(), b"immutable input")

    def test_digest_mismatch_is_rejected_with_and_without_optimization(self):
        code = (
            "import importlib.util; "
            f"spec=importlib.util.spec_from_file_location('benchmark_core',{str(SCRIPT)!r}); "
            "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
            "module.check_digest({'before':{'digest':'old'},'after':{'digest':'changed'}})"
        )
        for flags in ([], ["-O"]):
            with self.subTest(flags=flags):
                result = subprocess.run([sys.executable, *flags, "-c", code], capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Business digest changed", result.stderr)


if __name__ == "__main__":
    unittest.main()
