#!/usr/bin/env python3
"""Deterministic, bounded-memory generators for accepted scale datasets."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Iterator, Sequence


CSV_HEADER = b"from,to\n"
DATASET_FILE = "edges.csv"
MANIFEST_FILE = "manifest.json"
INCOMPLETE_FILE = "INCOMPLETE"
MANIFEST_SCHEMA = "TBANK_SYNTHETIC_DATASET_V1"
RESULT_SCHEMA = "TBANK_DATASET_GENERATOR_RESULT_V1"
ALGORITHM_NAME = "source-major-circulant-hub-rewrite"
ALGORITHM_VERSION = 1
DEFAULT_CHUNK_BYTES = 1 << 20
MIN_CHUNK_BYTES = 64
MAX_CHUNK_BYTES = 64 << 20
MAX_MANIFEST_BYTES = 1 << 20
REVISION_PATTERN = re.compile(r"[0-9a-f]{40}")


class GeneratorError(RuntimeError):
    """A deterministic generator contract violation."""


@dataclass(frozen=True)
class InHubRewrite:
    target: int
    source_begin: int
    source_count: int

    @property
    def source_end(self) -> int:
        return self.source_begin + self.source_count


@dataclass(frozen=True)
class Profile:
    name: str
    vertex_count: int
    base_degree: int
    edge_slice_size: int
    out_hub_degree: int | None = None
    in_hubs: tuple[InHubRewrite, ...] = ()

    @property
    def edge_count(self) -> int:
        return self.vertex_count * self.base_degree

    @property
    def out_hub(self) -> int | None:
        if self.out_hub_degree is None:
            return None
        return self.vertex_count - 1

    @property
    def donor_count(self) -> int:
        if self.out_hub_degree is None:
            return 0
        return self.out_hub_degree - self.base_degree


PROFILES: dict[str, Profile] = {
    "scale": Profile(
        name="scale",
        vertex_count=1_000_000,
        base_degree=40,
        edge_slice_size=8192,
    ),
    "scenario-b": Profile(
        name="scenario-b",
        vertex_count=10_000_000,
        base_degree=4,
        edge_slice_size=8192,
    ),
    "skew": Profile(
        name="skew",
        vertex_count=1_000_000,
        base_degree=50,
        edge_slice_size=8192,
        out_hub_degree=50_000,
        in_hubs=(
            InHubRewrite(target=0, source_begin=50_000, source_count=49_950),
            InHubRewrite(target=1, source_begin=99_950, source_count=16_335),
            InHubRewrite(target=2, source_begin=116_285, source_count=16_335),
        ),
    ),
    "reduced-skew": Profile(
        name="reduced-skew",
        vertex_count=101,
        base_degree=4,
        edge_slice_size=4,
        out_hub_degree=25,
        in_hubs=(
            InHubRewrite(target=0, source_begin=25, source_count=21),
            InHubRewrite(target=1, source_begin=46, source_count=5),
            InHubRewrite(target=2, source_begin=51, source_count=5),
        ),
    ),
}


@dataclass(frozen=True)
class RevisionState:
    revision: str
    tracked_tree_clean: bool
    implementation_matches_revision: bool
    dirty_allowed: bool


@dataclass(frozen=True)
class PayloadResult:
    sha256: str
    byte_count: int
    edge_count: int


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_git(arguments: Sequence[str]) -> str:
    completed = subprocess.run(
        ["git", "-C", str(_repository_root()), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        diagnostic = completed.stderr.strip() or completed.stdout.strip()
        raise GeneratorError(f"git command failed: {diagnostic}")
    return completed.stdout


def _generator_implementation_matches_revision() -> bool:
    relative_generator = Path(__file__).resolve().relative_to(
        _repository_root()
    ).as_posix()
    tracked = subprocess.run(
        [
            "git",
            "-C",
            str(_repository_root()),
            "ls-files",
            "--error-unmatch",
            "--",
            relative_generator,
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0
    if not tracked:
        return False
    diff = subprocess.run(
        [
            "git",
            "-C",
            str(_repository_root()),
            "diff",
            "--quiet",
            "HEAD",
            "--",
            relative_generator,
        ],
        check=False,
    )
    if diff.returncode not in (0, 1):
        raise GeneratorError("cannot compare generator implementation to HEAD")
    return diff.returncode == 0


def resolve_revision(revision: str, allow_dirty: bool) -> RevisionState:
    if REVISION_PATTERN.fullmatch(revision) is None:
        raise GeneratorError("generator revision must be a lowercase 40-hex commit")
    head = _run_git(["rev-parse", "HEAD"]).strip()
    if revision != head:
        raise GeneratorError(
            f"generator revision {revision} does not match current HEAD {head}"
        )
    tracked_status = _run_git(
        ["status", "--porcelain=v1", "--untracked-files=no"]
    )
    tracked_tree_clean = not tracked_status.strip()
    implementation_matches_revision = (
        _generator_implementation_matches_revision()
    )
    if (
        not tracked_tree_clean or not implementation_matches_revision
    ) and not allow_dirty:
        raise GeneratorError(
            "worktree/generator does not match HEAD; commit it or use "
            "--allow-dirty for local tests"
        )
    return RevisionState(
        revision=revision,
        tracked_tree_clean=tracked_tree_clean,
        implementation_matches_revision=implementation_matches_revision,
        dirty_allowed=allow_dirty,
    )


def _validate_chunk_bytes(chunk_bytes: int) -> None:
    if not MIN_CHUNK_BYTES <= chunk_bytes <= MAX_CHUNK_BYTES:
        raise GeneratorError(
            f"chunk bytes must be in [{MIN_CHUNK_BYTES}, {MAX_CHUNK_BYTES}]"
        )


def _replacement_target(profile: Profile, source: int) -> int | None:
    for rewrite in profile.in_hubs:
        if rewrite.source_begin <= source < rewrite.source_end:
            return rewrite.target
    return None


def _generation_targets(profile: Profile, source: int) -> list[int]:
    """Return one source's targets; used only by bounded validation samples."""
    replacement = _replacement_target(profile, source)
    donor = profile.out_hub_degree is not None and source < profile.donor_count
    targets: list[int] = []
    for offset in range(1, profile.base_degree + 1):
        if donor and offset == profile.base_degree:
            continue
        if offset == 1 and replacement is not None:
            targets.append(replacement)
        else:
            targets.append((source + offset) % profile.vertex_count)
    if profile.out_hub is not None and source == profile.out_hub:
        assert profile.out_hub_degree is not None
        targets.extend(range(profile.base_degree, profile.out_hub_degree))
    return targets


def _histogram(entries: Sequence[tuple[int, int]]) -> list[dict[str, int]]:
    aggregated: dict[int, int] = {}
    for degree, count in entries:
        if count:
            aggregated[degree] = aggregated.get(degree, 0) + count
    return [
        {"degree": degree, "vertex_count": aggregated[degree]}
        for degree in sorted(aggregated)
    ]


def _maximum_vertices(profile: Profile, direction: str) -> dict[str, object]:
    if profile.out_hub_degree is None:
        return {"kind": "all", "count": profile.vertex_count}
    if direction == "out":
        assert profile.out_hub is not None
        return {"kind": "explicit", "vertices": [profile.out_hub]}
    maximum = max(
        profile.base_degree + rewrite.source_count
        for rewrite in profile.in_hubs
    )
    return {
        "kind": "explicit",
        "vertices": [
            rewrite.target
            for rewrite in profile.in_hubs
            if profile.base_degree + rewrite.source_count == maximum
        ],
    }


def degree_and_slice_statistics(profile: Profile) -> dict[str, object]:
    if profile.out_hub_degree is None:
        out_histogram = _histogram(
            [(profile.base_degree, profile.vertex_count)]
        )
        in_histogram = list(out_histogram)
    else:
        out_histogram = _histogram(
            [
                (profile.base_degree - 1, profile.donor_count),
                (
                    profile.base_degree,
                    profile.vertex_count - profile.donor_count - 1,
                ),
                (profile.out_hub_degree, 1),
            ]
        )
        removed_incoming = sum(rewrite.source_count for rewrite in profile.in_hubs)
        in_entries: list[tuple[int, int]] = [
            (profile.base_degree - 1, removed_incoming),
            (
                profile.base_degree,
                profile.vertex_count - removed_incoming - len(profile.in_hubs),
            ),
        ]
        in_entries.extend(
            (profile.base_degree + rewrite.source_count, 1)
            for rewrite in profile.in_hubs
        )
        in_histogram = _histogram(in_entries)

    mean = {
        "numerator": profile.edge_count,
        "denominator": profile.vertex_count,
    }
    slice_counts: dict[int, int] = {}
    for bucket in in_histogram:
        slices = math.ceil(bucket["degree"] / profile.edge_slice_size)
        slice_counts[slices] = slice_counts.get(slices, 0) + bucket["vertex_count"]

    in_hubs = [
        {
            "vertex": rewrite.target,
            "degree": profile.base_degree + rewrite.source_count,
        }
        for rewrite in profile.in_hubs
    ]
    out_hubs: list[dict[str, int]] = []
    if profile.out_hub is not None:
        assert profile.out_hub_degree is not None
        out_hubs.append(
            {"vertex": profile.out_hub, "degree": profile.out_hub_degree}
        )

    return {
        "degree": {
            "out": {
                "minimum": out_histogram[0]["degree"],
                "maximum": out_histogram[-1]["degree"],
                "mean": mean,
                "histogram": out_histogram,
                "maximum_vertices": _maximum_vertices(profile, "out"),
            },
            "in": {
                "minimum": in_histogram[0]["degree"],
                "maximum": in_histogram[-1]["degree"],
                "mean": mean,
                "histogram": in_histogram,
                "maximum_vertices": _maximum_vertices(profile, "in"),
            },
        },
        "hubs": {
            "out": out_hubs,
            "in": in_hubs,
            "directions_are_distinct": not out_hubs
            or all(
                out_hubs[0]["vertex"] != in_hub["vertex"]
                for in_hub in in_hubs
            ),
        },
        "slicing": {
            "edge_slice_size": profile.edge_slice_size,
            "in_degree_slice_histogram": [
                {"slice_count": count, "vertex_count": slice_counts[count]}
                for count in sorted(slice_counts)
            ],
            "sliced_destinations": [
                {
                    "vertex": hub["vertex"],
                    "in_degree": hub["degree"],
                    "slice_count": math.ceil(
                        hub["degree"] / profile.edge_slice_size
                    ),
                }
                for hub in in_hubs
                if hub["degree"] > profile.edge_slice_size
            ],
        },
    }


def construction_description(profile: Profile) -> dict[str, object]:
    out_rewrite: dict[str, object] | None = None
    if profile.out_hub is not None:
        assert profile.out_hub_degree is not None
        out_rewrite = {
            "hub": profile.out_hub,
            "target_degree": profile.out_hub_degree,
            "donor_sources": {"begin": 0, "count": profile.donor_count},
            "omitted_base_offset": profile.base_degree,
            "added_targets": {
                "begin": profile.base_degree,
                "count": profile.donor_count,
            },
        }
    return {
        "base": {
            "kind": "directed-circulant",
            "source_order": "ascending",
            "offset_begin": 1,
            "offset_end_inclusive": profile.base_degree,
        },
        "out_hub_rewrite": out_rewrite,
        "in_hub_rewrites": [
            {
                "target": rewrite.target,
                "source_begin": rewrite.source_begin,
                "source_count": rewrite.source_count,
                "replaced_base_offset": 1,
            }
            for rewrite in profile.in_hubs
        ],
    }


def validate_profile(profile: Profile) -> dict[str, object]:
    if profile.name not in PROFILES or PROFILES[profile.name] != profile:
        raise GeneratorError("profile is not a registered immutable contract")
    if not 1 <= profile.base_degree < profile.vertex_count:
        raise GeneratorError("base degree must be in [1, vertex_count)")
    if profile.vertex_count - 1 > 2_147_483_647:
        raise GeneratorError("profile IDs exceed signed int32")
    if profile.edge_count <= 0:
        raise GeneratorError("profile must contain edges")
    if profile.edge_slice_size <= 0:
        raise GeneratorError("slice size must be positive")

    if profile.out_hub_degree is None:
        if profile.in_hubs:
            raise GeneratorError("in-hub rewrites require an out-hub rewrite")
    else:
        if not profile.base_degree < profile.out_hub_degree < profile.vertex_count:
            raise GeneratorError("out-hub degree must be unique and below V")
        if profile.donor_count != profile.out_hub_degree - profile.base_degree:
            raise GeneratorError("out-hub donor arithmetic is inconsistent")
        assert profile.out_hub is not None
        if profile.donor_count >= profile.out_hub:
            raise GeneratorError("donor range overlaps out hub")

        expected_source_begin = profile.out_hub_degree
        targets: set[int] = set()
        removed_ranges: list[range] = []
        for rewrite in profile.in_hubs:
            if rewrite.target in targets:
                raise GeneratorError("in-hub targets must be distinct")
            targets.add(rewrite.target)
            if rewrite.source_count <= 0:
                raise GeneratorError("in-hub source count must be positive")
            if rewrite.source_begin != expected_source_begin:
                raise GeneratorError("in-hub source blocks must be consecutive")
            if rewrite.source_end > profile.out_hub:
                raise GeneratorError("in-hub source block overlaps out hub")
            if rewrite.target >= profile.base_degree:
                raise GeneratorError("in-hub target must retain its base indegree")
            removed = range(rewrite.source_begin + 1, rewrite.source_end + 1)
            if rewrite.target in removed:
                raise GeneratorError("in-hub rewrite would duplicate a base edge")
            removed_ranges.append(removed)
            expected_source_begin = rewrite.source_end

        for left, right in zip(removed_ranges, removed_ranges[1:]):
            if left.stop != right.start:
                raise GeneratorError("removed destination ranges must be disjoint")
        if any(
            target in removed
            for target in targets
            for removed in removed_ranges
        ):
            raise GeneratorError("in-hub target overlaps a removed destination")

        if profile.edge_count - profile.donor_count + profile.donor_count != (
            profile.vertex_count * profile.base_degree
        ):
            raise GeneratorError("rewrite does not preserve the exact edge count")

    sample_sources = {0, profile.vertex_count - 1}
    if profile.out_hub_degree is not None:
        sample_sources.update(
            {profile.donor_count - 1, profile.donor_count}
        )
    for rewrite in profile.in_hubs:
        sample_sources.update(
            {rewrite.source_begin, rewrite.source_end - 1}
        )
    for source in sample_sources:
        if not 0 <= source < profile.vertex_count:
            raise GeneratorError("validation sample source is outside graph")
        targets = _generation_targets(profile, source)
        expected_count = profile.base_degree
        if profile.out_hub_degree is not None and source < profile.donor_count:
            expected_count -= 1
        if profile.out_hub is not None and source == profile.out_hub:
            assert profile.out_hub_degree is not None
            expected_count += profile.out_hub_degree - profile.base_degree
        if len(targets) != expected_count:
            raise GeneratorError("source sequence has the wrong edge count")
        if len(set(targets)) != len(targets):
            raise GeneratorError("source sequence contains a duplicate edge")
        if any(target < 0 or target >= profile.vertex_count for target in targets):
            raise GeneratorError("source sequence contains an out-of-range target")
        if source in targets:
            raise GeneratorError("contract unexpectedly introduced a self-loop")

    if profile.out_hub is not None:
        assert profile.out_hub_degree is not None
        if _generation_targets(profile, profile.out_hub) != list(
            range(profile.out_hub_degree)
        ):
            raise GeneratorError("out-hub target sequence is not canonical")

    statistics = degree_and_slice_statistics(profile)
    out_histogram = statistics["degree"]["out"]["histogram"]
    in_histogram = statistics["degree"]["in"]["histogram"]
    if sum(bucket["vertex_count"] for bucket in out_histogram) != profile.vertex_count:
        raise GeneratorError("out-degree histogram has the wrong vertex count")
    if sum(bucket["vertex_count"] for bucket in in_histogram) != profile.vertex_count:
        raise GeneratorError("in-degree histogram has the wrong vertex count")
    if sum(
        bucket["degree"] * bucket["vertex_count"] for bucket in out_histogram
    ) != profile.edge_count:
        raise GeneratorError("out-degree histogram has the wrong edge count")
    if sum(
        bucket["degree"] * bucket["vertex_count"] for bucket in in_histogram
    ) != profile.edge_count:
        raise GeneratorError("in-degree histogram has the wrong edge count")
    return statistics


def _write_buffer(stream: object, buffer: bytearray) -> int:
    if not buffer:
        return 0
    written = stream.write(buffer)  # type: ignore[attr-defined]
    if written != len(buffer):
        raise GeneratorError("short write while publishing dataset")
    return written


def _stream_payload(profile: Profile, target: Path, chunk_bytes: int) -> PayloadResult:
    digest = hashlib.sha256()
    byte_count = 0
    edge_count = 0
    buffer = bytearray(CSV_HEADER)

    with target.open("xb") as stream:
        for source in range(profile.vertex_count):
            replacement = _replacement_target(profile, source)
            donor = (
                profile.out_hub_degree is not None
                and source < profile.donor_count
            )
            for offset in range(1, profile.base_degree + 1):
                if donor and offset == profile.base_degree:
                    continue
                destination = (
                    replacement
                    if offset == 1 and replacement is not None
                    else (source + offset) % profile.vertex_count
                )
                buffer.extend(f"{source},{destination}\n".encode("ascii"))
                edge_count += 1
                if len(buffer) >= chunk_bytes:
                    digest.update(buffer)
                    byte_count += _write_buffer(stream, buffer)
                    buffer.clear()

            if profile.out_hub is not None and source == profile.out_hub:
                assert profile.out_hub_degree is not None
                for destination in range(
                    profile.base_degree, profile.out_hub_degree
                ):
                    buffer.extend(f"{source},{destination}\n".encode("ascii"))
                    edge_count += 1
                    if len(buffer) >= chunk_bytes:
                        digest.update(buffer)
                        byte_count += _write_buffer(stream, buffer)
                        buffer.clear()

        if buffer:
            digest.update(buffer)
            byte_count += _write_buffer(stream, buffer)
        stream.flush()
        os.fsync(stream.fileno())

    if edge_count != profile.edge_count:
        raise GeneratorError(
            f"generated {edge_count} edges, expected {profile.edge_count}"
        )
    return PayloadResult(
        sha256=digest.hexdigest(),
        byte_count=byte_count,
        edge_count=edge_count,
    )


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_incomplete_marker(output: Path) -> None:
    marker = output / INCOMPLETE_FILE
    with marker.open("xb") as stream:
        payload = (
            b"Dataset generation is incomplete. Do not consume this directory.\n"
        )
        if stream.write(payload) != len(payload):
            raise GeneratorError("short write while creating INCOMPLETE marker")
        stream.flush()
        os.fsync(stream.fileno())
    _fsync_directory(output)


def _initialize_output_directory(output: Path) -> None:
    try:
        output.mkdir()
    except FileExistsError as error:
        raise GeneratorError(f"output path already exists: {output}") from error

    try:
        _write_incomplete_marker(output)
    except BaseException:
        marker = output / INCOMPLETE_FILE
        if not os.path.lexists(marker):
            try:
                output.rmdir()
            except OSError as cleanup_error:
                try:
                    _write_incomplete_marker(output)
                except BaseException as marker_error:
                    raise GeneratorError(
                        "cannot remove incomplete output or publish its marker"
                    ) from marker_error
                raise cleanup_error
            _fsync_directory(output.parent)
        raise

    # Persist the new directory entry after its durable marker exists.
    _fsync_directory(output.parent)


def _complete_output_directory(output: Path) -> None:
    marker = output / INCOMPLETE_FILE
    try:
        marker.unlink()
        _fsync_directory(output)
    except BaseException:
        if not os.path.lexists(marker):
            try:
                _write_incomplete_marker(output)
            except BaseException as restore_error:
                raise GeneratorError(
                    "completion failed and INCOMPLETE marker could not be restored"
                ) from restore_error
        raise


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def canonical_replay_command(
    profile: Profile,
    output: Path,
    revision: RevisionState,
    chunk_bytes: int,
) -> dict[str, object]:
    _validate_chunk_bytes(chunk_bytes)
    arguments = [
        str(Path(sys.executable).resolve()),
        str(Path(__file__).resolve()),
        "--profile",
        profile.name,
        "--output",
        str(output.resolve(strict=False)),
        "--generator-revision",
        revision.revision,
        "--chunk-bytes",
        str(chunk_bytes),
    ]
    if revision.dirty_allowed:
        arguments.append("--allow-dirty")
    return {
        "cwd": str(_repository_root()),
        "argv": arguments,
    }


def _validate_replay_command(
    command: object,
    profile: Profile,
    revision: str,
    dirty_allowed: bool,
) -> int:
    if not isinstance(command, dict) or set(command) != {"cwd", "argv"}:
        raise GeneratorError("manifest replay command is malformed")
    cwd = command.get("cwd")
    arguments = command.get("argv")
    if cwd != str(_repository_root()):
        raise GeneratorError("manifest replay cwd is not canonical")
    if not isinstance(arguments, list) or any(
        not isinstance(argument, str) for argument in arguments
    ):
        raise GeneratorError("manifest replay argv is malformed")
    expected_length = 11 if dirty_allowed else 10
    if len(arguments) != expected_length:
        raise GeneratorError("manifest replay argv has the wrong length")
    if arguments[0] != str(Path(sys.executable).resolve()):
        raise GeneratorError("manifest replay Python interpreter is not canonical")
    if arguments[1] != str(Path(__file__).resolve()):
        raise GeneratorError("manifest replay generator path is not canonical")
    for index in (0, 1, 5):
        candidate = Path(arguments[index])
        if not candidate.is_absolute() or str(candidate.resolve(strict=False)) != (
            arguments[index]
        ):
            raise GeneratorError("manifest replay path is not canonical absolute")
    expected_prefix = [
        "--profile",
        profile.name,
        "--output",
    ]
    if arguments[2:5] != expected_prefix:
        raise GeneratorError("manifest replay profile/output arguments are malformed")
    if arguments[6:9] != [
        "--generator-revision",
        revision,
        "--chunk-bytes",
    ]:
        raise GeneratorError("manifest replay revision/chunk arguments are malformed")
    try:
        chunk_bytes = int(arguments[9])
    except ValueError as error:
        raise GeneratorError("manifest replay chunk size is malformed") from error
    if str(chunk_bytes) != arguments[9]:
        raise GeneratorError("manifest replay chunk size is not canonical decimal")
    _validate_chunk_bytes(chunk_bytes)
    if dirty_allowed and arguments[10] != "--allow-dirty":
        raise GeneratorError("dirty replay command lacks --allow-dirty")
    return chunk_bytes


def _publish_manifest(output: Path, manifest: dict[str, object]) -> None:
    partial = output / f"{MANIFEST_FILE}.partial"
    final = output / MANIFEST_FILE
    encoded = _canonical_json(manifest)
    with partial.open("xb") as stream:
        if stream.write(encoded) != len(encoded):
            raise GeneratorError("short write while publishing manifest")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, final)
    _fsync_directory(output)


def _manifest(
    profile: Profile,
    revision: RevisionState,
    command: dict[str, object],
    payload: PayloadResult,
) -> dict[str, object]:
    return {
        "schema": MANIFEST_SCHEMA,
        "complete": True,
        "profile": profile.name,
        "algorithm": {
            "name": ALGORITHM_NAME,
            "version": ALGORITHM_VERSION,
        },
        "graph": {
            "directed": True,
            "unique_edges": True,
            "vertex_count": profile.vertex_count,
            "edge_count": profile.edge_count,
        },
        "construction": construction_description(profile),
        "seed": {"policy": "none", "deterministic": True},
        "provenance": {"source": "synthetic", "license": "CC0-1.0"},
        "generator": {
            "revision": revision.revision,
            "tracked_tree_clean": revision.tracked_tree_clean,
            "implementation_matches_revision": (
                revision.implementation_matches_revision
            ),
            "dirty_allowed": revision.dirty_allowed,
        },
        "command": {
            "cwd": command["cwd"],
            "argv": list(command["argv"]),
        },
        "dataset": {
            "path": DATASET_FILE,
            "format": "edge-csv",
            "header": CSV_HEADER.decode("ascii").rstrip("\n"),
            "line_ending": "LF",
            "final_newline": True,
            "sha256": payload.sha256,
            "bytes": payload.byte_count,
        },
        "statistics": degree_and_slice_statistics(profile),
    }


def generate_dataset(
    profile: Profile,
    output: Path,
    initial_revision: RevisionState,
    command: dict[str, object],
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
) -> dict[str, object]:
    validate_profile(profile)
    _validate_chunk_bytes(chunk_bytes)
    replay_chunk_bytes = _validate_replay_command(
        command,
        profile,
        initial_revision.revision,
        initial_revision.dirty_allowed,
    )
    if replay_chunk_bytes != chunk_bytes:
        raise GeneratorError("replay command chunk size differs from generation")

    _initialize_output_directory(output)

    partial_payload = output / f"{DATASET_FILE}.partial"
    final_payload = output / DATASET_FILE
    payload = _stream_payload(profile, partial_payload, chunk_bytes)
    os.replace(partial_payload, final_payload)
    _fsync_directory(output)

    final_revision = resolve_revision(
        initial_revision.revision, initial_revision.dirty_allowed
    )
    if not initial_revision.dirty_allowed and not final_revision.tracked_tree_clean:
        raise GeneratorError("tracked worktree changed during formal generation")
    manifest = _manifest(profile, final_revision, command, payload)
    _publish_manifest(output, manifest)

    _complete_output_directory(output)
    return manifest


def _verification_chunks(
    profile: Profile, chunk_bytes: int
) -> Iterator[bytes]:
    """Independent expected-byte implementation used only by verifier."""
    buffer = bytearray(b"from,to\n")
    for source in range(profile.vertex_count):
        replacement: int | None = None
        for rule in profile.in_hubs:
            if rule.source_begin <= source < rule.source_begin + rule.source_count:
                replacement = rule.target
                break
        omit_last = profile.out_hub_degree is not None and source < (
            profile.out_hub_degree - profile.base_degree
        )
        for offset in range(1, profile.base_degree + 1):
            if omit_last and offset == profile.base_degree:
                continue
            if offset == 1 and replacement is not None:
                destination = replacement
            else:
                destination = (source + offset) % profile.vertex_count
            buffer.extend((str(source) + "," + str(destination) + "\n").encode("ascii"))
            if len(buffer) >= chunk_bytes:
                yield bytes(buffer)
                buffer.clear()
        if (
            profile.out_hub_degree is not None
            and source == profile.vertex_count - 1
        ):
            for destination in range(
                profile.base_degree, profile.out_hub_degree
            ):
                buffer.extend(
                    (str(source) + "," + str(destination) + "\n").encode("ascii")
                )
                if len(buffer) >= chunk_bytes:
                    yield bytes(buffer)
                    buffer.clear()
    if buffer:
        yield bytes(buffer)


def _open_directory_no_follow(path: Path) -> int:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise GeneratorError("safe no-follow verification is unsupported")
    try:
        before = os.lstat(path)
    except OSError as error:
        raise GeneratorError(f"cannot inspect dataset directory: {error}") from error
    if not stat.S_ISDIR(before.st_mode):
        raise GeneratorError("dataset path must be a real directory, not a link/device")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise GeneratorError(
            f"cannot safely open dataset directory: {error}"
        ) from error
    after = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(after.st_mode)
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
    ):
        os.close(descriptor)
        raise GeneratorError("dataset directory changed while it was opened")
    return descriptor


def _open_regular_at(
    directory_descriptor: int,
    name: str,
    *,
    maximum_bytes: int | None = None,
    expected_bytes: int | None = None,
) -> int:
    try:
        before = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise GeneratorError(f"cannot inspect {name}: {error}") from error
    if not stat.S_ISREG(before.st_mode):
        raise GeneratorError(f"{name} must be a regular file, not a link/device/FIFO")
    if maximum_bytes is not None and before.st_size > maximum_bytes:
        raise GeneratorError(f"{name} exceeds the strict size limit")
    if expected_bytes is not None and before.st_size != expected_bytes:
        raise GeneratorError(f"{name} size does not match manifest")

    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    # A FIFO/device swapped in after lstat must never block the verifier.
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory_descriptor)
    except OSError as error:
        raise GeneratorError(f"cannot safely open {name}: {error}") from error
    after = os.fstat(descriptor)
    if (
        not stat.S_ISREG(after.st_mode)
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
    ):
        os.close(descriptor)
        raise GeneratorError(f"{name} changed or is not regular after open")
    if maximum_bytes is not None and after.st_size > maximum_bytes:
        os.close(descriptor)
        raise GeneratorError(f"{name} exceeds the strict size limit")
    if expected_bytes is not None and after.st_size != expected_bytes:
        os.close(descriptor)
        raise GeneratorError(f"{name} size does not match manifest")
    return descriptor


def _load_manifest(directory_descriptor: int) -> dict[str, object]:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise GeneratorError(f"manifest contains duplicate key {key!r}")
            result[key] = value
        return result

    descriptor = _open_regular_at(
        directory_descriptor,
        MANIFEST_FILE,
        maximum_bytes=MAX_MANIFEST_BYTES,
    )
    try:
        stream = os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise
    try:
        with stream:
            encoded = stream.read(MAX_MANIFEST_BYTES + 1)
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise GeneratorError("manifest.json exceeds the strict size limit")
        value = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise GeneratorError(f"cannot read canonical manifest: {error}") from error
    if not isinstance(value, dict):
        raise GeneratorError("manifest root must be an object")
    if _canonical_json(value) != encoded:
        raise GeneratorError("manifest is not canonical JSON")
    return value


def _validate_manifest_contract(
    manifest: dict[str, object], profile: Profile, revision: RevisionState
) -> tuple[str, int]:
    expected_keys = {
        "schema",
        "complete",
        "profile",
        "algorithm",
        "graph",
        "construction",
        "seed",
        "provenance",
        "generator",
        "command",
        "dataset",
        "statistics",
    }
    if set(manifest) != expected_keys:
        raise GeneratorError("manifest top-level fields do not match schema")
    expected_static = {
        "schema": MANIFEST_SCHEMA,
        "complete": True,
        "profile": profile.name,
        "algorithm": {"name": ALGORITHM_NAME, "version": ALGORITHM_VERSION},
        "graph": {
            "directed": True,
            "unique_edges": True,
            "vertex_count": profile.vertex_count,
            "edge_count": profile.edge_count,
        },
        "construction": construction_description(profile),
        "seed": {"policy": "none", "deterministic": True},
        "provenance": {"source": "synthetic", "license": "CC0-1.0"},
        "statistics": degree_and_slice_statistics(profile),
    }
    for key, expected in expected_static.items():
        if manifest.get(key) != expected:
            raise GeneratorError(f"manifest field {key!r} violates profile contract")

    generator = manifest.get("generator")
    if not isinstance(generator, dict) or set(generator) != {
        "revision",
        "tracked_tree_clean",
        "implementation_matches_revision",
        "dirty_allowed",
    }:
        raise GeneratorError("manifest generator provenance is malformed")
    if generator.get("revision") != revision.revision:
        raise GeneratorError("manifest generator revision does not match current HEAD")
    clean = generator.get("tracked_tree_clean")
    implementation_matches_revision = generator.get(
        "implementation_matches_revision"
    )
    dirty_allowed = generator.get("dirty_allowed")
    if (
        not isinstance(clean, bool)
        or not isinstance(implementation_matches_revision, bool)
        or not isinstance(dirty_allowed, bool)
    ):
        raise GeneratorError("manifest generator cleanliness fields must be booleans")
    if (not clean or not implementation_matches_revision) and not dirty_allowed:
        raise GeneratorError("dirty manifest must be explicitly marked dirty_allowed")
    if dirty_allowed and not revision.dirty_allowed:
        raise GeneratorError(
            "dirty/local-test manifest requires --allow-dirty to verify"
        )

    _validate_replay_command(
        manifest.get("command"),
        profile,
        revision.revision,
        dirty_allowed,
    )

    dataset = manifest.get("dataset")
    expected_dataset_keys = {
        "path",
        "format",
        "header",
        "line_ending",
        "final_newline",
        "sha256",
        "bytes",
    }
    if not isinstance(dataset, dict) or set(dataset) != expected_dataset_keys:
        raise GeneratorError("manifest dataset descriptor is malformed")
    expected_dataset_static = {
        "path": DATASET_FILE,
        "format": "edge-csv",
        "header": "from,to",
        "line_ending": "LF",
        "final_newline": True,
    }
    for key, expected in expected_dataset_static.items():
        if dataset.get(key) != expected:
            raise GeneratorError(f"manifest dataset field {key!r} is invalid")
    digest = dataset.get("sha256")
    byte_count = dataset.get("bytes")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise GeneratorError("manifest dataset SHA-256 is malformed")
    if (
        isinstance(byte_count, bool)
        or not isinstance(byte_count, int)
        or byte_count <= 0
    ):
        raise GeneratorError("manifest dataset byte count is malformed")
    return digest, byte_count


def verify_dataset(
    output: Path,
    revision: RevisionState,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
) -> dict[str, object]:
    _validate_chunk_bytes(chunk_bytes)
    directory_descriptor = _open_directory_no_follow(output)
    try:
        entries = set(os.listdir(directory_descriptor))
        if INCOMPLETE_FILE in entries:
            raise GeneratorError("dataset directory still has INCOMPLETE marker")
        if entries != {DATASET_FILE, MANIFEST_FILE}:
            raise GeneratorError("complete dataset directory has unexpected entries")

        manifest = _load_manifest(directory_descriptor)
        profile_name = manifest.get("profile")
        if not isinstance(profile_name, str) or profile_name not in PROFILES:
            raise GeneratorError("manifest profile is unknown")
        profile = PROFILES[profile_name]
        validate_profile(profile)
        expected_digest, expected_bytes = _validate_manifest_contract(
            manifest, profile, revision
        )

        digest = hashlib.sha256()
        byte_count = 0
        payload_descriptor = _open_regular_at(
            directory_descriptor,
            DATASET_FILE,
            expected_bytes=expected_bytes,
        )
        try:
            stream = os.fdopen(payload_descriptor, "rb", buffering=chunk_bytes)
        except BaseException:
            os.close(payload_descriptor)
            raise
        with stream:
            for expected_chunk in _verification_chunks(profile, chunk_bytes):
                actual_chunk = stream.read(len(expected_chunk))
                if actual_chunk != expected_chunk:
                    raise GeneratorError(
                        "dataset byte sequence differs at or after offset "
                        f"{byte_count}"
                    )
                digest.update(actual_chunk)
                byte_count += len(actual_chunk)
            if stream.read(1):
                raise GeneratorError(
                    "dataset has trailing bytes after expected sequence"
                )
    finally:
        os.close(directory_descriptor)

    observed_digest = digest.hexdigest()
    if observed_digest != expected_digest:
        raise GeneratorError("dataset SHA-256 does not match manifest")
    if byte_count != expected_bytes:
        raise GeneratorError("dataset byte count does not match manifest")
    return {
        "profile": profile.name,
        "sha256": observed_digest,
        "bytes": byte_count,
        "edge_count": profile.edge_count,
    }


def _positive_chunk(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if not MIN_CHUNK_BYTES <= parsed <= MAX_CHUNK_BYTES:
        raise argparse.ArgumentTypeError(
            f"must be in [{MIN_CHUNK_BYTES}, {MAX_CHUNK_BYTES}]"
        )
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate or independently verify deterministic graph datasets."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", type=Path, help="new exclusive output directory")
    mode.add_argument(
        "--validate-only",
        action="store_true",
        help="validate formulas and boundary sequences without filesystem writes",
    )
    mode.add_argument("--verify", type=Path, metavar="DIRECTORY")
    parser.add_argument("--profile", choices=sorted(PROFILES))
    parser.add_argument("--generator-revision", required=True)
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="local tests only; records dirty provenance in generated manifest",
    )
    parser.add_argument(
        "--chunk-bytes",
        type=_positive_chunk,
        default=DEFAULT_CHUNK_BYTES,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    parsed = _parser().parse_args(arguments)
    try:
        revision = resolve_revision(
            parsed.generator_revision, parsed.allow_dirty
        )
        if parsed.verify is not None:
            if parsed.profile is not None:
                raise GeneratorError("--profile is forbidden with --verify")
            verified = verify_dataset(parsed.verify, revision, parsed.chunk_bytes)
            result = {
                "schema": RESULT_SCHEMA,
                "operation": "verify",
                "valid": True,
                **verified,
            }
        else:
            if parsed.profile is None:
                raise GeneratorError("--profile is required for generation/validation")
            profile = PROFILES[parsed.profile]
            statistics = validate_profile(profile)
            if parsed.validate_only:
                result = {
                    "schema": RESULT_SCHEMA,
                    "operation": "validate-only",
                    "valid": True,
                    "profile": profile.name,
                    "vertex_count": profile.vertex_count,
                    "edge_count": profile.edge_count,
                    "statistics": statistics,
                }
            else:
                assert parsed.output is not None
                command = canonical_replay_command(
                    profile,
                    parsed.output,
                    revision,
                    parsed.chunk_bytes,
                )
                manifest = generate_dataset(
                    profile,
                    parsed.output,
                    revision,
                    command,
                    parsed.chunk_bytes,
                )
                dataset = manifest["dataset"]
                result = {
                    "schema": RESULT_SCHEMA,
                    "operation": "generate",
                    "valid": True,
                    "profile": profile.name,
                    "output": str(parsed.output),
                    "sha256": dataset["sha256"],
                    "bytes": dataset["bytes"],
                    "edge_count": profile.edge_count,
                }
        sys.stdout.buffer.write(_canonical_json(result))
        return 0
    except (GeneratorError, OSError, ValueError) as error:
        failure = {
            "schema": RESULT_SCHEMA,
            "valid": False,
            "error": str(error),
        }
        sys.stderr.buffer.write(_canonical_json(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
