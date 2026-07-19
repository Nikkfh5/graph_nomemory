#pragma once

#include <cstdint>
#include <variant>

namespace tbank::tasks {

// Logical partitioning parameters. They are deliberately independent of the
// worker count: changing the scheduler must not change task boundaries or the
// order of floating-point reductions.
struct TaskConfig {
    std::uint64_t edge_slice_size;
    std::uint64_t max_task_edges;
    std::uint32_t max_task_vertices;

    friend bool operator==(const TaskConfig&, const TaskConfig&) = default;
};

// A contiguous destination interval whose complete incoming adjacency is
// owned by one task. Zero-indegree destinations are represented by this task
// kind as part of an interval with a possibly zero edge_count.
struct OrdinaryTask {
    std::uint32_t dst_begin;
    std::uint32_t dst_count;
    std::uint64_t edge_begin;
    std::uint64_t edge_count;

    friend bool operator==(const OrdinaryTask&, const OrdinaryTask&) = default;
};

// One ordered part of a single high-indegree destination. The reducer must
// combine all slices for dst in increasing slice_index order and perform the
// destination's final write exactly once.
struct HubSlice {
    std::uint32_t dst;
    std::uint32_t slice_index;
    std::uint32_t slice_count;
    std::uint64_t edge_begin;
    std::uint64_t edge_count;

    friend bool operator==(const HubSlice&, const HubSlice&) = default;
};

using Task = std::variant<OrdinaryTask, HubSlice>;

}  // namespace tbank::tasks
