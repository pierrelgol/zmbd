const std = @import("std");
const Io = std.Io;
const mem = std.mem;
pub const ShardingLayer = @This();

file: Io.File,
memory_map: Io.File.MemoryMap,

pub const Range = struct {
    start: usize,
    end: usize,

    pub fn len(self: Range) usize {
        return self.end - self.start;
    }

    pub fn isEmpty(self: Range) bool {
        return self.start == self.end;
    }
};

pub fn open(io: Io, cwd: Io.Dir, sub_path: []const u8) !ShardingLayer {
    const file = try cwd.openFile(io, sub_path, .{ .mode = .read_only });
    errdefer file.close(io);

    const stat = try file.stat(io);
    const len = std.math.cast(usize, stat.size) orelse return error.FileTooBig;
    const memory_map = try file.createMemoryMap(io, .{
        .len = len,
        .protection = .{ .read = true, .write = false },
        .populate = false,
    });

    return .{
        .file = file,
        .memory_map = memory_map,
    };
}

pub fn deinit(self: *ShardingLayer, io: Io) void {
    self.memory_map.destroy(io);
    self.file.close(io);
    self.* = undefined;
}

pub fn bytes(self: *const ShardingLayer) []const u8 {
    return self.memory_map.memory;
}

pub fn slice(self: *const ShardingLayer, range: Range) []const u8 {
    return self.bytes()[range.start..range.end];
}

pub fn rangeAt(self: *const ShardingLayer, index: usize, count: usize, delimiter: u8) Range {
    return rangeAtBytes(self.bytes(), index, count, delimiter);
}

pub fn fillRanges(self: *const ShardingLayer, ranges: []Range, delimiter: u8) []Range {
    const count = ranges.len;
    for (ranges, 0..) |*range, index| {
        range.* = rangeAtBytes(self.bytes(), index, count, delimiter);
    }
    return ranges;
}

fn rangeAtBytes(bytes_: []const u8, index: usize, count: usize, delimiter: u8) Range {
    std.debug.assert(count > 0);
    std.debug.assert(index < count);

    const len = bytes_.len;
    if (len == 0) {
        return .{ .start = 0, .end = 0 };
    }

    const start = if (index == 0) 0 else boundaryAfter(bytes_, proportionalOffset(len, index, count), delimiter);
    const end = if (index + 1 == count) len else boundaryAfter(bytes_, proportionalOffset(len, index + 1, count), delimiter);

    return .{
        .start = @min(start, end),
        .end = end,
    };
}

fn proportionalOffset(len: usize, index: usize, count: usize) usize {
    return (len * index) / count;
}

fn boundaryAfter(bytes_: []const u8, start: usize, delimiter: u8) usize {
    if (start >= bytes_.len) {
        return bytes_.len;
    }

    var index = start;
    while (index < bytes_.len) : (index += 1) {
        if (bytes_[index] == delimiter) {
            return index + 1;
        }
    }

    return bytes_.len;
}

test "rangeAtBytes aligns shard boundaries to delimiter" {
    const bytes_ = "aaa\nbbbb\ncc\n";

    try std.testing.expectEqualDeep(Range{ .start = 0, .end = 4 }, rangeAtBytes(bytes_, 0, 3, '\n'));
    try std.testing.expectEqualDeep(Range{ .start = 4, .end = 9 }, rangeAtBytes(bytes_, 1, 3, '\n'));
    try std.testing.expectEqualDeep(Range{ .start = 9, .end = 12 }, rangeAtBytes(bytes_, 2, 3, '\n'));
}

test "rangeAtBytes returns empty trailing shards when shard count exceeds line count" {
    const bytes_ = "abc\n";

    try std.testing.expectEqualDeep(Range{ .start = 0, .end = 4 }, rangeAtBytes(bytes_, 0, 3, '\n'));
    try std.testing.expectEqualDeep(Range{ .start = 4, .end = 4 }, rangeAtBytes(bytes_, 1, 3, '\n'));
    try std.testing.expectEqualDeep(Range{ .start = 4, .end = 4 }, rangeAtBytes(bytes_, 2, 3, '\n'));
}
