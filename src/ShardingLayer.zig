const std = @import("std");
const Io = std.Io;
const mem = std.mem;

pub const ShardingLayer = @This();

file: Io.File,
size: usize,
allocator: ?mem.Allocator = null,
items: []Item = &.{},
ready_queue_storage: []*Item = &.{},
ready_queue: ?ReadyQueue = null,

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

pub const Item = struct {
    index: usize,
    range: Range,
};

pub const ReadyQueue = Io.Queue(*Item);

pub fn open(io: Io, cwd: Io.Dir, sub_path: []const u8) !ShardingLayer {
    const file = try cwd.openFile(io, sub_path, .{ .mode = .read_only });
    errdefer file.close(io);

    const stat = try file.stat(io);
    return .{
        .file = file,
        .size = std.math.cast(usize, stat.size) orelse return error.FileTooBig,
    };
}

pub fn deinit(self: *ShardingLayer, io: Io) void {
    self.deinitQueue(io);
    self.file.close(io);
    self.* = undefined;
}

pub fn initQueue(self: *ShardingLayer, allocator: mem.Allocator, io: Io, shard_count: usize, delimiter: u8) !void {
    std.debug.assert(self.ready_queue == null);
    std.debug.assert(shard_count > 0);

    self.allocator = allocator;
    self.items = try allocator.alloc(Item, shard_count);
    std.debug.assert(self.items.len == shard_count);
    errdefer {
        allocator.free(self.items);
        self.items = &.{};
        self.allocator = null;
    }

    self.ready_queue_storage = try allocator.alloc(*Item, shard_count);
    std.debug.assert(self.ready_queue_storage.len == shard_count);
    errdefer {
        allocator.free(self.ready_queue_storage);
        self.ready_queue_storage = &.{};
        allocator.free(self.items);
        self.items = &.{};
        self.allocator = null;
    }

    self.ready_queue = .init(self.ready_queue_storage);

    var scratch: [64 * 1024]u8 = undefined;
    std.debug.assert(scratch.len > 0);
    for (self.items, 0..) |*item, index| {
        item.* = .{
            .index = index,
            .range = try rangeAtFile(self.file, io, self.size, index, shard_count, delimiter, &scratch),
        };
        try self.ready_queue.?.putOneUncancelable(io, item);
    }
}

pub fn deinitQueue(self: *ShardingLayer, io: Io) void {
    if (self.ready_queue) |*queue| {
        queue.close(io);
        self.ready_queue = null;
    }

    const allocator = self.allocator orelse return;
    allocator.free(self.ready_queue_storage);
    allocator.free(self.items);
    self.ready_queue_storage = &.{};
    self.items = &.{};
    self.allocator = null;
}

pub fn getReadyQueue(self: *ShardingLayer) *ReadyQueue {
    return &self.ready_queue.?;
}

fn rangeAtFile(file: Io.File, io: Io, size: usize, index: usize, count: usize, delimiter: u8, scratch: []u8) !Range {
    std.debug.assert(count > 0);
    std.debug.assert(index < count);

    if (size == 0) {
        return .{ .start = 0, .end = 0 };
    }

    const start = if (index == 0) 0 else try boundaryAfterFile(
        file,
        io,
        size,
        proportionalOffset(size, index, count),
        delimiter,
        scratch,
    );
    const end = if (index + 1 == count) size else try boundaryAfterFile(
        file,
        io,
        size,
        proportionalOffset(size, index + 1, count),
        delimiter,
        scratch,
    );

    return .{
        .start = @min(start, end),
        .end = end,
    };
}

fn proportionalOffset(len: usize, index: usize, count: usize) usize {
    return (len * index) / count;
}

fn boundaryAfterFile(file: Io.File, io: Io, size: usize, start: usize, delimiter: u8, scratch: []u8) !usize {
    std.debug.assert(scratch.len > 0);
    if (start >= size) {
        return size;
    }

    var offset = start;
    while (offset < size) {
        const to_read = @min(scratch.len, size - offset);
        const amt = try file.readPositionalAll(io, scratch[0..to_read], offset);
        std.debug.assert(amt <= to_read);
        if (amt == 0) {
            return size;
        }

        if (mem.indexOfScalar(u8, scratch[0..amt], delimiter)) |pos| {
            return offset + pos + 1;
        }

        offset += amt;
    }

    return size;
}

test "rangeAtSlice aligns shard boundaries to delimiter" {
    const bytes_ = "aaa\nbbbb\ncc\n";
    const a = rangeAtSlice(bytes_, 0, 3, '\n');
    const b = rangeAtSlice(bytes_, 1, 3, '\n');
    const c = rangeAtSlice(bytes_, 2, 3, '\n');

    try std.testing.expectEqual(@as(usize, 0), a.start);
    try std.testing.expectEqual(b.start, a.end);
    try std.testing.expectEqual(c.start, b.end);
    try std.testing.expectEqual(bytes_.len, c.end);

    try std.testing.expect(a.end == bytes_.len or bytes_[a.end - 1] == '\n');
    try std.testing.expect(b.end == bytes_.len or bytes_[b.end - 1] == '\n');
    try std.testing.expect(c.end == bytes_.len or bytes_[c.end - 1] == '\n');
}

test "rangeAtSlice returns empty trailing shards when shard count exceeds line count" {
    const bytes_ = "abc\n";

    try std.testing.expectEqualDeep(Range{ .start = 0, .end = 4 }, rangeAtSlice(bytes_, 0, 3, '\n'));
    try std.testing.expectEqualDeep(Range{ .start = 4, .end = 4 }, rangeAtSlice(bytes_, 1, 3, '\n'));
    try std.testing.expectEqualDeep(Range{ .start = 4, .end = 4 }, rangeAtSlice(bytes_, 2, 3, '\n'));
}

fn rangeAtSlice(bytes_: []const u8, index: usize, count: usize, delimiter: u8) Range {
    std.debug.assert(count > 0);
    std.debug.assert(index < count);

    if (bytes_.len == 0) {
        return .{ .start = 0, .end = 0 };
    }

    const start = if (index == 0) 0 else boundaryAfterSlice(bytes_, proportionalOffset(bytes_.len, index, count), delimiter);
    const end = if (index + 1 == count) bytes_.len else boundaryAfterSlice(bytes_, proportionalOffset(bytes_.len, index + 1, count), delimiter);
    return .{ .start = @min(start, end), .end = end };
}

fn boundaryAfterSlice(bytes_: []const u8, start: usize, delimiter: u8) usize {
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
