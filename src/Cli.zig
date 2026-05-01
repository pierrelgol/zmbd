const std = @import("std");
const ascii = std.ascii;
const mem = std.mem;
const process = std.process;

filename: ?[]const u8,
worker_count: u32,
loader_count: u32,
max_seq_length: u32,
loader_buffer_size: usize,
block_sentence_capacity: usize,
asset_dir: []const u8,

pub const empty: @This() = .{
    .filename = null,
    .worker_count = 16,
    .max_seq_length = 80,
    .loader_count = 16,
    .loader_buffer_size = 1024 * 1024,
    .block_sentence_capacity = 1024,
    .asset_dir = "asset",
};

pub fn parse(it: *process.Args.Iterator) !@This() {
    var result: @This() = .empty;

    while (it.next()) |arg| {
        const trimmed = mem.trim(u8, arg, &ascii.whitespace);
        if (trimmed.len == 0) continue;

        if (mem.eql(u8, "-p", trimmed)) {
            result.filename = it.next() orelse return error.MissingArgument;
            continue;
        }

        if (mem.eql(u8, "-j", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.worker_count = try std.fmt.parseInt(u32, value, 10);
            continue;
        }

        if (mem.eql(u8, "-l", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.max_seq_length = try std.fmt.parseInt(u32, value, 10);
            continue;
        }
        if (mem.eql(u8, "-L", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.loader_count = try std.fmt.parseInt(u32, value, 10);
            continue;
        }

        if (mem.eql(u8, "-b", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.loader_buffer_size = try std.fmt.parseInt(usize, value, 10);
            continue;
        }

        if (mem.eql(u8, "-c", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.block_sentence_capacity = try std.fmt.parseInt(usize, value, 10);
            continue;
        }

        if (mem.eql(u8, "-a", trimmed)) {
            result.asset_dir = it.next() orelse return error.MissingArgument;
            continue;
        }
    }

    try result.validate();
    return result;
}

fn validate(self: @This()) !void {
    if (self.worker_count == 0) return error.InvalidWorkerCount;
    if (self.loader_count == 0) return error.InvalidLoaderCount;
    if (self.max_seq_length == 0) return error.InvalidMaxSequenceLength;
    if (self.loader_buffer_size == 0) return error.InvalidLoaderBufferSize;
    if (self.block_sentence_capacity == 0) return error.InvalidBlockSentenceCapacity;
    if (self.asset_dir.len == 0) return error.InvalidAssetDirectory;
}
