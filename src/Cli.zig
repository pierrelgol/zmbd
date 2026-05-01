const std = @import("std");
const ascii = std.ascii;
const mem = std.mem;
const process = std.process;

filename: ?[]const u8,
worker_count: u32,
loader_count: u32,
max_seq_length: u32,

pub const empty: @This() = .{
    .filename = null,
    .worker_count = 8,
    .max_seq_length = 128,
    .loader_count = 4,
};

pub fn parse(it: *process.Args.Iterator) !@This() {
    var result: @This() = .empty;

    while (it.next()) |arg| {
        const trimmed = mem.trim(u8, arg, &ascii.whitespace);

        if (mem.eql(u8, "-p", trimmed)) {
            result.filename = it.next() orelse return error.MissingArgument;
        }

        if (mem.eql(u8, "-j", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.worker_count = try std.fmt.parseInt(u32, value, 10);
        }

        if (mem.eql(u8, "-l", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.max_seq_length = try std.fmt.parseInt(u32, value, 10);
        }
        if (mem.eql(u8, "-L", trimmed)) {
            const value = mem.trim(u8, it.next() orelse return error.MissingArgument, &ascii.whitespace);
            result.loader_count = try std.fmt.parseInt(u32, value, 10);
        }
    }

    return result;
}
