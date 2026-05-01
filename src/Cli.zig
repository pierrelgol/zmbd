const std = @import("std");
const ascii = std.ascii;
const mem = std.mem;
const process = std.process;

filename: []const u8,
worker_count: usize,

pub const empty: @This() = .{
    .filename = undefined,
    .worker_count = 1,
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
            result.worker_count = try std.fmt.parseInt(usize, value, 10);
        }
    }

    return result;
}
