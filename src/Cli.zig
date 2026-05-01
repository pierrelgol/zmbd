const std = @import("std");
const ascii = std.ascii;
const mem = std.mem;
const process = std.process;

filename: []const u8,

pub const empty: @This() = .{
    .filename = undefined,
};

pub fn parse(it: *process.Args.Iterator) !@This() {
    var result: @This() = .empty;

    while (it.next()) |arg| {
        const trimmed = mem.trim(u8, arg, &ascii.whitespace);

        if (mem.eql(u8, "-p", trimmed)) {
            result.filename = it.next() orelse return error.MissingArgument;
        }
    }

    return result;
}
