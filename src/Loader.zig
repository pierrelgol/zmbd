const std = @import("std");
const Io = std.Io;
const mem = std.mem;

const Cli = @import("Cli.zig");

cli: Cli,
file: ?Io.File,
buffer: []u8,
reader: ?Io.File.Reader,

pub fn init(cli: Cli, buffer: []u8) @This() {
    return .{
        .cli = cli,
        .buffer = buffer,
        .reader = null,
        .file = null,
    };
}

pub fn deinit(self: @This(), io: std.Io) void {
    if (self.file) |file| {
        file.close(io);
    }
}

pub fn loadStreaming(self: *@This(), io: std.Io, cwd: Io.Dir) !void {
    self.file = try cwd.openFile(io, self.cli.filename, .{ .mode = .read_only });
    self.reader = self.file.?.reader(io, self.buffer);
}

pub fn loadAllocOwned(self: *@This(), allocator: mem.Allocator, io: std.Io, cwd: Io.Dir) ![]u8 {
    return try cwd.readFileAlloc(io, self.cli.filename, allocator, .unlimited);
}

pub fn getReader(self: *@This()) *Io.Reader {
    return &self.reader.?.interface;
}

pub fn sentenceFiller(self: *@This(), delimiter: u8) SentenceFiller {
    return .init(self.getReader(), delimiter);
}

pub const Work = struct {
    buffer: []u8,
    len: usize = 0,

    pub fn init(buffer: []u8) Work {
        std.debug.assert(buffer.len > 0);

        return .{
            .buffer = buffer,
        };
    }

    pub fn slice(self: *const Work) []const u8 {
        return self.buffer[0..self.len];
    }

    fn reset(self: *Work) void {
        self.len = 0;
    }
};

pub const SentenceFiller = struct {
    reader: *Io.Reader,
    delimiter: u8,

    pub fn init(reader: *Io.Reader, delimiter: u8) SentenceFiller {
        return .{
            .reader = reader,
            .delimiter = delimiter,
        };
    }

    pub fn next(self: *SentenceFiller, work_item: *Work) !void {
        work_item.reset();
        while (work_item.len < work_item.buffer.len) : (work_item.len += 1) {
            const byte = self.reader.takeByte() catch |err| switch (err) {
                error.EndOfStream => {
                    if (work_item.len == 0) {
                        return error.Canceled;
                    }
                    return;
                },
                else => return err,
            };

            if (byte == self.delimiter) {
                return;
            }

            work_item.buffer[work_item.len] = byte;
        }
    }
};
