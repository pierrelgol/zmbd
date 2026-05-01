const std = @import("std");
const Io = std.Io;
const mem = std.mem;

const Cli = @import("Cli.zig");
const ShardingLayer = @import("ShardingLayer.zig");

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

pub fn rangeSentenceFiller(self: *@This(), file: Io.File, io: Io, range: ShardingLayer.Range, delimiter: u8) RangeSentenceFiller {
    return .init(file, io, range, self.buffer, delimiter);
}

pub fn rangeBlockFiller(self: *@This(), file: Io.File, io: Io, range: ShardingLayer.Range, delimiter: u8) RangeBlockFiller {
    return .init(file, io, range, self.buffer, delimiter);
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

pub const Span = struct {
    start: u32,
    len: u32,
};

pub const Block = struct {
    bytes: []u8,
    spans: []Span,
    used: usize = 0,
    span_count: usize = 0,

    pub fn init(bytes: []u8, spans: []Span) Block {
        std.debug.assert(bytes.len > 0);
        std.debug.assert(spans.len > 0);

        return .{
            .bytes = bytes,
            .spans = spans,
        };
    }

    pub fn reset(self: *Block) void {
        self.used = 0;
        self.span_count = 0;
    }

    pub fn slice(self: *const Block, span: Span) []const u8 {
        const start: usize = span.start;
        const end = start + span.len;
        return self.bytes[start..end];
    }

    fn appendSpan(self: *Block, start: usize, len: usize) void {
        std.debug.assert(self.span_count < self.spans.len);
        self.spans[self.span_count] = .{
            .start = @intCast(start),
            .len = @intCast(len),
        };
        self.span_count += 1;
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

pub const RangeSentenceFiller = struct {
    file: Io.File,
    io: Io,
    range: ShardingLayer.Range,
    buffer: []u8,
    delimiter: u8,
    file_offset: usize,
    buffer_start: usize = 0,
    buffer_len: usize = 0,

    pub fn init(file: Io.File, io: Io, range: ShardingLayer.Range, buffer: []u8, delimiter: u8) RangeSentenceFiller {
        return .{
            .file = file,
            .io = io,
            .range = range,
            .buffer = buffer,
            .delimiter = delimiter,
            .file_offset = range.start,
        };
    }

    fn refill(self: *RangeSentenceFiller) !bool {
        if (self.file_offset >= self.range.end) {
            self.buffer_start = 0;
            self.buffer_len = 0;
            return false;
        }

        const to_read = @min(self.buffer.len, self.range.end - self.file_offset);
        const amt = try self.file.readPositionalAll(self.io, self.buffer[0..to_read], self.file_offset);
        self.file_offset += amt;
        self.buffer_start = 0;
        self.buffer_len = amt;
        return amt != 0;
    }

    pub fn next(self: *RangeSentenceFiller, work_item: *Work) !void {
        work_item.reset();

        while (true) {
            if (self.buffer_start == self.buffer_len) {
                const has_more = try self.refill();
                if (!has_more) {
                    if (work_item.len == 0) {
                        return error.Canceled;
                    }
                    return;
                }
            }

            const unread = self.buffer[self.buffer_start..self.buffer_len];
            const remaining = work_item.buffer.len - work_item.len;

            if (remaining == 0) {
                return;
            }

            if (mem.indexOfScalar(u8, unread, self.delimiter)) |pos| {
                const line = unread[0..pos];
                const copy_len = @min(line.len, remaining);
                mem.copyForwards(u8, work_item.buffer[work_item.len .. work_item.len + copy_len], line[0..copy_len]);
                work_item.len += copy_len;
                self.buffer_start += copy_len;

                if (copy_len < line.len) {
                    return;
                }

                self.buffer_start += 1;
                if (work_item.len == 0) {
                    continue;
                }
                return;
            }

            const copy_len = @min(unread.len, remaining);
            mem.copyForwards(u8, work_item.buffer[work_item.len .. work_item.len + copy_len], unread[0..copy_len]);
            work_item.len += copy_len;
            self.buffer_start += copy_len;

            if (copy_len == remaining) {
                return;
            }
        }
    }
};

pub const RangeBlockFiller = struct {
    file: Io.File,
    io: Io,
    range: ShardingLayer.Range,
    buffer: []u8,
    delimiter: u8,
    file_offset: usize,
    buffer_start: usize = 0,
    buffer_len: usize = 0,

    pub fn init(file: Io.File, io: Io, range: ShardingLayer.Range, buffer: []u8, delimiter: u8) RangeBlockFiller {
        return .{
            .file = file,
            .io = io,
            .range = range,
            .buffer = buffer,
            .delimiter = delimiter,
            .file_offset = range.start,
        };
    }

    fn refill(self: *RangeBlockFiller) !bool {
        if (self.file_offset >= self.range.end) {
            self.buffer_start = 0;
            self.buffer_len = 0;
            return false;
        }

        const to_read = @min(self.buffer.len, self.range.end - self.file_offset);
        const amt = try self.file.readPositionalAll(self.io, self.buffer[0..to_read], self.file_offset);
        self.file_offset += amt;
        self.buffer_start = 0;
        self.buffer_len = amt;
        return amt != 0;
    }

    pub fn nextBlock(self: *RangeBlockFiller, block: *Block, max_sentence_len: usize) !void {
        block.reset();

        while (block.span_count < block.spans.len) {
            const sentence_start = block.used;

            while (block.used - sentence_start < max_sentence_len) {
                if (self.buffer_start == self.buffer_len) {
                    const has_more = try self.refill();
                    if (!has_more) {
                        if (block.used == sentence_start) {
                            if (block.span_count == 0) {
                                return error.Canceled;
                            }
                            return;
                        }

                        block.appendSpan(sentence_start, block.used - sentence_start);
                        return;
                    }
                }

                const unread = self.buffer[self.buffer_start..self.buffer_len];
                const remaining = max_sentence_len - (block.used - sentence_start);

                if (mem.indexOfScalar(u8, unread, self.delimiter)) |pos| {
                    const line = unread[0..pos];
                    const copy_len = @min(line.len, remaining);
                    mem.copyForwards(u8, block.bytes[block.used .. block.used + copy_len], line[0..copy_len]);
                    block.used += copy_len;
                    self.buffer_start += copy_len;

                    if (copy_len < line.len) {
                        break;
                    }

                    self.buffer_start += 1;
                    if (block.used == sentence_start) {
                        break;
                    }
                    break;
                }

                const copy_len = @min(unread.len, remaining);
                mem.copyForwards(u8, block.bytes[block.used .. block.used + copy_len], unread[0..copy_len]);
                block.used += copy_len;
                self.buffer_start += copy_len;

                if (copy_len == remaining) {
                    break;
                }
            }

            if (block.used == sentence_start) {
                continue;
            }

            block.appendSpan(sentence_start, block.used - sentence_start);

            if (block.used == block.bytes.len) {
                return;
            }
        }
    }
};
