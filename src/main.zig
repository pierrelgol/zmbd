const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const process = std.process;
const log = std.log;
const ascii = std.ascii;
const Io = std.Io;

const Cli = struct {
    filename: []const u8,
    bench_encode: bool = false,

    pub const empty: Cli = .{
        .filename = undefined,
        .bench_encode = false,
    };

    pub fn parse(it: *process.Args.Iterator) !Cli {
        var result: Cli = .empty;

        while (it.next()) |arg| {
            const trimmed = mem.trim(u8, arg, &ascii.whitespace);

            if (mem.eql(u8, "-p", trimmed)) {
                result.filename = it.next() orelse return error.MissingArgument;
            }

            if (mem.eql(u8, "-be", trimmed)) {
                result.bench_encode = true;
            }
        }

        return result;
    }
};

const Tokenizer = struct {
    arena: heap.ArenaAllocator,
    ids: std.ArrayList(ByteEncoded.Id),
    masks: std.ArrayList(ByteEncoded.Mask),
    const valid_mask: ByteEncoded.Mask = 1;
    const pad_mask: ByteEncoded.Mask = 0;

    pub const Policy = struct {
        max_seq_len: u32 = 128,
        add_bos: bool = true,
        add_eos: bool = true,

        pub fn overhead(policy: Policy) usize {
            return @intFromBool(policy.add_bos) + @intFromBool(policy.add_eos);
        }

        pub fn effectiveMaxSequenceLength(policy: Policy) usize {
            return policy.max_seq_len -| policy.overhead();
        }
    };

    pub fn init(gpa: mem.Allocator) Tokenizer {
        return .{
            .arena = .init(gpa),
            .ids = .empty,
            .masks = .empty,
        };
    }

    pub fn deinit(self: *Tokenizer) void {
        defer self.* = undefined;
        defer self.arena.deinit();
    }

    pub fn reset(self: *Tokenizer) void {
        _ = self.arena.reset(.retain_capacity);
        self.ids = .empty;
        self.masks = .empty;
    }

    fn paddedCapacity(len: usize) error{Overflow}!usize {
        return try std.math.ceilPowerOfTwo(usize, len);
    }

    fn ensureTotalCapacityPrecise(self: *Tokenizer, new_capacity: usize) !void {
        const allocator = self.arena.allocator();
        try self.ids.ensureTotalCapacityPrecise(allocator, new_capacity);
        try self.masks.ensureTotalCapacityPrecise(allocator, new_capacity);
    }

    pub fn encode(self: *Tokenizer, sentence: []const u8, policy: Policy) !void {
        const encoded_len = sentence.len + policy.overhead();
        const capacity = try paddedCapacity(encoded_len);
        const padding_len = capacity - encoded_len;

        self.reset();
        try self.ensureTotalCapacityPrecise(capacity);
        self.encodeAssumeCapacity(sentence, policy, padding_len);
    }

    fn encodeAssumeCapacity(self: *Tokenizer, sentence: []const u8, policy: Policy, padding_len: usize) void {
        if (policy.add_bos) {
            self.appendAssumeCapacity(.bos, valid_mask);
        }

        for (sentence) |byte| {
            self.appendAssumeCapacity(.from(byte), valid_mask);
        }

        if (policy.add_eos) {
            self.appendAssumeCapacity(.eos, valid_mask);
        }

        self.padAssumeCapacity(padding_len);
    }

    fn appendAssumeCapacity(self: *Tokenizer, id: ByteEncoded.Id, mask: ByteEncoded.Mask) void {
        self.ids.appendAssumeCapacity(id);
        self.masks.appendAssumeCapacity(mask);
    }

    fn padAssumeCapacity(self: *Tokenizer, n: usize) void {
        self.ids.appendNTimesAssumeCapacity(.pad, n);
        self.masks.appendNTimesAssumeCapacity(pad_mask, n);
    }

    pub const ByteEncoded = struct {
        pub const Id = enum(u32) {
            bos = 256,
            eos = 257,
            pad = 258,
            _,

            pub fn from(raw: u32) Id {
                return @as(Id, @enumFromInt(raw));
            }

            pub fn to(id: Id) u32 {
                return @as(u32, @intFromEnum(id));
            }
        };
        // 0 marks padding, 1 marks tokens consumed by the model.
        pub const Mask = u1;
    };
};

const Loader = struct {
    cli: Cli,
    file: ?Io.File,
    buffer: []u8,
    reader: ?Io.File.Reader,

    pub const WorkItem = struct {
        buffer: []u8,
        len: usize = 0,

        pub fn init(buffer: []u8) WorkItem {
            std.debug.assert(buffer.len > 0);

            return .{
                .buffer = buffer,
            };
        }

        pub fn slice(self: *const WorkItem) []const u8 {
            return self.buffer[0..self.len];
        }

        fn reset(self: *WorkItem) void {
            self.len = 0;
        }
    };

    const SentenceFiller = struct {
        reader: *Io.Reader,
        delimiter: u8,
        pending: ?[]const u8 = null,
        offset: usize = 0,

        pub fn init(reader: *Io.Reader, delimiter: u8) SentenceFiller {
            return .{
                .reader = reader,
                .delimiter = delimiter,
            };
        }

        pub fn fillNext(self: *SentenceFiller, work_item: *WorkItem) !void {
            work_item.reset();

            if (self.pending) |sentence| {
                self.takeChunkInto(sentence, work_item);
                return;
            }

            const sentence = try self.nextSentence() orelse return error.Canceled;
            self.pending = sentence;
            self.offset = 0;
            self.takeChunkInto(sentence, work_item);
        }

        fn nextSentence(self: *SentenceFiller) !?[]const u8 {
            const sentence = self.reader.takeDelimiterExclusive(self.delimiter) catch |err| switch (err) {
                error.EndOfStream => return null,
                else => return err,
            };
            self.reader.toss(1);
            return sentence;
        }

        fn takeChunkInto(self: *SentenceFiller, sentence: []const u8, work_item: *WorkItem) void {
            const end = @min(self.offset + work_item.buffer.len, sentence.len);
            const chunk = sentence[self.offset..end];

            @memcpy(work_item.buffer[0..chunk.len], chunk);
            work_item.len = chunk.len;

            self.offset = end;
            if (self.offset == sentence.len) {
                self.pending = null;
                self.offset = 0;
            }
        }
    };

    pub fn init(cli: Cli, buffer: []u8) Loader {
        return .{
            .cli = cli,
            .buffer = buffer,
            .reader = null,
            .file = null,
        };
    }

    pub fn deinit(self: Loader, io: std.Io) void {
        if (self.file) |file| {
            file.close(io);
        }
    }

    pub fn loadStreaming(self: *Loader, io: std.Io, cwd: Io.Dir) !void {
        self.file = try cwd.openFile(io, self.cli.filename, .{ .mode = .read_only });
        self.reader = self.file.?.reader(io, self.buffer);
    }

    pub fn loadAllocOwned(self: *Loader, allocator: mem.Allocator, io: std.Io, cwd: Io.Dir) ![]u8 {
        return try cwd.readFileAlloc(io, self.cli.filename, allocator, .unlimited);
    }

    pub fn getReader(self: *Loader) *Io.Reader {
        return &self.reader.?.interface;
    }

    pub fn sentenceFiller(self: *Loader, delimiter: u8) SentenceFiller {
        return .init(self.getReader(), delimiter);
    }
};

pub fn encodeStreaming(gpa: mem.Allocator, io: std.Io, loader: *Loader, tokenizer: *Tokenizer) !void {
    const policy: Tokenizer.Policy = .{};
    const sequence_buffer = try gpa.alloc(u8, policy.effectiveMaxSequenceLength());
    defer gpa.free(sequence_buffer);

    loader.loadStreaming(io, .cwd()) catch |err| {
        return log.err("{}", .{err});
    };

    var work_item: Loader.WorkItem = .init(sequence_buffer);
    var sentence_filler = loader.sentenceFiller('\n');

    const begin = Io.Timestamp.now(io, .awake);
    var bytes_written: usize = 0;
    defer {
        const elapsed = begin.untilNow(io, .awake);
        const throughput = @as(f64, @floatFromInt(bytes_written)) / @as(f64, @floatFromInt(elapsed.nanoseconds)) * @as(f64, std.time.ns_per_s);
        log.info("time : {f} | throughput: {B}/s", .{ elapsed, @as(usize, @intFromFloat(throughput)) });
    }
    while (true) {
        sentence_filler.fillNext(&work_item) catch |err| switch (err) {
            error.Canceled => break,
            else => return err,
        };

        const sentence = work_item.slice();
        bytes_written += sentence.len;
        try tokenizer.encode(sentence, policy);
    }
}

pub fn encodeAlloc(gpa: mem.Allocator, io: std.Io, loader: *Loader, tokenizer: *Tokenizer) !void {
    const file_content = try loader.loadAllocOwned(gpa, io, .cwd());
    defer gpa.free(file_content);

    const begin = Io.Timestamp.now(io, .awake);
    var bytes_written: usize = 0;
    defer {
        const elapsed = begin.untilNow(io, .awake);
        const throughput = @as(f64, @floatFromInt(bytes_written)) / @as(f64, @floatFromInt(elapsed.nanoseconds)) * @as(f64, std.time.ns_per_s);
        log.info("time : {f} | throughput: {B}/s", .{ elapsed, @as(usize, @intFromFloat(throughput)) });
    }
    var it = mem.tokenizeScalar(u8, file_content, '\n');
    while (it.next()) |sentence| {
        bytes_written += sentence.len;
        try tokenizer.encode(sentence, .{});
    }
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = init.minimal.args.iterateAllocator(gpa) catch |err| {
        return log.err("{}", .{err});
    };
    defer args.deinit();

    const cli = Cli.parse(&args) catch |err| {
        return log.err("{}", .{err});
    };
    std.debug.print("{s}\n", .{cli.filename});

    var loader_buffer: [heap.pageSize()]u8 = undefined;
    var loader: Loader = .init(cli, &loader_buffer);
    defer loader.deinit(io);

    var tokenizer: Tokenizer = .init(gpa);
    defer tokenizer.deinit();

    if (cli.bench_encode) {
        try encodeAlloc(gpa, io, &loader, &tokenizer);
    } else {
        try encodeStreaming(gpa, io, &loader, &tokenizer);
    }
    std.debug.print("\n", .{});
}
