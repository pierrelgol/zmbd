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

    pub fn encode(self: *Tokenizer, sentence: []const u8) !void {
        _ = self.arena.reset(.retain_capacity);
        self.ids = .empty;
        self.masks = .empty;
        const pow_of_2_padded_size = try std.math.ceilPowerOfTwo(usize, sentence.len + 2);
        // std.debug.print("{d}\n", .{pow_of_2_padded_size});

        try self.ids.ensureTotalCapacityPrecise(self.arena.allocator(), pow_of_2_padded_size);
        try self.masks.ensureTotalCapacityPrecise(self.arena.allocator(), pow_of_2_padded_size);

        self.beginSentenceAssumeCapacity();
        self.encodeSentenceAssumeCapacity(sentence);
        self.endSentenceAssumeCapacity();
        self.padSentenceAssumeCapacity(pow_of_2_padded_size - (sentence.len + 2));
    }

    fn beginSentenceAssumeCapacity(self: *Tokenizer) void {
        self.ids.appendAssumeCapacity(.bos);
        self.masks.appendAssumeCapacity(1);
    }

    fn encodeSentenceAssumeCapacity(self: *Tokenizer, sentence: []const u8) void {
        for (sentence) |byte| {
            self.ids.appendAssumeCapacity(.from(byte));
            self.masks.appendAssumeCapacity(@as(u1, 1));
        }
    }

    fn endSentenceAssumeCapacity(self: *Tokenizer) void {
        self.ids.appendAssumeCapacity(.eos);
        self.masks.appendAssumeCapacity(1);
    }

    fn padSentenceAssumeCapacity(self: *Tokenizer, n: usize) void {
        self.ids.appendNTimesAssumeCapacity(.pad, n);
        self.masks.appendNTimesAssumeCapacity(@as(u1, 0), n);
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
        // 0 is for padding, 1 is for attentions
        pub const Mask = u1;
    };
};

const Loader = struct {
    cli: Cli,
    file: ?Io.File,
    buffer: []u8,
    reader: ?Io.File.Reader,

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
};

pub fn encodeStreaming(io: std.Io, loader: *Loader, tokenizer: *Tokenizer) !void {
    loader.loadStreaming(io, .cwd()) catch |err| {
        return log.err("{}", .{err});
    };

    const reader = loader.getReader();

    const begin = Io.Timestamp.now(io, .awake);
    var bytes_written: usize = 0;
    defer {
        const elapsed = begin.untilNow(io, .awake);
        const throughput = @as(f64, @floatFromInt(bytes_written)) / @as(f64, @floatFromInt(elapsed.nanoseconds)) * @as(f64, std.time.ns_per_s);
        log.info("time : {f} | throughput: {B}/s", .{ elapsed, @as(usize, @intFromFloat(throughput)) });
    }
    while (reader.takeDelimiterExclusive('\n') catch null) |sentence| {
        reader.toss(1);

        bytes_written += sentence.len;
        try tokenizer.encode(sentence);

        // for (tokenizer.ids.items, tokenizer.masks.items) |id, m| {
        //     std.debug.print("{d}:{d} ", .{ id, m });
        // }
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
        try tokenizer.encode(sentence);
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
        try encodeStreaming(io, &loader, &tokenizer);
    }
    std.debug.print("\n", .{});
}
