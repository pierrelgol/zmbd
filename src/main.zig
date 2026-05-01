const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const process = std.process;
const log = std.log;
const ascii = std.ascii;
const Io = std.Io;

const AvailableQueue = Io.Queue(*Loader.Work);
const ReadyQueue = Io.Queue(*Loader.Work);
var global_metrics: Metrics = .{};

const Metrics = struct {
    bytes_processed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    fn reset(self: *Metrics) void {
        self.bytes_processed.store(0, .unordered);
    }

    fn addBytes(self: *Metrics, n: usize) void {
        _ = self.bytes_processed.fetchAdd(n, .monotonic);
    }

    fn report(self: *const Metrics, elapsed_ns: u64) void {
        const bytes_processed: f64 = @floatFromInt(self.bytes_processed.load(.monotonic));
        const elapsed_ns_f64: f64 = @floatFromInt(elapsed_ns);
        const throughput: f64 = if (elapsed_ns == 0) 0 else bytes_processed / elapsed_ns_f64 * std.time.ns_per_s;
        log.info("time : {d:.3}ms | throughput: {B}/s", .{
            elapsed_ns_f64 / std.time.ns_per_ms,
            @as(u64, @intFromFloat(throughput)),
        });
    }
};

const Context = struct {
    io: std.Io,
    policy: Tokenizer.Policy,
    available_queue: *AvailableQueue,
    ready_queue: *ReadyQueue,
};

const Cli = struct {
    filename: []const u8,

    pub const empty: Cli = .{
        .filename = undefined,
    };

    pub fn parse(it: *process.Args.Iterator) !Cli {
        var result: Cli = .empty;

        while (it.next()) |arg| {
            const trimmed = mem.trim(u8, arg, &ascii.whitespace);

            if (mem.eql(u8, "-p", trimmed)) {
                result.filename = it.next() orelse return error.MissingArgument;
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
        max_seq_len: u32 = 4096,
        add_bos: bool = true,
        add_eos: bool = true,

        pub fn overhead(policy: Policy) usize {
            return @as(usize, @intFromBool(policy.add_bos)) + @as(usize, @intFromBool(policy.add_eos));
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

    const SentenceFiller = struct {
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
};

fn tokenizerWorker(context: Context) Io.Cancelable!void {
    var tokenizer: Tokenizer = .init(std.heap.page_allocator);
    defer tokenizer.deinit();

    while (true) {
        const work_item = context.ready_queue.getOne(context.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };

        const sentence = work_item.slice();
        tokenizer.encode(sentence, context.policy) catch |err| @panic(@errorName(err));
        global_metrics.addBytes(sentence.len);
        context.available_queue.putOne(context.io, work_item) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
    }
}

fn runQueuePipeline(gpa: mem.Allocator, io: std.Io, loader: *Loader) !void {
    const policy: Tokenizer.Policy = .{};
    const worker_count = 128;
    const sequence_len = policy.effectiveMaxSequenceLength();

    const all_sequence_bytes = try gpa.alloc(u8, worker_count * sequence_len);
    defer gpa.free(all_sequence_bytes);

    const work_items = try gpa.alloc(Loader.Work, worker_count);
    defer gpa.free(work_items);

    const available_storage = try gpa.alloc(*Loader.Work, worker_count);
    defer gpa.free(available_storage);

    const ready_storage = try gpa.alloc(*Loader.Work, worker_count);
    defer gpa.free(ready_storage);

    var available_queue: AvailableQueue = .init(available_storage);
    var ready_queue: ReadyQueue = .init(ready_storage);
    var group: Io.Group = .init;

    errdefer {
        ready_queue.close(io);
        available_queue.close(io);
        group.cancel(io);
        group.await(io) catch {};
    }

    for (work_items, 0..) |*work_item, i| {
        const start = i * sequence_len;
        const end = start + sequence_len;
        work_item.* = .init(all_sequence_bytes[start..end]);
        try available_queue.putOneUncancelable(io, work_item);
    }

    const worker_context = Context{
        .io = io,
        .policy = policy,
        .available_queue = &available_queue,
        .ready_queue = &ready_queue,
    };

    for (0..worker_count) |_| {
        try group.concurrent(io, tokenizerWorker, .{worker_context});
    }

    loader.loadStreaming(io, .cwd()) catch |err| {
        return log.err("{}", .{err});
    };

    var sentence_filler = loader.sentenceFiller('\n');
    while (true) {
        const work_item = try available_queue.getOneUncancelable(io);
        sentence_filler.next(work_item) catch |err| switch (err) {
            error.Canceled => {
                try available_queue.putOneUncancelable(io, work_item);
                ready_queue.close(io);
                return try group.await(io);
            },
            else => return err,
        };
        try ready_queue.putOneUncancelable(io, work_item);
    }
}

pub fn main(init: std.process.Init.Minimal) !void {
    const gpa = heap.smp_allocator;
    var threaded = Io.Threaded.init(gpa, .{
        .argv0 = .init(init.args),
        .environ = init.environ,
    });
    defer threaded.deinit();
    const io = threaded.io();

    var args = init.args.iterateAllocator(gpa) catch |err| {
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

    global_metrics.reset();
    const begin = Io.Timestamp.now(io, .awake);
    defer {
        const elapsed = begin.untilNow(io, .awake);
        global_metrics.report(@intCast(elapsed.nanoseconds));
    }

    try runQueuePipeline(gpa, io, &loader);
    std.debug.print("\n", .{});
}
