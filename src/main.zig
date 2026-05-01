const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const log = std.log;
const Io = std.Io;
const Cli = @import("Cli.zig");
const ShardingLayer = @import("ShardingLayer.zig");
const Loader = @import("Loader.zig");
const Tokenizer = @import("Tokenizer.zig");
comptime {
    std.testing.refAllDecls(@This());
}

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

fn tokenizerWorker(context: Context) Io.Cancelable!void {
    var tokenizer: Tokenizer = Tokenizer.initWithPolicy(std.heap.page_allocator, context.policy) catch |err| @panic(@errorName(err));
    defer tokenizer.deinit();
    var bytes_processed: usize = 0;
    defer global_metrics.addBytes(bytes_processed);

    while (true) {
        const work_item = context.ready_queue.getOne(context.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };

        const sentence = work_item.slice();
        tokenizer.encode(sentence);
        bytes_processed += sentence.len;
        context.available_queue.putOne(context.io, work_item) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
    }
}

fn runQueuePipeline(gpa: mem.Allocator, io: std.Io, loader: *Loader, worker_count: usize, max_seq_len: u32) !void {
    const policy: Tokenizer.Policy = .{ .max_seq_len = max_seq_len };
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

    global_metrics.reset();
    const begin = Io.Timestamp.now(io, .awake);
    defer {
        const elapsed = begin.untilNow(io, .awake);
        global_metrics.report(@intCast(elapsed.nanoseconds));
    }

    try runQueuePipeline(gpa, io, &loader, cli.worker_count, cli.max_seq_length);
    std.debug.print("\n", .{});
}
