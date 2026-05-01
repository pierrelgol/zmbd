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
const ShardQueue = ShardingLayer.ReadyQueue;
const loader_buffer_size = 256 * 1024;
const batch_size = 32;
var global_metrics: Metrics = .{};

const Batch = struct {
    items: []*Loader.Work,
    count: usize = 0,

    fn init(items: []*Loader.Work) Batch {
        std.debug.assert(items.len > 0);
        return .{ .items = items };
    }

    fn reset(self: *Batch) void {
        self.count = 0;
    }
};

const FreeBatchQueue = Io.Queue(*Batch);
const ReadyBatchQueue = Io.Queue(*Batch);

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
    free_batch_queue: *FreeBatchQueue,
    ready_queue: *ReadyBatchQueue,
};

const LoaderContext = struct {
    io: std.Io,
    cli: Cli,
    file: Io.File,
    shard_queue: *ShardQueue,
    available_queue: *AvailableQueue,
    free_batch_queue: *FreeBatchQueue,
    ready_queues: []ReadyBatchQueue,
};

fn tokenizerWorker(context: Context) Io.Cancelable!void {
    var tokenizer: Tokenizer = Tokenizer.initWithPolicy(std.heap.page_allocator, context.policy) catch |err| @panic(@errorName(err));
    defer tokenizer.deinit();

    while (true) {
        const work_item = context.ready_queue.getOne(context.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };

        for (work_item.items[0..work_item.count]) |sentence_work| {
            tokenizer.encode(sentence_work.slice());
            context.available_queue.putOne(context.io, sentence_work) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => return error.Canceled,
            };
        }

        work_item.reset();
        context.free_batch_queue.putOne(context.io, work_item) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
    }
}

fn submitBatch(context: LoaderContext, batch: *Batch, target_queue: *ReadyBatchQueue) Io.Cancelable!void {
    if (batch.count == 0) {
        context.free_batch_queue.putOne(context.io, batch) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
        return;
    }

    target_queue.putOne(context.io, batch) catch |err| switch (err) {
        error.Closed => return,
        error.Canceled => return error.Canceled,
    };
}

fn loaderWorker(context: LoaderContext) Io.Cancelable!void {
    var loader_buffer: [loader_buffer_size]u8 = undefined;
    var loader: Loader = .init(context.cli, &loader_buffer);
    defer loader.deinit(context.io);

    var local_bytes_processed: usize = 0;
    var next_queue_index: usize = 0;
    var batch = context.free_batch_queue.getOne(context.io) catch |err| switch (err) {
        error.Closed => return,
        error.Canceled => return error.Canceled,
    };
    defer global_metrics.addBytes(local_bytes_processed);
    defer {
        if (batch.count != 0) {
            submitBatch(context, batch, &context.ready_queues[next_queue_index]) catch {};
        } else {
            context.free_batch_queue.putOne(context.io, batch) catch {};
        }
    }

    while (true) {
        const shard_item = context.shard_queue.getOne(context.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };

        local_bytes_processed += shard_item.range.len();
        var sentence_filler = loader.rangeSentenceFiller(context.file, context.io, shard_item.range, '\n');
        while (true) {
            const work_item = context.available_queue.getOne(context.io) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => return error.Canceled,
            };

            sentence_filler.next(work_item) catch |err| switch (err) {
                error.Canceled => {
                    context.available_queue.putOne(context.io, work_item) catch |put_err| switch (put_err) {
                        error.Closed => return,
                        error.Canceled => return error.Canceled,
                    };
                    break;
                },
                else => @panic(@errorName(err)),
            };

            batch.items[batch.count] = work_item;
            batch.count += 1;

            if (batch.count == batch.items.len) {
                try submitBatch(context, batch, &context.ready_queues[next_queue_index]);
                next_queue_index = (next_queue_index + 1) % context.ready_queues.len;
                batch = context.free_batch_queue.getOne(context.io) catch |err| switch (err) {
                    error.Closed => return,
                    error.Canceled => return error.Canceled,
                };
            }
        }
    }
}

fn runQueuePipeline(gpa: mem.Allocator, io: std.Io, opt: Cli) !void {
    const policy: Tokenizer.Policy = .{ .max_seq_len = opt.max_seq_length };
    const worker_count: usize = opt.worker_count;
    const loader_count: usize = opt.loader_count;
    const sequence_len = policy.effectiveMaxSequenceLength();
    const effective_batch_size: usize = batch_size;
    const batch_count = @max(loader_count * 2, worker_count);
    // Keep enough sentence buffers to back every in-flight batch.
    const work_item_count = @max(worker_count, batch_count * effective_batch_size);

    const all_sequence_bytes = try gpa.alloc(u8, work_item_count * sequence_len);
    defer gpa.free(all_sequence_bytes);

    const work_items = try gpa.alloc(Loader.Work, work_item_count);
    defer gpa.free(work_items);

    const available_storage = try gpa.alloc(*Loader.Work, work_item_count);
    defer gpa.free(available_storage);

    const batches = try gpa.alloc(Batch, batch_count);
    defer gpa.free(batches);
    const batch_item_storage = try gpa.alloc(*Loader.Work, batch_count * effective_batch_size);
    defer gpa.free(batch_item_storage);
    const free_batch_storage = try gpa.alloc(*Batch, batch_count);
    defer gpa.free(free_batch_storage);
    const ready_queues = try gpa.alloc(ReadyBatchQueue, worker_count);
    defer gpa.free(ready_queues);
    const ready_queue_storage = try gpa.alloc(*Batch, worker_count * loader_count);
    defer gpa.free(ready_queue_storage);

    var available_queue: AvailableQueue = .init(available_storage);
    var free_batch_queue: FreeBatchQueue = .init(free_batch_storage);
    var tokenizer_group: Io.Group = .init;
    var loader_group: Io.Group = .init;

    errdefer {
        for (ready_queues) |*queue| {
            queue.close(io);
        }
        free_batch_queue.close(io);
        available_queue.close(io);
        loader_group.cancel(io);
        tokenizer_group.cancel(io);
        loader_group.await(io) catch {};
        tokenizer_group.await(io) catch {};
    }

    for (work_items, 0..) |*work_item, i| {
        const start = i * sequence_len;
        const end = start + sequence_len;
        work_item.* = .init(all_sequence_bytes[start..end]);
        try available_queue.putOneUncancelable(io, work_item);
    }

    for (batches, 0..) |*batch, i| {
        const start = i * effective_batch_size;
        const end = start + effective_batch_size;
        batch.* = .init(batch_item_storage[start..end]);
        try free_batch_queue.putOneUncancelable(io, batch);
    }

    for (ready_queues, 0..) |*queue, i| {
        const start = i * opt.loader_count;
        const end = start + opt.loader_count;
        queue.* = .init(ready_queue_storage[start..end]);
    }

    for (ready_queues) |*ready_queue| {
        const worker_context = Context{
            .io = io,
            .policy = policy,
            .available_queue = &available_queue,
            .free_batch_queue = &free_batch_queue,
            .ready_queue = ready_queue,
        };
        try tokenizer_group.concurrent(io, tokenizerWorker, .{worker_context});
    }

    var sharding_layer = ShardingLayer.open(io, .cwd(), opt.filename) catch |err| {
        return log.err("{}", .{err});
    };
    defer sharding_layer.deinit(io);

    try sharding_layer.initQueue(gpa, io, opt.loader_count, '\n');
    sharding_layer.getReadyQueue().close(io);

    const loader_context = LoaderContext{
        .io = io,
        .cli = opt,
        .file = sharding_layer.file,
        .shard_queue = sharding_layer.getReadyQueue(),
        .available_queue = &available_queue,
        .free_batch_queue = &free_batch_queue,
        .ready_queues = ready_queues,
    };

    for (0..loader_count) |_| {
        try loader_group.concurrent(io, loaderWorker, .{loader_context});
    }

    try loader_group.await(io);
    for (ready_queues) |*queue| {
        queue.close(io);
    }
    return try tokenizer_group.await(io);
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

    global_metrics.reset();
    const begin = Io.Timestamp.now(io, .awake);
    defer {
        const elapsed = begin.untilNow(io, .awake);
        global_metrics.report(@intCast(elapsed.nanoseconds));
    }

    try runQueuePipeline(gpa, io, cli);
    std.debug.print("\n", .{});
}
