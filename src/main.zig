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

const FreeBlockQueue = Io.Queue(*Loader.Block);
const ReadyBlockQueue = Io.Queue(*Loader.Block);
const ShardQueue = ShardingLayer.ReadyQueue;
const loader_buffer_size = 4 * 1024 * 1024;
const block_sentence_capacity = 1024;
const asset_dir_name = "asset";
var global_metrics: Metrics = .{};

const AssetEntry = struct {
    path: []u8,
    size: u64,
};

const Metrics = struct {
    bytes_processed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    fn reset(self: *Metrics) void {
        self.bytes_processed.store(0, .unordered);
    }

    fn addBytes(self: *Metrics, n: usize) void {
        _ = self.bytes_processed.fetchAdd(n, .monotonic);
    }

    fn throughput(self: *const Metrics, elapsed: Io.Duration) u64 {
        const bytes_processed: f64 = @floatFromInt(self.bytes_processed.load(.monotonic));
        const elapsed_ns_f64: f64 = @floatFromInt(elapsed.nanoseconds);
        const bytes_per_second: f64 = if (elapsed.nanoseconds == 0) 0 else bytes_processed / elapsed_ns_f64 * std.time.ns_per_s;
        return @intFromFloat(bytes_per_second);
    }

    fn report(self: *const Metrics, elapsed: Io.Duration) void {
        log.info("time : {f} | throughput: {Bi}/s", .{
            elapsed,
            self.throughput(elapsed),
        });
    }
};

const Context = struct {
    io: std.Io,
    policy: Tokenizer.Policy,
    free_block_queue: *FreeBlockQueue,
    ready_queue: *ReadyBlockQueue,
};

const LoaderContext = struct {
    io: std.Io,
    file: Io.File,
    shard_queue: *ShardQueue,
    max_sentence_len: usize,
    free_block_queue: *FreeBlockQueue,
    ready_queues: []ReadyBlockQueue,
};

fn tokenizerWorker(context: Context) Io.Cancelable!void {
    var tokenizer: Tokenizer = Tokenizer.initWithPolicy(std.heap.page_allocator, context.policy) catch |err| @panic(@errorName(err));
    defer tokenizer.deinit();

    while (true) {
        const block = context.ready_queue.getOne(context.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };

        for (block.spans[0..block.span_count]) |span| {
            tokenizer.encode(block.slice(span));
        }

        block.reset();
        context.free_block_queue.putOne(context.io, block) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
    }
}

fn submitBlock(context: LoaderContext, block: *Loader.Block, target_queue: *ReadyBlockQueue) Io.Cancelable!void {
    if (block.span_count == 0) {
        context.free_block_queue.putOne(context.io, block) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
        return;
    }

    target_queue.putOne(context.io, block) catch |err| switch (err) {
        error.Closed => return,
        error.Canceled => return error.Canceled,
    };
}

fn loaderWorker(context: LoaderContext) Io.Cancelable!void {
    var loader_buffer: [loader_buffer_size]u8 = undefined;
    var loader: Loader = .init(&loader_buffer);
    defer loader.deinit(context.io);

    var local_bytes_processed: usize = 0;
    var next_queue_index: usize = 0;
    var block = context.free_block_queue.getOne(context.io) catch |err| switch (err) {
        error.Closed => return,
        error.Canceled => return error.Canceled,
    };
    defer global_metrics.addBytes(local_bytes_processed);
    defer {
        if (block.span_count != 0) {
            submitBlock(context, block, &context.ready_queues[next_queue_index]) catch {};
        } else {
            context.free_block_queue.putOne(context.io, block) catch {};
        }
    }

    while (true) {
        const shard_item = context.shard_queue.getOne(context.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };

        local_bytes_processed += shard_item.range.len();
        var sentence_filler = loader.rangeBlockFiller(context.file, context.io, shard_item.range, '\n');
        while (true) {
            sentence_filler.nextBlock(block, context.max_sentence_len) catch |err| switch (err) {
                error.Canceled => {
                    break;
                },
                else => @panic(@errorName(err)),
            };

            if (block.span_count == block.spans.len or block.used == block.bytes.len) {
                try submitBlock(context, block, &context.ready_queues[next_queue_index]);
                next_queue_index = (next_queue_index + 1) % context.ready_queues.len;
                block = context.free_block_queue.getOne(context.io) catch |err| switch (err) {
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
    const block_count = @max(loader_count * 2, worker_count);
    const block_bytes_len = sequence_len * block_sentence_capacity;

    const all_block_bytes = try gpa.alloc(u8, block_count * block_bytes_len);
    defer gpa.free(all_block_bytes);
    const all_block_spans = try gpa.alloc(Loader.Span, block_count * block_sentence_capacity);
    defer gpa.free(all_block_spans);
    const blocks = try gpa.alloc(Loader.Block, block_count);
    defer gpa.free(blocks);
    const free_block_storage = try gpa.alloc(*Loader.Block, block_count);
    defer gpa.free(free_block_storage);
    const ready_queues = try gpa.alloc(ReadyBlockQueue, worker_count);
    defer gpa.free(ready_queues);
    const ready_queue_storage = try gpa.alloc(*Loader.Block, worker_count * block_count);
    defer gpa.free(ready_queue_storage);

    var free_block_queue: FreeBlockQueue = .init(free_block_storage);
    var tokenizer_group: Io.Group = .init;
    var loader_group: Io.Group = .init;

    errdefer {
        for (ready_queues) |*queue| {
            queue.close(io);
        }
        free_block_queue.close(io);
        loader_group.cancel(io);
        tokenizer_group.cancel(io);
        loader_group.await(io) catch {};
        tokenizer_group.await(io) catch {};
    }

    for (blocks, 0..) |*block, i| {
        const byte_start = i * block_bytes_len;
        const byte_end = byte_start + block_bytes_len;
        const span_start = i * block_sentence_capacity;
        const span_end = span_start + block_sentence_capacity;
        block.* = .init(all_block_bytes[byte_start..byte_end], all_block_spans[span_start..span_end]);
        try free_block_queue.putOneUncancelable(io, block);
    }

    for (ready_queues, 0..) |*queue, i| {
        const start = i * block_count;
        const end = start + block_count;
        queue.* = .init(ready_queue_storage[start..end]);
    }

    for (ready_queues) |*ready_queue| {
        const worker_context = Context{
            .io = io,
            .policy = policy,
            .free_block_queue = &free_block_queue,
            .ready_queue = ready_queue,
        };
        try tokenizer_group.concurrent(io, tokenizerWorker, .{worker_context});
    }

    var sharding_layer = ShardingLayer.open(io, .cwd(), opt.filename.?) catch |err| {
        return log.err("{}", .{err});
    };
    defer sharding_layer.deinit(io);

    try sharding_layer.initQueue(gpa, io, opt.loader_count, '\n');
    sharding_layer.getReadyQueue().close(io);

    const loader_context = LoaderContext{
        .io = io,
        .file = sharding_layer.file,
        .shard_queue = sharding_layer.getReadyQueue(),
        .max_sentence_len = sequence_len,
        .free_block_queue = &free_block_queue,
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

fn runOne(gpa: mem.Allocator, io: Io, cli: Cli, pretty: bool, size: u64) !void {
    global_metrics.reset();
    const begin = Io.Timestamp.now(io, .awake);
    try runQueuePipeline(gpa, io, cli);
    const elapsed = begin.untilNow(io, .awake);

    if (pretty) {
        std.debug.print("{s: <24}  {Bi: >12}  {f: >10}  {Bi: >24}/s\n", .{
            cli.filename.?,
            size,
            elapsed,
            global_metrics.throughput(elapsed),
        });
    } else {
        std.debug.print("{s}\n", .{cli.filename.?});
        global_metrics.report(elapsed);
        std.debug.print("\n", .{});
    }
}

fn runAssetSuite(gpa: mem.Allocator, io: Io, cli: Cli) !void {
    var dir = try Io.Dir.cwd().openDir(io, asset_dir_name, .{ .iterate = true });
    defer dir.close(io);

    var it = dir.iterate();
    var assets = try std.ArrayList(AssetEntry).initCapacity(gpa, 16);
    defer {
        for (assets.items) |entry| gpa.free(entry.path);
        assets.deinit(gpa);
    }

    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;

        const path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ asset_dir_name, entry.name });
        errdefer gpa.free(path);
        const stat = try dir.statFile(io, entry.name, .{});
        try assets.append(gpa, .{
            .path = path,
            .size = stat.size,
        });
    }

    const lessThan = struct {
        fn lessThan(_: void, lhs: AssetEntry, rhs: AssetEntry) bool {
            return lhs.size < rhs.size;
        }
    }.lessThan;

    std.mem.sort(AssetEntry, assets.items, {}, lessThan);
    std.debug.print("{s: <24}  {s: >12}  {s: >10}  {s: >24}\n", .{ "file", "size", "time", "throughput" });

    for (assets.items) |entry| {
        var file_cli = cli;
        file_cli.filename = entry.path;
        try runOne(gpa, io, file_cli, true, entry.size);
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

    if (cli.filename) |_| {
        try runOne(gpa, io, cli, false, 0);
    } else {
        try runAssetSuite(gpa, io, cli);
    }
}
