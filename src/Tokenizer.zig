const std = @import("std");
const heap = std.heap;
const mem = std.mem;

pub const Tokenizer = @This();

arena: heap.ArenaAllocator,
ids: std.ArrayList(ByteEncoded.Id),
masks: std.ArrayList(ByteEncoded.Mask),
policy: ?Policy,

const valid_mask: ByteEncoded.Mask = 1;
const pad_mask: ByteEncoded.Mask = 0;

pub const Policy = struct {
    max_seq_len: u32 = 80,
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
        .policy = null,
    };
}

pub fn initWithPolicy(gpa: mem.Allocator, policy: Policy) !Tokenizer {
    var tokenizer = init(gpa);
    errdefer tokenizer.deinit();

    std.debug.assert(policy.max_seq_len > 0);
    tokenizer.policy = policy;
    try tokenizer.ensureTotalCapacityPrecise(policy.max_seq_len);
    return tokenizer;
}

pub fn deinit(self: *Tokenizer) void {
    defer self.* = undefined;
    defer self.arena.deinit();
}

pub fn reset(self: *Tokenizer) void {
    self.ids.clearRetainingCapacity();
    self.masks.clearRetainingCapacity();
}

fn ensureTotalCapacityPrecise(self: *Tokenizer, new_capacity: usize) !void {
    const allocator = self.arena.allocator();
    try self.ids.ensureTotalCapacityPrecise(allocator, new_capacity);
    try self.masks.ensureTotalCapacityPrecise(allocator, new_capacity);
}

pub fn encode(self: *Tokenizer, sentence: []const u8) void {
    const policy = self.policy orelse @panic("Tokenizer.encode requires initWithPolicy");
    const encoded_len = sentence.len + policy.overhead();
    std.debug.assert(encoded_len <= policy.max_seq_len);
    std.debug.assert(policy.max_seq_len > 0);

    self.reset();
    self.encodeAssumeCapacity(sentence, policy, policy.max_seq_len - encoded_len);
}

inline fn encodeAssumeCapacity(self: *Tokenizer, sentence: []const u8, policy: Policy, padding_len: usize) void {
    @branchHint(.likely);
    self.appendPrefixAssumeCapacity(policy);
    self.appendSentenceAssumeCapacity(sentence);
    self.appendSuffixAssumeCapacity(policy);
    self.padAssumeCapacity(padding_len);
}

inline fn appendPrefixAssumeCapacity(self: *Tokenizer, policy: Policy) void {
    if (policy.add_bos) {
        self.appendAssumeCapacity(.bos, valid_mask);
    }
}

inline fn appendSentenceAssumeCapacity(self: *Tokenizer, sentence: []const u8) void {
    for (sentence) |byte| {
        self.appendAssumeCapacity(.from(byte), valid_mask);
    }
}

inline fn appendSuffixAssumeCapacity(self: *Tokenizer, policy: Policy) void {
    if (policy.add_eos) {
        self.appendAssumeCapacity(.eos, valid_mask);
    }
}

inline fn appendAssumeCapacity(self: *Tokenizer, id: ByteEncoded.Id, mask: ByteEncoded.Mask) void {
    std.debug.assert(self.ids.items.len < self.ids.capacity);
    std.debug.assert(self.masks.items.len < self.masks.capacity);
    self.ids.appendAssumeCapacity(id);
    self.masks.appendAssumeCapacity(mask);
}

inline fn padAssumeCapacity(self: *Tokenizer, n: usize) void {
    std.debug.assert(self.ids.items.len + n <= self.ids.capacity);
    std.debug.assert(self.masks.items.len + n <= self.masks.capacity);
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

    pub const Mask = u1;
};
