const std = @import("std");
const heap = std.heap;
const mem = std.mem;

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

pub fn init(gpa: mem.Allocator) @This() {
    return .{
        .arena = .init(gpa),
        .ids = .empty,
        .masks = .empty,
    };
}

pub fn deinit(self: *@This()) void {
    defer self.* = undefined;
    defer self.arena.deinit();
}

pub fn reset(self: *@This()) void {
    _ = self.arena.reset(.retain_capacity);
    self.ids = .empty;
    self.masks = .empty;
}

fn paddedCapacity(len: usize) error{Overflow}!usize {
    return try std.math.ceilPowerOfTwo(usize, len);
}

fn ensureTotalCapacityPrecise(self: *@This(), new_capacity: usize) !void {
    const allocator = self.arena.allocator();
    try self.ids.ensureTotalCapacityPrecise(allocator, new_capacity);
    try self.masks.ensureTotalCapacityPrecise(allocator, new_capacity);
}

pub fn encode(self: *@This(), sentence: []const u8, policy: Policy) !void {
    const encoded_len = sentence.len + policy.overhead();
    const capacity = try paddedCapacity(encoded_len);
    const padding_len = capacity - encoded_len;

    self.reset();
    try self.ensureTotalCapacityPrecise(capacity);
    self.encodeAssumeCapacity(sentence, policy, padding_len);
}

fn encodeAssumeCapacity(self: *@This(), sentence: []const u8, policy: Policy, padding_len: usize) void {
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

fn appendAssumeCapacity(self: *@This(), id: ByteEncoded.Id, mask: ByteEncoded.Mask) void {
    self.ids.appendAssumeCapacity(id);
    self.masks.appendAssumeCapacity(mask);
}

fn padAssumeCapacity(self: *@This(), n: usize) void {
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
