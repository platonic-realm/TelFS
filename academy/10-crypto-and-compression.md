# 10 — Crypto and compression

Both are optional; the FS works without either. Both have to be
chosen at first mount and can never change afterward, because every
chunk on the channel is encoded with the current settings. A reader
that fed a zstd-compressed chunk into the no-op decompressor would
return garbage; same for a v1-encrypted chunk into a v3 decryptor.

This chapter walks both, the pipeline order between them, and the
three-versions story that explains why the cipher abstraction looks
the way it does.

## Pipeline order: compress then encrypt

In `internal/chunk/writer.go:uploadOne`:

```go
compressed, err := w.codec.Compress(dc.data)
// ...
wire, err := w.cipher.Seal(w.ino, idx, compressed)
```

Compress before encrypt, always. Two reasons:

- **Ciphertext is high-entropy.** Encrypted bytes look random;
  compressors find no redundancy. Compressing afterward gains nothing
  and burns CPU.
- **Performance.** zstd's `SpeedDefault` is faster than AES-GCM on
  most modern CPUs (~3 GB/s vs ~2 GB/s). Doing the cheaper step on
  more data and the expensive step on less is the right shape.

The reverse order (encrypt then compress) is *never* what you want
in any system. It's worth pointing this out because it occasionally
surfaces in code reviews as a "shouldn't we encrypt to protect from
compression side channels?" — no, because there's nothing for the
compressor to leak after encryption (ciphertext is uniformly random).

## Compression

`internal/compress/compress.go`. Two codecs:

| Name | Bytes |
|---|---|
| `NameNone` | identity — no-op |
| `NameZstd` | zstd via `klauspost/compress/zstd` |

The choice is stored at `meta_kv.compression_codec` and is **immutable
after the first chunk is written**. The cmd-line setter
(`telfs init --compression`) refuses to change it post-chunk; the web
form does the same.

### Why zstd

zstd was picked over the alternatives:

- **gzip**: slower compression (DEFLATE), comparable ratios. Not worth it.
- **brotli**: better ratios than zstd on text, but ~10× slower
  compression. We're not optimizing one-shot HTTP responses; we're
  optimizing real-time write throughput. zstd wins.
- **lz4**: faster than zstd but worse ratios. We tested; ratios
  mattered more than the small encode-speed gain on a workload
  bottlenecked by upload bandwidth.
- **xz/lzma**: maximum ratios, glacial speed. Useless for our case.

zstd also has a *streaming encoder* we don't use — we compress one
chunk at a time with `EncodeAll`. That gives independent
decode-anywhere semantics: any chunk can be decompressed without
having seen previous chunks. Streaming would amortize header overhead
across chunks but break random reads.

`zstd.SpeedDefault` (level 3) is the speed/ratio sweet spot. Higher
levels gain a few percent at significant CPU cost; lower levels (1)
are barely worth running.

### Why the codec stores its decoder

```go
type Zstd struct {
    enc *zstd.Encoder
    dec *zstd.Decoder
}
```

Both `enc` and `dec` are reusable across goroutines (klauspost's
`EncodeAll`/`DecodeAll` are safe for concurrent use). The Writer
caches both inside the `Codec` so we don't pay zstd's per-call setup
cost — important when 4 concurrent writers are each compressing.

## Encryption: three versions

The cipher mode is in `meta_kv.crypto_mode`, with the values:

| Constant | String | Story |
|---|---|---|
| `ModeAESGCMv1` | `aes-gcm-v1` | The original. Passphrase + Argon2id → key. |
| `ModeAESGCMv2` | `aes-gcm-v2` | Wraps a per-FS DEK. Passphrase rotation supported. |
| `ModeAESGCMv3` | `aes-gcm-v3` | Convergent nonces. Dedup works under encryption. |

All three use AES-256-GCM with a 12-byte nonce. The difference is in
**key derivation** and **nonce strategy**.

### v1 — the simple form

Passphrase + per-FS salt + Argon2id parameters → 32 byte key. Every
chunk gets a fresh random nonce. AAD includes (ino, idx) so a swapped
chunk wouldn't decrypt.

The KDF parameters (time, memory, threads) are stored in
`meta_kv.crypto_argon` so future readers know how to derive the key.
A "canary" — a fixed plaintext encrypted with the derived key — lives
in `meta_kv.crypto_canary`. At mount, we decrypt the canary; success
proves the passphrase is correct, failure fails the mount fast (no
need to wait for a chunk decryption to discover the wrong key).

v1's problem: **you can't change the passphrase.** The passphrase
literally is the key. To "rotate" you'd have to re-encrypt every chunk
under the new key — which means re-uploading every chunk to Telegram.

### v2 — separating passphrase from data key

v2 introduces a **data encryption key (DEK)**, randomly generated at
`telfs encrypt init`. The DEK encrypts chunks. The passphrase derives
a key-encryption-key (KEK) via Argon2id; the KEK encrypts the DEK and
stores the result in `meta_kv.crypto_wrapped_dek`.

```
passphrase ─[Argon2id]─> KEK ─[AES-GCM]─> wrap(DEK) → meta_kv.wrapped_dek

chunk plaintext ─[AES-GCM(DEK, nonce, AAD)]─> chunk ciphertext
```

To rotate the passphrase: derive a new KEK, re-wrap the existing DEK,
overwrite `wrapped_dek`. The chunks on the channel are untouched. The
operation is `telfs encrypt rotate`.

This is the standard "envelope encryption" pattern. Cloud KMS uses
it; many disk encryption systems use it. It exists in TelFS because
the only-way-out problem of v1 was visible in user reports within a
month.

### v3 — convergent encryption for dedup

v2's nonces are random — same plaintext encrypted twice yields
different ciphertexts. That's textbook secure but it kills our dedup
story. A plaintext FS dedups identical bytes for free; a v2-encrypted
FS doesn't.

v3 changes the nonce strategy to **deterministic**: nonce =
`HMAC(DEK, plaintext)[:12]`. Same plaintext, same nonce, same
ciphertext — so two writes of identical bytes produce identical
on-channel bytes, and `chunk_blob` matches both.

The security tradeoff: an outside observer (Telegram) can detect that
two of your chunks contain the same plaintext, even though they can't
read either. That's a *small* information leak vs the v2 baseline,
and an *enormous* throughput win for workloads with repeated content
(backups, git clones, repeated tar of the same source tree).

The convergent cipher implements a marker interface
`crypto.Deterministic`:

```go
type Deterministic interface {
    Deterministic() bool
}
```

The Writer checks this with `dedupEnabled()`:

```go
type dedupSafe interface { Deterministic() bool }

func (w *Writer) dedupEnabled() bool {
    d, ok := w.cipher.(dedupSafe)
    return ok && d.Deterministic()
}
```

So plaintext (`NoopCipher`) and v3 (`AESGCMConvergent`) enable dedup;
v1 and v2 don't. The marker interface is a compile-checked guarantee
that you can't accidentally enable dedup on a random-nonce cipher.

> See commit `5447e52` for the v1 introduction, `9b980ce` for v2 +
> rotation, and `3cd5ce3` for v3 + convergent dedup.

## The Cipher interface

`internal/crypto/aes_gcm.go`:

```go
type Cipher interface {
    Seal(ino int64, idx int32, plaintext []byte) ([]byte, error)
    Open(ino int64, idx int32, ciphertext []byte) ([]byte, error)
}
```

Just two methods. `Seal` returns wire bytes (nonce + ciphertext +
tag); `Open` reverses. The (ino, idx) parameters become AAD —
authenticating that this chunk is bound to its inode/slot, so a
swapped chunk fails decryption.

The `NoopCipher` is a no-op implementation for plaintext FSes. It
passes `Seal` and `Open` calls straight through. The Writer treats it
identically to any other cipher; there's no special-case "is the FS
encrypted?" branch in hot paths.

## The KDF: Argon2id

We use `argon2.IDKey` (the recommended interactive variant) with
parameters chosen at `telfs encrypt init`:

```go
type ArgonParams struct {
    Time      uint32  // iterations
    MemoryKiB uint32  // memory cost in KiB
    Threads   uint8
    KeyLen    uint32
}
```

Defaults (`DefaultArgonParams`):

- `Time = 3`
- `MemoryKiB = 64 MiB`
- `Threads = 4`
- `KeyLen = 32`

These translate to ~0.5 seconds of derivation on commodity hardware.
They're stored in `meta_kv.crypto_argon` so future versions can read
back exactly what was used.

Why not scrypt or bcrypt? Argon2 is the more modern recommendation
(won the [Password Hashing Competition](https://www.password-hashing.net/)
in 2015). scrypt is fine but has known parallelism weaknesses; bcrypt
is a password-hasher, not a KDF, and doesn't support arbitrary key
lengths.

## The salt

A 16-byte random salt is generated at `telfs encrypt init` and stored
in `meta_kv.crypto_salt`. The KDF takes it. Without the salt, two
TelFS users picking the same passphrase would derive the same key
— a class break.

The salt is publicly visible on the channel (it's in the snapshot
envelope so cold recovery can re-derive). That's fine; salts aren't
secret.

## The canary

A fixed plaintext (`"telfs-canary-v1"` etc.) encrypted with the
derived key. Stored in `meta_kv.crypto_canary`. At mount, we try to
decrypt it:

```go
func VerifyCanary(c Cipher, canary []byte) error {
    plain, err := c.Open(0, 0, canary)
    if err != nil { return ErrBadPassphrase }
    if string(plain) != CanaryPlaintext { return ErrBadPassphrase }
    return nil
}
```

(ino=0, idx=0 are AAD; the canary's bytes don't belong to a real
chunk.) Success proves the passphrase is right; failure fails the
mount with a clear message.

Without the canary, a wrong passphrase would mount "successfully" but
every subsequent read would fail AES-GCM authentication and surface as
EIO. The canary turns "EIO on the first read" into "mount: bad
passphrase," which is much friendlier.

## The snapshot envelope

Snapshots include the full DB, which includes meta_kv, which includes
the encryption keys. We don't want to ship those to a public-ish
channel in the clear. So encrypted FSes wrap the gzipped DB in a
**TFSE envelope** (TelFS Snapshot Envelope) — `internal/snapshot/envelope.go`.

The envelope shape:

```
[8 bytes magic "TFSE1\0\0\0"]
[1 byte mode marker]
[arrays of public-but-needed keys: salt, argon params, canary, wrapped DEK for v2/v3]
[12 bytes nonce]
[ciphertext = AES-GCM-encrypted gzipped DB]
[16 bytes GCM tag]
```

Recovery (cold mount on a new host):

1. Download a snapshot.
2. If `IsWrapped(bytes)`, it's a TFSE envelope. Prompt for passphrase.
3. Derive KEK from passphrase + envelope's salt + envelope's argon.
4. For v2/v3, unwrap DEK from envelope's wrapped_dek.
5. Decrypt the inner blob. Result is the gzipped DB.
6. Decompress and restore.

The envelope is self-contained: everything needed to decrypt is in it,
given only the passphrase. That's required because the new host has
no local state — no `meta_kv` yet.

> See commit `9b980ce` for the v2 envelope shape with rotation; `3cd5ce3`
> for v3.

## Why the salt and KDF params live in BOTH meta_kv and the envelope

Local mount uses meta_kv (the live DB knows its own crypto state).
Cold recovery uses the envelope (there is no live DB yet). The values
are written once at `telfs encrypt init` and never change, so they
agree by construction. We pay the duplication cost (a few dozen
bytes per snapshot) for the strong invariant that recovery works
with only the channel + the passphrase.

## What encryption does not protect

The chapter is about cryptographic confidentiality of file contents.
**It does not protect against:**

- **Operator-level metadata**. Telegram still sees how many chunks
  exist, when they were uploaded, how big each is. From that pattern
  alone, a determined adversary can infer a lot (the file count says
  "this is a Linux kernel tar", the upload timing reveals work
  patterns).
- **Channel-side ban / loss**. If Telegram nukes your channel, your
  data is gone. Encryption doesn't help; backups would (we don't
  have channel-to-channel mirroring).
- **Malware on the daemon host**. The daemon has the DEK in memory.
  Any process that can read its address space wins.

The trust model is "Telegram can't read my files; channel members
can see file boundaries; my own host is trusted." That's a useful
profile for personal off-site backup; it's not enough for, say, an
encrypted-by-default cloud sync product.

## Next chapter

[11 — Operational features](11-operational-features.md) — trash,
rate limit, GC, fsck, doctor.
