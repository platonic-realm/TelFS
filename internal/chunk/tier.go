package chunk

// Multi-tier chunk size: the writer picks a per-inode chunk size at
// first dispatch based on how much data has accumulated in the
// dirty buffer. Once picked, it's locked for the file — the inode
// row stores it, and the reader uses it for offset→idx math.
//
// Rationale:
//   - Small files don't pay for big chunks: a 100 KB file with 64
//     MiB chunks would still be one chunk of 100 KB plaintext +
//     padding semantics is awkward.
//   - Big files don't pay for small chunks: a 5 GB file at 4 MiB
//     chunks is 1280 chunk_map rows + 1280 packer/pool submits.
//     At 64 MiB chunks it's 80, an order of magnitude less metadata.
//
// Tiny tier (below the packer's threshold) is implicit — chunks
// smaller than DefaultPackThreshold get aggregated by the packer
// regardless of the inode's chunk_size. Tiering here is about the
// per-inode chunk slot size; tiny files' tier is "small" by default
// and the packer takes them over from there.
const (
	// TierSmallChunkSize is the default — what most files get and
	// what writes below TierMidThreshold use.
	TierSmallChunkSize int64 = 4 << 20 // 4 MiB
	// TierMidChunkSize is for files in the 64 MB – 512 MB range.
	TierMidChunkSize int64 = 16 << 20 // 16 MiB
	// TierBigChunkSize is for files in the 512 MB – 4 GB range.
	TierBigChunkSize int64 = 32 << 20 // 32 MiB
	// TierVeryBigChunkSize is for files ≥ 4 GB.
	TierVeryBigChunkSize int64 = 64 << 20 // 64 MiB
)

const (
	// TierMidThreshold: promote small → mid above this size.
	TierMidThreshold int64 = 64 << 20 // 64 MiB
	// TierBigThreshold: promote mid → big above this size.
	TierBigThreshold int64 = 512 << 20 // 512 MiB
	// TierVeryBigThreshold: promote big → very_big above this size.
	TierVeryBigThreshold int64 = 4 << 30 // 4 GiB
)

// PickTierChunkSize returns the chunk size for a file whose first
// dispatch sees `dirtyBytes` of accumulated content. Stepped at the
// thresholds above; never returns smaller than the FS default
// (which is what an inode is created with).
func PickTierChunkSize(dirtyBytes int64) int64 {
	switch {
	case dirtyBytes >= TierVeryBigThreshold:
		return TierVeryBigChunkSize
	case dirtyBytes >= TierBigThreshold:
		return TierBigChunkSize
	case dirtyBytes >= TierMidThreshold:
		return TierMidChunkSize
	default:
		return TierSmallChunkSize
	}
}
