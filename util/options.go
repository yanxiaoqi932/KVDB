package util

type DataIndexMode uint

const (
	KeyValueMemMode DataIndexMode = iota // key和value都存储在内存的索引树中
	KeyOnlyMemMode                       // 仅有key存在内存的索引树中，value存在磁盘中
)

type Options struct {
	LogFileSizeThreshold int64  // 单个logfile的存储空间阈值
	DBPath               string // 数据库存储数据的目录
	IndexMode            DataIndexMode
	Sync                 bool // 是否每次写入entry都要刷盘，如果是True，则刷盘，但是这样会降低性能；False则有可能会因为机器crash导致未刷盘的数据丢失
	ChanBufferSize       int  // discard的通道槽数
}

func DefaultOpts() *Options {
	return &Options{
		LogFileSizeThreshold: 520 << 20, // 512mb
		DBPath:               "/tmp/kvdb",
		IndexMode:            KeyOnlyMemMode,
		Sync:                 false,
		ChanBufferSize:       8 << 20,
	}
}
