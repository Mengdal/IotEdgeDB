package ingest

// BufferChangeNotifier 由 database 包实现，接收缓冲变更通知。
// 用于在 ingest 和 database 包之间解耦，避免循环依赖。
type BufferChangeNotifier interface {
	// OnFlushComplete 刷盘完成后调用。bufferKey 的数据已写入 Parquet 文件。
	OnFlushComplete(bufferKey string)

	// OnNewData 有新 batch 追加到缓冲后调用。
	OnNewData(bufferKey string)
}
