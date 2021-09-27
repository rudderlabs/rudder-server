package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
)

func (ch *clickhouse) writeBlock(block *data.Block) error {
	ch.logf("[writeBlock][started]")
	defer ch.logf("[writeBlock][completed]")
	ch.Lock()
	ch.logf("[writeBlock][obtained_lock]")
	defer ch.Unlock()
	if err := ch.encoder.Uvarint(protocol.ClientData); err != nil {
		return err
	}
	ch.logf("[writeBlock][encoder_Uvarint]")

	if err := ch.encoder.String(""); err != nil { // temporary table
		return err
	}
	ch.logf("[writeBlock][encoder_String]")

	// implement CityHash v 1.0.2 and add LZ4 compression
	/*
		From Alexey Milovidov
		Насколько я помню, сжимаются блоки с данными Native формата, а всё остальное (всякие номера пакетов и т. п.)  передаётся без сжатия.

		Сжатые данные устроены так. Они представляют собой набор сжатых фреймов.
		Каждый фрейм имеет следующий вид:
		чексумма (16 байт),
		идентификатор алгоритма сжатия (1 байт),
		размер сжатых данных (4 байта, little endian, размер не включает в себя чексумму, но включает в себя остальные 9 байт заголовка),
		размер несжатых данных (4 байта, little endian), затем сжатые данные.
		Идентификатор алгоритма: 0x82 - lz4, 0x90 - zstd.
		Чексумма - CityHash128 из CityHash версии 1.0.2, вычисленный от сжатых данных с учётом 9 байт заголовка.

		См. CompressedReadBufferBase, CompressedWriteBuffer,
		utils/compressor, TCPHandler.
	*/
	ch.encoder.SelectCompress(ch.compress)
	ch.logf("[writeBlock][selected_compress]")
	ch.logf("[writeBlock][block.Write]")
	err := block.Write(&ch.ServerInfo, ch.encoder)
	ch.logf("[writeBlock][block.Write][complete]")
	ch.encoder.SelectCompress(false)
	ch.logf("[writeBlock][selected_compress_false]")
	return err
}
