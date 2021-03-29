defmodule Avrocado.Ocf do
  # require Record
  # Record.defrecord(:header, [:magic, :meta, :sync])

  def decode_file(filename) do
    decode_file(filename, :avro.make_decoder_options([]))
  end

  def decode_file(filename, options) do
    stream = File.stream!(filename, [], 65536)
    cached_stream = %Avrocado.CachedStream{stream: stream}
    decode_binary(cached_stream, options)
  end

  def decode_binary(%Avrocado.CachedStream{} = cached_stream) do
    decode_binary(cached_stream, :avro.make_decoder_options([]))
  end

  def decode_binary(%Avrocado.CachedStream{} = cached_stream, options) do
    Stream.resource(
      fn ->
        {[{"magic", _magic}, {"meta", meta}, {"sync", sync}], cached_stream} =
          decode_stream(ocf_schema(), cached_stream)

        # |> IO.inspect()

        {"avro.schema", schema_bytes} = meta |> List.keyfind("avro.schema", 0)
        codec = get_codec(meta)
        ## Ignore bad defaults because ocf schema should never need defaults
        schema = :avro.decode_schema(schema_bytes, [:ignore_bad_default_values])
        lkup = :avro.make_lkup_fun("_erlavro_ocf_root", schema)

        {lkup, schema, codec, sync, cached_stream, options}
      end,
      fn
        {lkup, schema, codec, sync, %Avrocado.CachedStream{cache: "", stream: []} = cached_stream,
         options} ->
          {:halt, {lkup, schema, codec, sync, cached_stream, options}}

        {lkup, schema, codec, sync, cached_stream, options} ->
          cached_stream = peek(cached_stream)

          {block, cached_stream} =
            decode_blocks(lkup, schema, codec, sync, cached_stream, [], options)

          {[block], {lkup, schema, codec, sync, cached_stream, options}}
      end,
      fn _ -> :ok end
    )
  end

  def decode_stream(type, %Avrocado.CachedStream{} = cached_stream) do
    Avrocado.StreamDecoder.decode_stream(cached_stream, type)
  end

  def decode_stream(type, bin) when is_binary(bin) do
    lkup = fn _ -> :erlang.error(:unexpected) end
    decode_stream(lkup, type, bin)
  end

  defp decode_stream(lkup, type, bin) when is_binary(bin) do
    # :avro_binary_decoder.decode_stream(bin, type, lkup)
    Avrocado.BinaryDecoder.decode_stream(bin, type, lkup)
  end

  def decode_stream(lkup, type, bin, options) when is_binary(bin) do
    # :avro_binary_decoder.decode_stream(bin, type, lkup, options)
    Avrocado.BinaryDecoder.decode_stream(bin, type, lkup, options)
  end

  defp decode_blocks(
         _lkup,
         _type,
         _codec,
         _sync,
         %Avrocado.CachedStream{cache: "", stream: []} = cached_stream,
         acc,
         _options
       ) do
    {acc, cached_stream}
  end

  defp decode_blocks(
         lkup,
         type,
         codec,
         sync,
         %Avrocado.CachedStream{} = cached_stream,
         acc,
         options
       ) do
    long_type = :avro_primitive.long_type()
    {count, cached_stream} = decode_stream(long_type, cached_stream)
    {size, cached_stream} = decode_stream(long_type, cached_stream)
    {bin, cached_stream} = Avrocado.CachedStream.read(cached_stream, size + 16)
    <<block::binary-size(size), ^sync::binary-size(16)>> = bin
    new_acc = decode_block(lkup, type, codec, block, count, acc, options)

    {new_acc, peek(cached_stream)}
  end

  defp peek(cached_stream) do
    {block, cached_stream} = Avrocado.CachedStream.read(cached_stream, 1)
    %Avrocado.CachedStream{cached_stream | cache: block <> cached_stream.cache}
  end

  defp decode_block(_lkup, _type, _codec, <<>>, 0, acc, _options) do
    :lists.reverse(acc)
  end

  defp decode_block(lkup, type, :deflate, bin, count, acc, options) do
    decompressed = :zlib.unzip(bin)
    decode_block(lkup, type, :null, decompressed, count, acc, options)
  end

  defp decode_block(lkup, type, :null, bin, count, acc, options) do
    {obj, tail} = decode_stream(lkup, type, bin, options)
    decode_block(lkup, type, :null, tail, count - 1, [obj | acc], options)
  end

  defp ocf_schema do
    magic_type = :avro_fixed.type("magic", 4)
    meta_type = :avro_map.type(:avro_primitive.bytes_type())
    sync_type = :avro_fixed.type("sync", 16)

    fields = [
      :avro_record.define_field("magic", magic_type),
      :avro_record.define_field("meta", meta_type),
      :avro_record.define_field("sync", sync_type)
    ]

    :avro_record.type("org.apache.avro.file.Header", fields)
  end

  # Get codec from meta fields
  defp get_codec(meta) do
    case List.keyfind(meta, "avro.codec", 0, false) do
      false -> :null
      {_, "null"} -> :null
      {_, "deflate"} -> :deflate
    end
  end
end
