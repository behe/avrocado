defmodule Avro.Ocf do
  require Record
  Record.defrecord(:header, [:magic, :meta, :sync])

  def decode_file(filename) do
    decode_file(filename, :avro.make_decoder_options([]))
  end

  def decode_file(filename, options) do
    {:ok, bin} = :file.read_file(filename)
    decode_binary(bin, options)
  end

  def decode_binary(bin, options) do
    {[{"magic", magic}, {"meta", meta}, {"sync", sync}], tail} = decode_stream(ocf_schema(), bin)

    {"avro.schema", schema_bytes} = meta |> List.keyfind("avro.schema", 0)

    codec = get_codec(meta)
    ## Ignore bad defaults because ocf schema should never need defaults
    schema = :avro.decode_schema(schema_bytes, [:ignore_bad_default_values])
    lkup = :avro.make_lkup_fun("_erlavro_ocf_root", schema)
    header = header(magic: magic, meta: meta, sync: sync)
    {header, schema, decode_blocks(lkup, schema, codec, sync, tail, [], options)}
  end

  def decode_stream(type, bin) when is_binary(bin) do
    lkup = fn _ -> :erlang.error(:unexpected) end
    decode_stream(lkup, type, bin)
  end

  defp decode_stream(lkup, type, bin) when is_binary(bin) do
    :avro_binary_decoder.decode_stream(bin, type, lkup)
  end

  def decode_stream(lkup, type, bin, options) when is_binary(bin) do
    :avro_binary_decoder.decode_stream(bin, type, lkup, options)
  end

  defp decode_blocks(_lkup, _type, _codec, _sync, <<>>, acc, _options) do
    :lists.reverse(acc)
  end

  defp decode_blocks(lkup, type, codec, sync, bin0, acc, options) do
    long_type = :avro_primitive.long_type()
    {count, bin1} = decode_stream(lkup, long_type, bin0)
    {size, bin} = decode_stream(lkup, long_type, bin1)
    <<block::binary-size(size), ^sync::binary-size(16), tail::binary>> = bin
    new_acc = decode_block(lkup, type, codec, block, count, acc, options)
    decode_blocks(lkup, type, codec, sync, tail, new_acc, options)
  end

  defp decode_block(_lkup, _type, _codec, <<>>, 0, acc, _options), do: acc
  # decode_block(Lkup, Type, deflate, Bin, Count, Acc, Options) ->
  #   Decompressed = zlib:unzip(Bin),
  #   decode_block(Lkup, Type, null, Decompressed, Count, Acc, Options);
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
    case List.keyfind(meta, "avro.codec", 0) do
      false -> :null
      {_, "null"} -> :null
      {_, "deflate"} -> :deflate
    end
  end
end
