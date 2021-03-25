defmodule :m_avro_ocf do
  use Bitwise
  require Record
  Record.defrecord(:r_header, :header, magic: :undefined, meta: :undefined, sync: :undefined)

  def decode_file(filename) do
    decode_file(filename, :avro.make_decoder_options([]))
  end

  def decode_file(filename, options) do
    {:ok, bin} = :file.read_file(filename)
    decode_binary(bin, options)
  end

  def decode_binary(bin) do
    decode_binary(bin, :avro.make_decoder_options([]))
  end

  def decode_binary(bin, options) do
    {[{"magic", magic}, {"meta", meta}, {"sync", sync}], tail} = decode_stream(ocf_schema(), bin)
    {_, schemaBytes} = :lists.keyfind("avro.schema", 1, meta)
    codec = get_codec(meta)

    schema =
      :avro.decode_schema(
        schemaBytes,
        [:ignore_bad_default_values]
      )

    lkup = :avro.make_lkup_fun('_erlavro_ocf_root', schema)
    header = r_header(magic: magic, meta: meta, sync: sync)
    {header, schema, decode_blocks(lkup, schema, codec, sync, tail, [], options)}
  end

  def write_file(filename, lkup, schema, objects) do
    write_file(filename, lkup, schema, objects, [])
  end

  def write_file(filename, lkup, schema, objects, meta) do
    header = make_header(schema, meta)
    {:ok, fd} = :file.open(filename, [:write])

    try do
      :ok = write_header(fd, header)
      :ok = append_file(fd, header, lkup, schema, objects)
    after
      :file.close(fd)
    end
  end

  def write_header(fd, header) do
    headerBytes = encode_header(header)
    :ok = :file.write(fd, headerBytes)
  end

  def append_file(fd, header, objects) do
    ioData = make_block(header, objects)
    :ok = :file.write(fd, ioData)
  end

  def append_file(fd, header, lkup, schema, objects) do
    encodedObjects =
      for o <- objects do
        :avro.encode(lkup, schema, o, :avro_binary)
      end

    append_file(fd, header, encodedObjects)
  end

  def make_header(type) do
    make_header(type, _ExtraMeta = [])
  end

  def make_header(type, meta0) do
    validatedMeta = validate_meta(meta0)

    meta =
      case :lists.keyfind("avro.codec", 1, validatedMeta) do
        false ->
          [{"avro.codec", "null"} | validatedMeta]

        _ ->
          validatedMeta
      end

    typeJson = :avro_json_encoder.encode_type(type)

    r_header(
      magic: <<"Obj", 1>>,
      meta: [{"avro.schema", :erlang.iolist_to_binary(typeJson)} | meta],
      sync: generate_sync_bytes()
    )
  end

  def make_ocf(header, objects) do
    headerBytes = encode_header(header)
    dataBytes = make_block(header, objects)
    [headerBytes, dataBytes]
  end

  defp encode_header(header) do
    headerFields = [
      {'magic', r_header(header, :magic)},
      {'meta', r_header(header, :meta)},
      {'sync', r_header(header, :sync)}
    ]

    headerRecord =
      :avro_record.new(
        ocf_schema(),
        headerFields
      )

    :avro_binary_encoder.encode_value(headerRecord)
  end

  defp make_block(header, objects) do
    count = length(objects)
    data = encode_block(r_header(header, :meta), objects)
    size = :erlang.size(data)

    [
      :avro_binary_encoder.encode_value(:avro_primitive.long(count)),
      :avro_binary_encoder.encode_value(:avro_primitive.long(size)),
      data,
      r_header(header, :sync)
    ]
  end

  defp validate_meta([]) do
    []
  end

  defp validate_meta([{k0, v} | rest]) do
    k = :erlang.iolist_to_binary(k0)
    is_reserved_meta_key(k) and :erlang.error({:reserved_meta_key, k0})

    is_invalid_codec_meta(
      k,
      v
    ) and :erlang.error({:bad_codec, v})

    is_binary(v) or :erlang.error({:bad_meta_value, v})
    [{k, v} | validate_meta(rest)]
  end

  defp is_reserved_meta_key("avro.codec") do
    false
  end

  defp is_reserved_meta_key(<<"avro.", _::binary>>) do
    true
  end

  defp is_reserved_meta_key(_) do
    false
  end

  defp is_invalid_codec_meta("avro.codec", "null") do
    false
  end

  defp is_invalid_codec_meta("avro.codec", "deflate") do
    false
  end

  defp is_invalid_codec_meta("avro.codec", _) do
    true
  end

  defp is_invalid_codec_meta(_, _) do
    false
  end

  defp generate_sync_bytes() do
    :crypto.strong_rand_bytes(16)
  end

  defp decode_stream(type, bin) when is_binary(bin) do
    lkup = fn _ ->
      :erlang.error(:unexpected)
    end

    :avro_binary_decoder.decode_stream(bin, type, lkup)
  end

  defp decode_stream(lkup, type, bin) when is_binary(bin) do
    :avro_binary_decoder.decode_stream(bin, type, lkup)
  end

  defp decode_stream(lkup, type, bin, options) when is_binary(bin) do
    :avro_binary_decoder.decode_stream(bin, type, lkup, options)
  end

  defp decode_blocks(_Lkup, _Type, _Codec, _Sync, <<>>, acc, _Options) do
    :lists.reverse(acc)
  end

  defp decode_blocks(lkup, type, codec, sync, bin0, acc, options) do
    longType = :avro_primitive.long_type()
    {count, bin1} = decode_stream(lkup, longType, bin0)
    IO.inspect(count, label: :count)
    {size, bin} = decode_stream(lkup, longType, bin1)
    <<block::size(size)-binary, ^sync::size(16)-binary, tail::binary>> = bin
    newAcc = decode_block(lkup, type, codec, block, count, acc, options)
    decode_blocks(lkup, type, codec, sync, tail, newAcc, options)
  end

  defp decode_block(_Lkup, _Type, _Codec, <<>>, 0, acc, _Options) do
    acc
  end

  defp decode_block(lkup, type, :deflate, bin, count, acc, options) do
    decompressed = :zlib.unzip(bin)
    decode_block(lkup, type, :null, decompressed, count, acc, options)
  end

  defp decode_block(lkup, type, :null, bin, count, acc, options) do
    {obj, tail} = decode_stream(lkup, type, bin, options)
    decode_block(lkup, type, :null, tail, count - 1, [obj | acc], options)
  end

  defp ocf_schema() do
    magicType = :avro_fixed.type('magic', 4)
    metaType = :avro_map.type(:avro_primitive.bytes_type())
    syncType = :avro_fixed.type('sync', 16)

    fields = [
      :avro_record.define_field('magic', magicType),
      :avro_record.define_field('meta', metaType),
      :avro_record.define_field('sync', syncType)
    ]

    :avro_record.type('org.apache.avro.file.Header', fields)
  end

  defp get_codec(meta) do
    case :lists.keyfind("avro.codec", 1, meta) do
      false ->
        :null

      {_, "null"} ->
        :null

      {_, "deflate"} ->
        :deflate
    end
  end

  defp encode_block(meta, data) do
    case get_codec(meta) do
      :null ->
        :erlang.iolist_to_binary(data)

      :deflate ->
        :zlib.zip(data)
    end
  end
end
