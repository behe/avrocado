defmodule Avrocado.Guard do
  defguard is_record(record, type) when is_tuple(record) and elem(record, 0) == type
  defguard is_primitive(record) when is_record(record, :avro_primitive_type)
  defguard is_record_type(record) when is_record(record, :avro_record_type)
  defguard is_map_type(record) when is_record(record, :avro_map_type)
  defguard is_fixed_type(record) when is_record(record, :avro_fixed_type)
  defguard is_array_type(record) when is_record(record, :avro_array_type)
  defguard is_union_type(record) when is_record(record, :avro_union_type)
  defguard is_enum_type(record) when is_record(record, :avro_enum_type)

  defguard is_name_raw(type_name)
           when is_atom(type_name) or is_list(type_name) or is_binary(type_name)
end

defmodule Avrocado.StreamDecoder do
  use Bitwise
  import Avrocado.Guard

  require Record

  Record.defrecord(:avro_primitive_type, [:name, custom: []])

  # Record.defrecord(:avro_primitive_type,
  #   name: :erlang.error({:required_field_missed, __MODULE__, :macros.line()}),
  #   custom: []
  # )

  def decode_stream(cached_stream, type) do
    decode_stream(cached_stream, type, :avro.make_decoder_options([]))
  end

  def decode_stream(cached_stream, type, options) when is_map(options) do
    do_decode(cached_stream, type, options)
  end

  defp do_decode(cached_stream, type, options) do
    dec(cached_stream, type, options)
  end

  defp dec(cached_stream, type, _options) when is_primitive(type) do
    primitive(cached_stream, avro_primitive_type(type, :name))
  end

  defp dec(cached_stream, type, options) when is_record_type(type) do
    dec_record(cached_stream, type, options)
  end

  # dec(Bin, T, _Lkup, #{hook := Hook}) when ?IS_ENUM_TYPE(T) ->
  #   {Index, Tail} = int(Bin),
  #   Hook(T, Index, Tail,
  #        fun(B) ->
  #          Symbol = avro_enum:get_symbol_from_index(T, Index),
  #          {Symbol, B}
  #        end);
  # dec(Bin, T, Lkup, Options) when ?IS_ARRAY_TYPE(T) ->
  #   ItemsType = avro_array:get_items_type(T),
  #   ItemDecodeFun =
  #     fun(Index, BinIn) ->
  #       dec_item(T, Index, ItemsType, BinIn, Lkup, Options)
  #     end,
  #   blocks(Bin, ItemDecodeFun);
  defp dec(cached_stream, type, %{map_type: map_type} = options) when is_map_type(type) do
    items_type = :avro_map.get_items_type(type)

    item_decode_fun = fn
      _index, %Avrocado.CachedStream{} = cached_stream_in ->
        {key, tail1} = string(cached_stream_in)
        {value, tail} = dec_item(items_type, tail1, options)
        {{key, value}, tail}

      _index, bin_in ->
        {key, tail1} = Avrocado.BinaryDecoder.string(bin_in)

        {value, tail} =
          Avrocado.BinaryDecoder.dec_item(type, key, items_type, tail1, fn _ -> nil end, options)

        {{key, value}, tail}
        #     _index, bin ->
        # {key, tail1} = primitive(cached_stream_in, @avro_string)
        # {value, tail} = dec_item(items_type, tail1, options)
        # {{key, value}, tail}
    end

    {key_values, tail} = blocks(cached_stream, item_decode_fun)

    case map_type do
      :proplist -> {key_values, tail}
      :map -> {:maps.from_list(key_values), tail}
    end
  end

  # dec(Bin, T, Lkup, Options) when ?IS_UNION_TYPE(T) ->
  #   {Index, Tail} = long(Bin),
  #   {ok, MemberType} = avro_union:lookup_type(Index, T),
  #   dec_item(T, Index, MemberType, Tail, Lkup, Options);
  defp dec(cached_stream, type, _options) when is_fixed_type(type) do
    size = :avro_fixed.get_size(type)
    Avrocado.CachedStream.read(cached_stream, size)
  end

  defp dec_record(cached_stream, type, %{record_type: record_type} = options) do
    field_types = :avro_record.get_all_field_types(type)

    {field_values_reversed, tail} =
      :lists.foldl(
        fn {field_name, field_type}, {values, cached_stream_in} ->
          {value, cached_stream_out} = dec_item(field_type, cached_stream_in, options)
          {[{field_name, value} | values], cached_stream_out}
        end,
        {[], cached_stream},
        field_types
      )

    field_values =
      case record_type do
        :proplist -> :lists.reverse(field_values_reversed)
        :map -> :maps.from_list(field_values_reversed)
      end

    {field_values, tail}
  end

  # Common decode logic for map/array items, union members, and record fields.
  defp dec_item(items_type, cached_stream, options) do
    do_decode(cached_stream, items_type, options)
  end

  # Decode primitive values.
  # NOTE: keep all binary decoding exceptions to error:{badmatch, _}
  #       to simplify higher level try catches when detecting error
  # primitive(Bin, ?AVRO_NULL) ->
  #   {null, Bin};
  # primitive(Bin, ?AVRO_BOOLEAN) ->
  #   <<Bool:8, Rest/binary>> = Bin,
  #   {Bool =:= 1, Rest};
  # primitive(Bin, ?AVRO_INT) ->
  #   int(Bin);
  defp primitive(cached_stream, "long"), do: long(cached_stream)

  # primitive(Bin, ?AVRO_FLOAT) ->
  #   <<Float:32/little-float, Rest/binary>> = Bin,
  #   {Float, Rest};
  # primitive(Bin, ?AVRO_DOUBLE) ->
  #   <<Float:64/little-float, Rest/binary>> = Bin,
  #   {Float, Rest};
  # primitive(Bin, ?AVRO_BYTES) ->
  #   bytes(Bin);
  defp primitive(cached_stream, "bytes"), do: bytes(cached_stream)
  defp primitive(cached_stream, "string"), do: string(cached_stream)

  defp string(cached_stream), do: bytes(cached_stream)

  defp bytes(cached_stream) do
    {size, cached_stream} = long(cached_stream)
    Avrocado.CachedStream.read(cached_stream, size)
  end

  defp blocks(cached_stream, item_decode_fun) do
    blocks(cached_stream, item_decode_fun, _index = 1, _acc = [])
  end

  defp blocks(cached_stream, item_decode_fun, index, acc) do
    {count0, cached_stream} = long(cached_stream)

    case count0 do
      0 ->
        ## a serial of blocks ends with 0
        {:lists.reverse(acc), cached_stream}

      _ ->
        # {count, cached_stream} =
        if count0 < 0 do
          ## block start with negative count number
          ## is followed by the block size in bytes
          ## here we simply discard the size info
          {size, cached_stream} = long(cached_stream)
          {rest1, cached_stream} = Avrocado.CachedStream.read(cached_stream, size)

          {blocks, ""} = Avrocado.BinaryDecoder.block(rest1, item_decode_fun, index, acc, -count0)
          # |> IO.inspect(label: :bin)
          blocks(cached_stream, item_decode_fun, index + -count0, blocks)

          # {-count0, rest1}
        else
          # {count0, cached_stream}
          block(cached_stream, item_decode_fun, index, acc, count0)
        end

        # block(cached_stream, item_decode_fun, index, acc, count)
    end
  end

  defp block(cached_stream, item_decode_fun, index, acc, 0) do
    blocks(cached_stream, item_decode_fun, index, acc)
  end

  defp block(cached_stream, item_decode_fun, index, acc, count) do
    {item, tail} = item_decode_fun.(index, cached_stream)
    block(tail, item_decode_fun, index + 1, [item | acc], count - 1)
  end

  # %% @private
  # -spec int(binary()) -> {integer(), binary()}.
  # int(Bin) -> zigzag(varint(Bin, 0, 0, 32)).

  defp long(cached_stream), do: zigzag(varint(cached_stream, 0, 0, 64))

  defp zigzag({int, cached_stream}), do: {zigzag(int), cached_stream}
  defp zigzag(int), do: bxor(int >>> 1, -(int &&& 1))

  defp varint(cached_stream, acc, acc_bits, max_bits) do
    {<<tag::1, value::7>>, cached_stream} = Avrocado.CachedStream.read(cached_stream, 1)
    ## assert
    true = acc_bits < max_bits
    new_acc = value <<< acc_bits ||| acc

    case tag do
      0 -> {new_acc, cached_stream}
      _ -> varint(cached_stream, new_acc, acc_bits + 7, max_bits)
    end
  end
end
