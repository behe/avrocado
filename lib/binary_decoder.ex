defmodule Avrocado.BinaryDecoder do
  use Bitwise
  import Avrocado.Guard
  require Record

  Record.defrecord(:avro_primitive_type, [:name, custom: []])

  # def decode(ioData, type, storeOrLkupFun) do
  #   decode(ioData, type, storeOrLkupFun,
  #            :avro.make_decoder_options([]))
  # end

  # def decode(ioData, type, storeOrLkupFun, options) do
  #   lkup = :avro_util.ensure_lkup_fun(storeOrLkupFun)
  #   {value, <<>>} = do_decode(ioData, type, lkup, options)
  #   value
  # end

  def decode_stream(io_data, type, store_or_lkup_fun) do
    decode_stream(io_data, type, store_or_lkup_fun, :avro.make_decoder_options([]))
  end

  def decode_stream(io_data, type, store_or_lkup_fun, options) when is_map(options) do
    do_decode(io_data, type, :avro_util.ensure_lkup_fun(store_or_lkup_fun), options)
  end

  # %% @private
  # -spec do_decode(iodata(), type_or_name(), lkup_fun(),
  #                 decoder_options()) -> {avro:out(), binary()}.
  # do_decode(IoData, Type, Lkup, Options) when is_list(IoData) ->
  #   do_decode(iolist_to_binary(IoData), Type, Lkup, Options);
  def do_decode(bin, type_name, lkup, options) when is_name_raw(type_name) do
    do_decode(bin, lkup.(:avro_util.canonicalize_name(type_name)), lkup, options)
  end

  def do_decode(bin, type, lkup, %{hook: hook} = options) when is_function(hook, 4) do
    dec(bin, type, lkup, options)
  end

  defp dec(bin, type, _lkup, _options) when is_primitive(type) do
    primitive(bin, avro_primitive_type(type, :name))
  end

  defp dec(bin, type, lkup, options) when is_record_type(type) do
    dec_record(bin, type, lkup, options)
  end

  defp dec(bin, type, _lkup, %{hook: hook}) when is_enum_type(type) do
    {index, tail} = int(bin)

    hook.(type, index, tail, fn b ->
      symbol = :avro_enum.get_symbol_from_index(type, index)
      {symbol, b}
    end)
  end

  defp dec(bin, type, lkup, options) when is_array_type(type) do
    items_type = :avro_array.get_items_type(type)

    item_decode_fun = fn index, bin_in ->
      dec_item(type, index, items_type, bin_in, lkup, options)
    end

    blocks(bin, item_decode_fun)
  end

  defp dec(bin, type, lkup, %{map_type: map_type} = options) when is_map_type(type) do
    items_type = :avro_map.get_items_type(type)

    item_decode_fun = fn _index, bin_in ->
      {key, tail1} = string(bin_in)
      {value, tail} = dec_item(type, key, items_type, tail1, lkup, options)
      {{key, value}, tail}
    end

    {key_values, tail} = blocks(bin, item_decode_fun)

    case map_type do
      :proplist -> {key_values, tail}
      :map -> {:maps.from_list(key_values), tail}
    end
  end

  defp dec(bin, type, lkup, options) when is_union_type(type) do
    {index, tail} = long(bin)
    {:ok, member_type} = :avro_union.lookup_type(index, type)
    dec_item(type, index, member_type, tail, lkup, options)
  end

  defp dec(bin, type, _lkup, %{hook: hook}) when is_fixed_type(type) do
    hook.(type, "", bin, fn b ->
      size = :avro_fixed.get_size(type)
      <<value::binary-size(size), tail::binary>> = b
      {value, tail}
    end)
  end

  defp dec_record(bin, type, lkup, %{record_type: record_type} = options) do
    field_types = :avro_record.get_all_field_types(type)

    {field_values_reversed, tail} =
      :lists.foldl(
        fn {field_name, field_type}, {values, bin_in} ->
          {value, bin_out} = dec_item(type, field_name, field_type, bin_in, lkup, options)
          {[{field_name, value} | values], bin_out}
        end,
        {[], bin},
        field_types
      )

    field_values =
      case record_type do
        :proplist -> :lists.reverse(field_values_reversed)
        :map -> :maps.from_list(field_values_reversed)
      end

    {field_values, tail}
  end

  def dec_item(parentType, itemId, itemsType, input, lkup, %{hook: hook} = options) do
    hook.(parentType, itemId, input, fn b ->
      do_decode(b, itemsType, lkup, options)
    end)
  end

  def primitive(bin, "null") do
    {:null, bin}
  end

  def primitive(bin, "boolean") do
    <<bool::binary-size(1), rest::binary>> = bin
    {bool == <<1>>, rest}
  end

  def primitive(bin, "int"), do: int(bin)
  def primitive(bin, "long"), do: long(bin)

  def primitive(bin, "float") do
    <<float::float-little-size(32), rest::binary>> = bin

    {float, rest}
  end

  def primitive(bin, "double") do
    <<double::float-little-size(64), rest::binary>> = bin
    {double, rest}
  end

  def primitive(bin, "bytes") do
    {bytes, tail} = bytes(bin)
    {bytes, tail}
  end

  def primitive(bin, "string") do
    {string, tail} = string(bin)
    {string, tail}
  end

  def string(bin), do: bytes(bin)

  defp bytes(bin) do
    {size, rest} = long(bin)
    <<bytes::binary-size(size), tail::binary>> = rest
    {bytes, tail}
  end

  defp blocks(bin, item_decode_fun) do
    blocks(bin, item_decode_fun, _index = 1, _acc = [])
  end

  defp blocks(bin, item_decode_fun, index, acc) do
    {count, rest} = long(bin)

    case count do
      0 ->
        {:lists.reverse(acc), rest}

      _ ->
        # {count, tail0} =
        if count < 0 do
          {size, rest1} = long(rest)
          <<head::binary-size(size), tail::binary>> = rest1
          {rest2, ""} = block(head, item_decode_fun, index, acc, -count)
          blocks(tail, item_decode_fun, index + -count, rest2)
          # {-count, rest1}
        else
          {head, tail} = block(rest, item_decode_fun, index, acc, count)
          blocks(tail, item_decode_fun, index, head)

          # {count, rest}
        end

        # block(tail0, item_decode_fun, index, acc, count)
    end
  end

  def block(bin, _item_decode_fun, _index, acc, 0) do
    # blocks(bin, item_decode_fun, index, acc)
    # {:lists.reverse(acc), bin}
    {acc, bin}
  end

  def block(bin, item_decode_fun, index, acc, count) do
    {item, tail} = item_decode_fun.(index, bin)
    block(tail, item_decode_fun, index + 1, [item | acc], count - 1)
  end

  defp int(bin) do
    {int, tail} = zigzag(varint(bin, 0, 0, 32))
    {int, tail}
  end

  defp long(bin) do
    {long, tail} = zigzag(varint(bin, 0, 0, 64))
    {long, tail}
  end

  def zigzag({int, tail_bin}) do
    {zigzag(int), tail_bin}
  end

  def zigzag(int) do
    int >>> bxor(1, -(int &&& 1))
  end

  defp varint(bin, acc, acc_bits, max_bits) do
    <<tag::size(1), value::size(7), tail::binary>> = bin
    true = acc_bits < max_bits
    newAcc = value <<< acc_bits ||| acc

    case tag === 0 do
      true ->
        {newAcc, tail}

      false ->
        varint(tail, newAcc, acc_bits + 7, max_bits)
    end
  end
end
