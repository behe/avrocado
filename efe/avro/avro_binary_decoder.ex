defmodule :m_avro_binary_decoder do
  use Bitwise
  def decode(ioData, type, storeOrLkupFun) do
    decode(ioData, type, storeOrLkupFun,
             :avro.make_decoder_options([]))
  end

  def decode(ioData, type, storeOrLkupFun, options) do
    lkup = :avro_util.ensure_lkup_fun(storeOrLkupFun)
    {value, <<>>} = do_decode(ioData, type, lkup, options)
    value
  end

  def decode_stream(ioData, type, storeOrLkupFun, hook)
      when is_function(hook) do
    decode_stream(ioData, type, storeOrLkupFun,
                    :avro.make_decoder_options([{:hook, hook}]))
  end

  def decode_stream(ioData, type, storeOrLkupFun, options)
      when is_map(options) do
    do_decode(ioData, type,
                :avro_util.ensure_lkup_fun(storeOrLkupFun), options)
  end

  defp dec_record(bin, t, lkup,
            %{record_type: recordType} = options) do
    fieldTypes = :avro_record.get_all_field_types(t)
    {fieldValuesReversed,
       tail} = :lists.foldl(fn {fieldName, fieldType},
                                 {values, binIn} ->
                                 {value, binOut} = dec_item(t, fieldName,
                                                              fieldType, binIn,
                                                              lkup, options)
                                 {[{fieldName, value} | values], binOut}
                            end,
                              {[], bin}, fieldTypes)
    fieldValues1 = (case (recordType) do
                      :proplist ->
                        :lists.reverse(fieldValuesReversed)
                      :map ->
                        :maps.from_list(fieldValuesReversed)
                    end)
    {fieldValues1, tail}
  end

  defp dec_item(parentType, itemId, itemsType, input, lkup,
            %{hook: hook} = options) do
    hook.(parentType, itemId, input,
            fn b ->
                 do_decode(b, itemsType, lkup, options)
            end)
  end

  defp bytes(bin) do
    {size, rest} = long(bin)
    <<bytes :: size(size) - binary, tail :: binary>> = rest
    {bytes, tail}
  end

  defp blocks(bin, itemDecodeFun) do
    blocks(bin, itemDecodeFun, _Index = 1, _Acc = [])
  end

  defp blocks(bin, itemDecodeFun, index, acc) do
    {count0, rest} = long(bin)
    case (count0 === 0) do
      true ->
        {:lists.reverse(acc), rest}
      false ->
        {count, tail0} = (case (count0 < 0) do
                            true ->
                              {_Size, rest1} = long(rest)
                              {- count0, rest1}
                            false ->
                              {count0, rest}
                          end)
        block(tail0, itemDecodeFun, index, acc, count)
    end
  end

  defp block(bin, itemDecodeFun, index, acc, 0) do
    blocks(bin, itemDecodeFun, index, acc)
  end

  defp block(bin, itemDecodeFun, index, acc, count) do
    {item, tail} = itemDecodeFun.(index, bin)
    block(tail, itemDecodeFun, index + 1, [item | acc],
            count - 1)
  end

  defp int(bin) do
    zigzag(varint(bin, 0, 0, 32))
  end

  defp long(bin) do
    zigzag(varint(bin, 0, 0, 64))
  end

  def zigzag({int, tailBin}) do
    {zigzag(int), tailBin}
  end

  def zigzag(int) do
    int >>> 1 ^^^ - (int &&& 1)
  end

  defp varint(bin, acc, accBits, maxBits) do
    <<tag :: size(1), value :: size(7),
        tail :: binary>> = bin
    true = accBits < maxBits
    newAcc = value <<< accBits ||| acc
    case (tag === 0) do
      true ->
        {newAcc, tail}
      false ->
        varint(tail, newAcc, accBits + 7, maxBits)
    end
  end

end