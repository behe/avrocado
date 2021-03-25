defmodule :m_avro_json_decoder do
  use Bitwise
  def decode_schema(jSON) do
    decode_schema(jSON, _Opts = [])
  end

  def decode_value(jsonValue, schema, maybeLkup, options) do
    lkup = :avro_util.ensure_lkup_fun(maybeLkup)
    decodedJson = decode_json(jsonValue)
    parse(decodedJson, schema, lkup, options)
  end

  def decode_value(jsonValue, schema, storeOrLkupFun) do
    decode_value(jsonValue, schema, storeOrLkupFun,
                   :avro.make_decoder_options([]))
  end

  defp parse_record_fields(fields) do
    :lists.map(fn {fieldAttrs} ->
                    parse_record_field(fieldAttrs)
               end,
                 fields)
  end

  defp parse_record_field(attrs) do
    name = :avro_util.get_opt("name", attrs)
    doc = :avro_util.get_opt("doc", attrs, "")
    type = :avro_util.get_opt("type", attrs)
    default = :avro_util.get_opt("default", attrs, :undefined)
    order = :avro_util.get_opt("order", attrs, "ascending")
    aliases = :avro_util.get_opt("aliases", attrs, [])
    fieldType = parse_schema(type)
    r_avro_record_field(name: name, doc: doc, type: fieldType,
        default: default, order: parse_order(order),
        aliases: parse_aliases(aliases))
  end

  defp parse_order("ascending") do
    :ascending
  end

  defp parse_order("descending") do
    :descending
  end

  defp parse_order("ignore") do
    :ignore
  end

  defp parse_enum_type(attrs) do
    nameBin = :avro_util.get_opt("name", attrs)
    nsBin = :avro_util.get_opt("namespace", attrs, "")
    doc = :avro_util.get_opt("doc", attrs, "")
    aliases = :avro_util.get_opt("aliases", attrs, [])
    symbols = :avro_util.get_opt("symbols", attrs)
    custom = filter_custom_props(attrs, ["symbols"])
    :avro_enum.type(nameBin, parse_enum_symbols(symbols),
                      [{:namespace, nsBin}, {:doc, doc}, {:aliases,
                                                            parse_aliases(aliases)} |
                                                             custom])
  end

  defp parse_enum_symbols([_ | _] = symbolsArray) do
    symbolsArray
  end

  defp parse_array_type(attrs) do
    items = :avro_util.get_opt("items", attrs)
    custom = filter_custom_props(attrs, ["items"])
    :avro_array.type(parse_schema(items), custom)
  end

  defp parse_map_type(attrs) do
    values = :avro_util.get_opt("values", attrs)
    custom = filter_custom_props(attrs, ["values"])
    :avro_map.type(parse_schema(values), custom)
  end

  defp parse_fixed_size(n) when is_integer(n) and n > 0 do
    n
  end

  defp parse_union_type(attrs) do
    types = :lists.map(fn schema ->
                            parse_schema(schema)
                       end,
                         attrs)
    :avro_union.type(types)
  end

  defp parse_aliases(aliasesArray) when is_list(aliasesArray) do
    :lists.map(fn aliasBin when is_binary(aliasBin) ->
                    :ok = :avro_util.verify_dotted_name(aliasBin)
                    aliasBin
               end,
                 aliasesArray)
  end

  defp parse_bytes(bytesStr) do
    :erlang.list_to_binary(parse_bytes(bytesStr, []))
  end

  defp parse_bytes(<<>>, acc) do
    :lists.reverse(acc)
  end

  defp parse_bytes(<<"\\u00", b1, b0, rest :: binary>>, acc) do
    byte = :erlang.list_to_integer([b1, b0], 16)
    parse_bytes(rest, [byte | acc])
  end

  defp parse_record({attrs}, type, lkup,
            %{record_type: recordType, is_wrapped: isWrapped,
                hook: hook} = options) do
    hook.(type, :none, attrs,
            fn jsonValues ->
                 fields = convert_attrs_to_record_fields(jsonValues,
                                                           type, lkup, options)
                 case ({isWrapped, recordType}) do
                   {true, _} ->
                     :avro_record.new(type, fields)
                   {false, :proplist} ->
                     fields
                   {false, :map} ->
                     :maps.from_list(fields)
                 end
            end)
  end

  defp convert_attrs_to_record_fields(attrs, type, lkup, %{hook: hook} = options) do
    :lists.map(fn {fieldName, value} ->
                    fieldType = :avro_record.get_field_type(fieldName, type)
                    fieldValue = hook.(type, fieldName, value,
                                         fn jsonV ->
                                              parse(jsonV, fieldType, lkup,
                                                      options)
                                         end)
                    {fieldName, fieldValue}
               end,
                 attrs)
  end

  defp parse_array(v, type, lkup,
            %{hook: hook, is_wrapped: isWrapped} = options)
      when is_list(v) do
    itemsType = :avro_array.get_items_type(type)
    {_Index, parsedArray} = :lists.foldl(fn item,
                                              {index, acc} ->
                                              callback = fn jsonV ->
                                                              parse(jsonV,
                                                                      itemsType,
                                                                      lkup,
                                                                      options)
                                                         end
                                              parsedItem = hook.(type, index,
                                                                   item,
                                                                   callback)
                                              {index + 1, [parsedItem | acc]}
                                         end,
                                           {_ZeroBasedIndexInitialValue = 0,
                                              []},
                                           v)
    items = :lists.reverse(parsedArray)
    case (isWrapped) do
      true ->
        :avro_array.new_direct(type, items)
      false ->
        items
    end
  end

  defp parse_map({attrs}, type, lkup,
            %{map_type: mapType, is_wrapped: isWrapped,
                hook: hook} = options) do
    itemsType = :avro_map.get_items_type(type)
    l = :lists.map(fn {keyBin, value} ->
                        callback = fn jsonV ->
                                        parse(jsonV, itemsType, lkup, options)
                                   end
                        v = hook.(type, keyBin, value, callback)
                        {keyBin, v}
                   end,
                     attrs)
    case (isWrapped) do
      true ->
        :avro_map.new(type, l)
      false ->
        case (mapType) do
          :proplist ->
            l
          :map ->
            :maps.from_list(l)
        end
    end
  end

  defp parse_union_ex(valueTypeName, value, unionType, lkup,
            %{hook: hook} = options) do
    hook.(unionType, valueTypeName, value,
            fn in__ ->
                 do_parse_union_ex(valueTypeName, in__, unionType, lkup,
                                     options)
            end)
  end

  defp do_parse_union_ex(valueTypeName, value, unionType, lkup,
            %{is_wrapped: isWrapped} = options) do
    case (:avro_union.lookup_type(valueTypeName,
                                    unionType)) do
      {:ok, valueType} ->
        parsedValue = parse(value, valueType, lkup, options)
        case (isWrapped) do
          true ->
            :avro_union.new_direct(unionType, parsedValue)
          false ->
            parsedValue
        end
      false ->
        :erlang.error({:unknown_union_member, valueTypeName})
    end
  end

  defp decode_json(jSON) do
    :jsone.decode(jSON, [{:object_format, :tuple}])
  end

  defp filter_custom_props(attrs, keys0) do
    keys = ["type", "name", "namespace", "doc", "aliases" | keys0]
    :avro_util.delete_opts(attrs, keys)
  end

end