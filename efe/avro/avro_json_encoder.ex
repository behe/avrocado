defmodule :m_avro_json_encoder do
  use Bitwise
  def encode_schema(type) do
    encode_schema(type, [])
  end

  def encode_type(type) do
    encode_schema(type)
  end

  def encode_value(value) do
    encode_json(do_encode_value(value))
  end

  def encode(sc, typeOrName, value) do
    lkup = :avro_util.ensure_lkup_fun(sc)
    encode_json(do_encode(lkup, typeOrName, value))
  end

  defp encode_json(input) do
    :jsone.encode(input, [:native_utf8])
  end

  defp optional_field(_Key, default, default, _MappingFun) do
    []
  end

  defp optional_field(key, value, _Default, mappingFun) do
    [{key, mappingFun.(value)}]
  end

  defp enc_name_ref(name, _EnclosingNamespace, %{canon: true}) do
    encode_string(name)
  end

  defp enc_name_ref(name, enclosingNamespace, _Opt) do
    maybeShortName = (case (:avro.split_type_name(name,
                                                    enclosingNamespace)) do
                        {shortName, ^enclosingNamespace} ->
                          shortName
                        {_ShortName, _AnotherNamespace} ->
                          name
                      end)
    encode_string(maybeShortName)
  end

  defp enc_primitive_type(r_avro_primitive_type(name: name), %{canon: true}) do
    encode_string(name)
  end

  defp enc_primitive_type(r_avro_primitive_type(name: name, custom: []), _) do
    encode_string(name)
  end

  defp enc_primitive_type(r_avro_primitive_type(name: name, custom: custom), _) do
    [{:type, encode_string(name)} | custom]
  end

  defp encode_string(string) do
    :erlang.iolist_to_binary(string)
  end

  defp encode_integer(int) when is_integer(int) do
    int
  end

  defp encode_aliases(aliases) do
    :lists.map(&encode_string/1, aliases)
  end

  defp encode_order(:descending) do
    "descending"
  end

  defp encode_order(:ignore) do
    "ignore"
  end

  defp encode_field_with_value({fieldName, value}) do
    {encode_string(fieldName), do_encode_value(value)}
  end

  defp encode_binary(bin) do
    [?", encode_binary_body(bin), ?"]
  end

  defp encode_binary_body(<<>>) do
    ''
  end

  defp encode_binary_body(<<h1 :: size(4), h2 :: size(4),
              rest :: binary>>) do
    [?\\, ?u, ?0, ?0, to_hex(h1), to_hex(h2) |
                                      encode_binary_body(rest)]
  end

  defp to_hex(d) when d >= 0 and d <= 9 do
    d + ?0
  end

  defp to_hex(d) when d >= 10 and d <= 15 do
    d - 10 + ?a
  end

  defp encode_float(number) do
    {{:json,
        :erlang.iolist_to_binary(:io_lib.format('~p', [number]))}}
  end

end