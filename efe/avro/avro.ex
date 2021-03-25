defmodule :m_avro do
  use Bitwise
  def decode_schema(jSON) do
    :avro_json_decoder.decode_schema(jSON)
  end

  def make_lkup_fun(assignedName, type) do
    :avro_util.make_lkup_fun(assignedName, type)
  end

  def decode_schema(jSON, options) do
    :avro_json_decoder.decode_schema(jSON, options)
  end

  def encode_schema(type) do
    :avro_json_encoder.encode_schema(type)
  end

  def encode_schema(type, options) do
    :avro_json_encoder.encode_schema(type, options)
  end

  def make_encoder(schema, options) do
    lkup = :avro_util.ensure_lkup_fun(schema)
    encoding = :proplists.get_value(:encoding, options,
                                      :avro_binary)
    isWrapped = :proplists.get_bool(:wrapped, options)
    case (isWrapped) do
      true ->
        fn typeOrName, value ->
             :avro.encode_wrapped(lkup, typeOrName, value, encoding)
        end
      false ->
        fn typeOrName, value ->
             :avro.encode(lkup, typeOrName, value, encoding)
        end
    end
  end

  def make_decoder(schema, options) do
    lkup = :avro_util.ensure_lkup_fun(schema)
    decoderOptions = make_decoder_options(options)
    fn typeOrName, bin ->
         :avro.do_decode(decoderOptions, bin, typeOrName, lkup)
    end
  end

  def encode(storeOrLkup, type, value, :avro_json) do
    :avro_json_encoder.encode(storeOrLkup, type, value)
  end

  def encode(storeOrLkup, type, value, :avro_binary) do
    :avro_binary_encoder.encode(storeOrLkup, type, value)
  end

  def decode(encoding, jSON, typeOrName, storeOrLkup, hook) do
    decoderOptions = make_decoder_options([{:encoding,
                                              encoding},
                                               {:hook, hook}])
    do_decode(decoderOptions, jSON, typeOrName, storeOrLkup)
  end

  def is_named_type(r_avro_enum_type()) do
    true
  end

  def is_named_type(r_avro_fixed_type()) do
    true
  end

  def is_named_type(r_avro_record_type()) do
    true
  end

  def is_named_type(_) do
    false
  end

  def get_aliases(r_avro_array_type()) do
    []
  end

  def get_aliases(r_avro_enum_type(aliases: aliases)) do
    aliases
  end

  def get_aliases(r_avro_fixed_type(aliases: aliases)) do
    aliases
  end

  def get_aliases(r_avro_map_type()) do
    []
  end

  def get_aliases(r_avro_primitive_type()) do
    []
  end

  def get_aliases(r_avro_record_type(aliases: aliases)) do
    aliases
  end

  def get_aliases(r_avro_union_type()) do
    []
  end

  def get_custom_props(r_avro_array_type(custom: c)) do
    c
  end

  def get_custom_props(r_avro_enum_type(custom: c)) do
    c
  end

  def get_custom_props(r_avro_fixed_type(custom: c)) do
    c
  end

  def get_custom_props(r_avro_map_type(custom: c)) do
    c
  end

  def get_custom_props(r_avro_primitive_type(custom: c)) do
    c
  end

  def get_custom_props(r_avro_record_type(custom: c)) do
    c
  end

  def get_custom_props(r_avro_union_type()) do
    []
  end

  def flatten_type(type) do
    :avro_util.flatten_type(type)
  end

  def expand_type(type, sc) do
    :avro_util.expand_type(type, sc)
  end

  def expand_type_bloated(type, sc) do
    :avro_util.expand_type(type, sc, :bloated)
  end

  def is_compatible(readerSchema, writerSchema) do
    :avro_util.is_compatible(readerSchema, writerSchema)
  end

  def to_term(r_avro_value(type: t) = v) do
    to_term(t, v)
  end

  def canonical_form_fingerprint(type) do
    jSON = encode_schema(type, [{:canon, true}])
    crc64_fingerprint(jSON)
  end

  def crc64_fingerprint(bin) do
    :avro_fingerprint.crc64(bin)
  end

  defp do_cast(r_avro_primitive_type() = t, v) do
    :avro_primitive.cast(t, v)
  end

  defp do_cast(r_avro_record_type() = t, v) do
    :avro_record.cast(t, v)
  end

  defp do_cast(r_avro_enum_type() = t, v) do
    :avro_enum.cast(t, v)
  end

  defp do_cast(r_avro_array_type() = t, v) do
    :avro_array.cast(t, v)
  end

  defp do_cast(r_avro_map_type() = t, v) do
    :avro_map.cast(t, v)
  end

  defp do_cast(r_avro_union_type() = t, v) do
    :avro_union.cast(t, v)
  end

  defp do_cast(r_avro_fixed_type() = t, v) do
    :avro_fixed.cast(t, v)
  end

  def do_decode(%{encoding: :avro_json} = options, jSON,
           typeOrName, storeOrLkup) do
    :avro_json_decoder.decode_value(jSON, typeOrName,
                                      storeOrLkup,
                                      :maps.merge(options,
                                                    %{is_wrapped: false}))
  end

  def do_decode(%{encoding: :avro_binary} = options, bin,
           typeOrName, storeOrLkup) do
    :avro_binary_decoder.decode(bin, typeOrName,
                                  storeOrLkup, options)
  end

  defp to_term(r_avro_primitive_type(), v) do
    :avro_primitive.get_value(v)
  end

  defp to_term(r_avro_record_type(), v) do
    :avro_record.to_term(v)
  end

  defp to_term(r_avro_enum_type(), v) do
    :avro_enum.get_value(v)
  end

  defp to_term(r_avro_array_type(), v) do
    :avro_array.to_term(v)
  end

  defp to_term(r_avro_map_type(), v) do
    :avro_map.to_term(v)
  end

  defp to_term(r_avro_union_type(), v) do
    :avro_union.to_term(v)
  end

  defp to_term(r_avro_fixed_type(), v) do
    :avro_fixed.get_value(v)
  end

end