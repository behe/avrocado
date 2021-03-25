defmodule :m_avro_record do
  use Bitwise
  def type(name, fields) do
    type(name, fields, [])
  end

  def resolve_fullname(r_avro_record_type(fullname: fullName, fields: fields,
             aliases: aliases) = t,
           ns) do
    case (:avro.build_type_fullname(fullName, ns)) do
      ^fullName ->
        t
      newFullName ->
        newFields = resolve_field_type_fullnames(fields, ns)
        newAliases = :avro_util.canonicalize_aliases(aliases,
                                                       ns)
        r_avro_record_type(t, fullname: newFullName,  fields: newFields, 
               aliases: newAliases)
    end
  end

  def define_field(name, type) do
    define_field(name, type, [])
  end

  def get_all_field_data(r_avro_record_type(fields: fields)) do
    :lists.map(fn r_avro_record_field(name: fieldName, type: fieldTypeOrName,
                      default: default) ->
                    {fieldName, fieldTypeOrName, default}
               end,
                 fields)
  end

  def parse_defaults(r_avro_record_type(fields: fields, fullname: fullName) = t,
           parseFun) do
    f = fn r_avro_record_field(type: fieldType0, default: default,
               name: fieldName) = field ->
             fieldType = resolve_default_type(fieldType0)
             newDefault = parse_default(fullName, fieldName,
                                          parseFun, fieldType, default)
             newFieldType = :avro_util.parse_defaults(fieldType0,
                                                        parseFun)
             r_avro_record_field(field, type: newFieldType,  default: newDefault)
        end
    r_avro_record_type(t, fields: :lists.map(f, fields))
  end

  def encode_defaults(r_avro_record_type(fields: fields, fullname: fullName) = t,
           lkup) do
    f = fn r_avro_record_field(type: fieldType0, default: default,
               name: fieldName) = field ->
             fieldType = resolve_default_type(fieldType0)
             newDefault = encode_default(fullName, fieldName, lkup,
                                           fieldType, default)
             newFieldType = :avro_util.encode_defaults(fieldType0,
                                                         lkup)
             r_avro_record_field(field, type: newFieldType,  default: newDefault)
        end
    r_avro_record_type(t, fields: :lists.map(f, fields))
  end

  defp encode_default(fullName, fieldName, lkup, type, value) do
    f = fn v ->
             :erlang.iolist_to_binary(:avro_json_encoder.encode(lkup,
                                                                  type, v))
        end
    do_default(fullName, fieldName, f, value)
  end

  defp parse_default(fullName, fieldName, parseFun, type, value) do
    doFun = fn v ->
                 parseFun.(type, v)
            end
    do_default(fullName, fieldName, doFun, value)
  end

  def set_values(values, record) do
    :lists.foldl(fn {fieldName, value}, r ->
                      set_value(fieldName, value, r)
                 end,
                   record, values)
  end

  defp resolve_field_type_fullnames(fields, ns) do
    f = fn r_avro_record_field(type: type) = field ->
             r_avro_record_field(field, type: :avro.resolve_fullname(type, ns))
        end
    :lists.map(f, fields)
  end

  defp lookup_value_by_name([], _Values) do
    false
  end

  defp lookup_value_by_name([fieldName | rest], values) do
    case (:maps.find(fieldName, values)) do
      {:ok, value} ->
        {:ok, value}
      :error ->
        lookup_value_by_name(rest, values)
    end
  end

  defp lookup_value_from_map(fieldDef, values) do
    r_avro_record_field(name: fieldName, default: default,
        aliases: aliases) = fieldDef
    case (lookup_value_by_name([fieldName | aliases],
                                 values)) do
      {:ok, value} ->
        value
      false ->
        default
    end
  end

  defp get_field_def_by_alias(_Alias, []) do
    false
  end

  defp get_field_def_by_alias(alias, [fieldDef | rest]) do
    case (:lists.member(alias, r_avro_record_field(fieldDef, :aliases))) do
      true ->
        {:ok, fieldDef}
      false ->
        get_field_def_by_alias(alias, rest)
    end
  end

end