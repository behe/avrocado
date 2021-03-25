defmodule :m_avro_util do
  use Bitwise
  def validate(type, opts) do
    allowBadRefs = :proplists.get_bool(:allow_bad_references,
                                         opts)
    allowTypeRedef = :proplists.get_bool(:allow_type_redefine,
                                           opts)
    validate(type, allowBadRefs, allowTypeRedef)
  end

  def make_lkup_fun(assignedName, type) do
    store0 = :avro_schema_store.new([:dict])
    store = :avro_schema_store.add_type(assignedName, type,
                                          store0)
    :avro_schema_store.to_lookup_fun(store)
  end

  def get_opt(key, opts) do
    case (:lists.keyfind(key, 1, opts)) do
      {^key, value} ->
        value
      false ->
        :erlang.error({:not_found, key})
    end
  end

  def get_opt(key, opts, default) do
    case (:lists.keyfind(key, 1, opts)) do
      {^key, value} ->
        value
      false ->
        default
    end
  end

  def delete_opts(kvList, keys) do
    :lists.foldl(fn k, acc ->
                      :lists.keydelete(k, 1, acc)
                 end,
                   kvList, keys)
  end

  def canonicalize_custom_props(props0) do
    props = delete_opts(props0,
                          [:namespace, :doc, :aliases])
    jSON = :jsone.encode(make_jsone_input(props))
    :jsone.decode(jSON, [{:object_format, :proplist}])
  end

  def verify_aliases(aliases) do
    :lists.foreach(fn alias ->
                        verify_dotted_name(alias)
                   end,
                     aliases)
  end

  def verify_type(type) do
    true = :avro.is_named_type(type)
    verify_type_name(type)
  end

  def canonicalize_aliases(aliases, ns) do
    :lists.map(fn alias ->
                    :avro.build_type_fullname(alias, ns)
               end,
                 aliases)
  end

  def canonicalize_name(name) do
    ensure_binary(name)
  end

  def ensure_binary(a) when is_atom(a) do
    :erlang.atom_to_binary(a, :utf8)
  end

  def ensure_binary(l) when is_list(l) do
    :erlang.iolist_to_binary(l)
  end

  def ensure_binary(b) when is_binary(b) do
    b
  end

  def expand_type(type, sc) do
    expand_type(type, sc, :compact)
  end

  def resolve_duplicated_refs(type0) do
    {type, _Refs} = resolve_duplicated_refs(type0, [])
    type
  end

  def is_compatible(reader, writer) do
    try do
      do_is_compatible_next(reader, writer, [], [])
    catch
      {:not_compatible, rPath, wPath} ->
        {false,
           {:not_compatible, :lists.reverse(rPath),
              :lists.reverse(wPath)}}
      {:reader_missing_defalut_value, path} ->
        {false,
           {:reader_missing_default_value, :lists.reverse(path)}}
    end
  end

  defp do_is_compatible_next(reader, writer, rPath, wPath) do
    newRPath = [:avro.get_type_fullname(reader) | rPath]
    newWPath = [:avro.get_type_fullname(writer) | wPath]
    do_is_compatible(reader, writer, newRPath, newWPath)
  end

  defp flatten(r_avro_primitive_type() = primitive) do
    {primitive, []}
  end

  defp flatten(r_avro_enum_type() = enum) do
    {enum, []}
  end

  defp flatten(r_avro_fixed_type() = fixed) do
    {fixed, []}
  end

  defp flatten(r_avro_record_type() = record) do
    {newFields, extractedTypes} = :lists.foldr(fn field,
                                                    {fieldsAcc, extractedAcc} ->
                                                    {newType,
                                                       extracted} = flatten_type(r_avro_record_field(field, :type))
                                                    {[r_avro_record_field(field, type: newType) |
                                                          fieldsAcc],
                                                       extracted ++ extractedAcc}
                                               end,
                                                 {[], []}, r_avro_record_type(record, :fields))
    {r_avro_record_type(record, fields: newFields), extractedTypes}
  end

  defp flatten(r_avro_array_type() = array) do
    childType = :avro_array.get_items_type(array)
    {newChildType, extracted} = flatten_type(childType)
    {:avro_array.type(newChildType), extracted}
  end

  defp flatten(r_avro_map_type(type: childType) = map) do
    {newChildType, extracted} = flatten_type(childType)
    {r_avro_map_type(map, type: newChildType), extracted}
  end

  defp flatten(r_avro_union_type() = union) do
    childrenTypes = :avro_union.get_types(union)
    {newChildren,
       extractedTypes} = :lists.foldr(fn childType,
                                           {flattenAcc, extractedAcc} ->
                                           {childType1,
                                              extracted} = flatten_type(childType)
                                           {[childType1 | flattenAcc],
                                              extracted ++ extractedAcc}
                                      end,
                                        {[], []}, childrenTypes)
    {:avro_union.type(newChildren), extractedTypes}
  end

  defp name_string(name) when is_binary(name) do
    :erlang.binary_to_list(name)
  end

  defp name_string(name) when is_atom(name) do
    :erlang.atom_to_list(name)
  end

  defp name_string(name) when is_list(name) do
    name
  end

  def is_valid_name([]) do
    false
  end

  def is_valid_name([h | t]) do
    is_valid_name_head(h) and :lists.all(fn i ->
                                              is_valid_name_char(i)
                                         end,
                                           t)
  end

  defp is_valid_name_head(s) do
    s >= ?A and s <= ?Z or s >= ?a and s <= ?z or s === ?_
  end

  defp is_valid_name_char(s) do
    is_valid_name_head(s) or s >= ?0 and s <= ?9
  end

  def tokens_ex([], _Delimiter) do
    ['']
  end

  def tokens_ex([delimiter | rest], delimiter) do
    [[] | tokens_ex(rest, delimiter)]
  end

  def tokens_ex([c | rest], delimiter) do
    [token | tail] = tokens_ex(rest, delimiter)
    [[c | token] | tail]
  end

  defp do_validate(type, validateFun) do
    _ = do_validate(type, validateFun, _Hist = [])
    :ok
  end

  defp do_validate(type, validateFun, hist) do
    newHist = validateFun.(type, hist)
    :lists.foldl(fn sT, histIn ->
                      do_validate(sT, validateFun, histIn)
                 end,
                   newHist, sub_types(type))
  end

  defp validate_redef(t, hist) do
    names = names(t)
    :lists.foreach(fn name ->
                        case (for {n, :def} <- hist, n === name do
                                n
                              end) do
                          [] ->
                            :ok
                          _ ->
                            :erlang.throw({:type_redefined, name})
                        end
                   end,
                     names)
  end

  defp names(r_avro_enum_type(fullname: fN, aliases: aliases)) do
    [fN | aliases]
  end

  defp names(r_avro_fixed_type(fullname: fN, aliases: aliases)) do
    [fN | aliases]
  end

  defp names(r_avro_record_type(fullname: fN, aliases: aliases)) do
    [fN | aliases]
  end

  defp names(_) do
    []
  end

  defp make_jsone_input([]) do
    []
  end

  defp make_jsone_input([h | t]) when is_tuple(h) do
    {k, v} = h
    key = ensure_binary(k)
    val = (case (v) do
             n when is_number(n) ->
               n
             b when is_boolean(b) ->
               b
             b when is_binary(b) ->
               b
             a when is_atom(a) ->
               :erlang.atom_to_binary(a, :utf8)
             {l} when is_list(l) ->
               make_jsone_input(l)
             l when is_list(l) ->
               case (:io_lib.printable_unicode_list(l)) do
                 true ->
                   :unicode.characters_to_binary(l, :utf8, :utf8)
                 false ->
                   make_jsone_input(l)
               end
           end)
    [{key, val} | make_jsone_input(t)]
  end

  defp make_jsone_input([a | l]) when is_atom(a) do
    [:erlang.atom_to_binary(a, :utf8) | make_jsone_input(l)]
  end

  defp make_jsone_input([h | t]) do
    [h | make_jsone_input(t)]
  end

end