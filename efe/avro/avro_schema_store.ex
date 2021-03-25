defmodule :m_avro_schema_store do
  use Bitwise
  def new() do
    new([])
  end

  def new(options) do
    case (:proplists.get_bool(:dict, options)) do
      true ->
        {:dict, :dict.new()}
      false ->
        new_ets(options)
    end
  end

  def new(options, files) do
    store = new(options)
    import_files(files, store)
  end

  def is_store({:dict, _}) do
    true
  end

  def is_store(t) do
    is_integer(t) or is_atom(t) or is_reference(t)
  end

  def to_lookup_fun(store) do
    fn name ->
         {:ok, type} = :avro_schema_store.lookup_type(name,
                                                        store)
         type
    end
  end

  def import_files(files, store) do
    :lists.foldl(fn file, s ->
                      import_file(file, s)
                 end,
                   store, files)
  end

  def import_file(file, store) do
    case (:file.read_file(file)) do
      {:ok, json} ->
        name = parse_basename(file)
        import_schema_json(name, json, store)
      {:error, reason} ->
        :erlang.error({:failed_to_read_schema_file, file,
                         reason})
    end
  end

  def import_schema_json(json, store) do
    import_schema_json(:undefined, json, store)
  end

  def close({:dict, _}) do
    :ok
  end

  def close(store) do
    :ets.delete(store)
    :ok
  end

  def ensure_store(store) do
    true = is_store(store)
    store
  end

  def add_type(type, store) do
    add_type(:undefined, type, store)
  end

  defp to_list({:dict, dict}) do
    :dict.to_list(dict)
  end

  defp to_list(store) do
    :ets.tab2list(store)
  end

  defp new_ets(options) do
    access = :avro_util.get_opt(:access, options, :public)
    {name, etsOpts} = (case (:avro_util.get_opt(:name,
                                                  options, :undefined)) do
                         :undefined ->
                           {:avro_schema_store, []}
                         name1 ->
                           {name1, [:named_table]}
                       end)
    :ets.new(name,
               [access, {:read_concurrency, true} | etsOpts])
  end

  defp parse_basename(fileName) do
    baseName0 = :filename.basename(fileName)
    baseName1 = :filename.basename(fileName, '.avsc')
    baseName2 = :filename.basename(fileName, '.json')
    :lists.foldl(fn n, shortest ->
                      bN = :avro_util.ensure_binary(n)
                      case (:erlang.size(bN) < :erlang.size(shortest)) do
                        true ->
                          bN
                        false ->
                          shortest
                      end
                 end,
                   :avro_util.ensure_binary(baseName0),
                   [baseName1, baseName2])
  end

  def import_schema_json(assignedName, json, store) do
    schema = :avro.decode_schema(json)
    add_type(assignedName, schema, store)
  end

  defp do_add_type(type, store) do
    fullName = :avro.get_type_fullname(type)
    aliases = :avro.get_aliases(type)
    do_add_type_by_names([fullName | aliases], type, store)
  end

  defp do_add_type_by_names([], _Type, store) do
    store
  end

  defp do_add_type_by_names([name | rest], type, store) do
    case (get_type_from_store(name, store)) do
      {:ok, ^type} ->
        store
      {:ok, otherType} ->
        :erlang.error({:name_clash, name, type, otherType})
      false ->
        store1 = put_type_to_store(name, type, store)
        do_add_type_by_names(rest, type, store1)
    end
  end

  defp put_type_to_store(name, type, {:dict, dict}) do
    newDict = :dict.store(name, type, dict)
    {:dict, newDict}
  end

  defp put_type_to_store(name, type, store) do
    true = :ets.insert(store, {name, type})
    store
  end

  defp get_type_from_store(name, {:dict, dict}) do
    case (:dict.find(name, dict)) do
      :error ->
        false
      {:ok, type} ->
        {:ok, type}
    end
  end

  defp get_type_from_store(name, store) do
    case (:ets.lookup(store, name)) do
      [] ->
        false
      [{^name, type}] ->
        {:ok, type}
    end
  end

end