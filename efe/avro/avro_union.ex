defmodule :m_avro_union do
  use Bitwise
  def resolve_fullname(union, ns) do
    f = fn t ->
             :avro.resolve_fullname(t, ns)
        end
    update_member_types(union, f)
  end

  def update_member_types(t0, f) do
    types = get_types(t0)
    updatedTypes = :lists.map(f, types)
    type(updatedTypes)
  end

  def get_types(r_avro_union_type(id2type: indexedTypes)) do
    {_Ids,
       types} = :lists.unzip(:gb_trees.to_list(indexedTypes))
    types
  end

  defp try_encode_union_loop(unionType, [], value, _Index, _EncodeFun) do
    :erlang.error({:failed_to_encode_union, unionType,
                     value})
  end

  defp try_encode_union_loop(unionType, [memberT | rest], value, index,
            encodeFun) do
    try do
      encodeFun.(memberT, value, index)
    catch
      _C, _E ->
        try_encode_union_loop(unionType, rest, value, index + 1,
                                encodeFun)
    end
  end

  defp cast_over_types([], _Value) do
    {:error, :type_mismatch}
  end

  defp cast_over_types([type | rest], value) do
    case (:avro.cast(type, value)) do
      {:error, _} ->
        cast_over_types(rest, value)
      r ->
        r
    end
  end

  defp assert_no_duplicated_names([], _UniqueNames) do
    :ok
  end

  defp assert_no_duplicated_names([{name, _Index} | rest], uniqueNames) do
    case (:lists.member(name, uniqueNames)) do
      true ->
        :erlang.error({"duplicated union member", name})
      false ->
        assert_no_duplicated_names(rest, [name | uniqueNames])
    end
  end

end