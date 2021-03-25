defmodule :m_avro_array do
  use Bitwise
  require :r_avro_array_type

  def type(type) do
    type(type, [])
  end

  def type(type, customProps) do
    r_avro_array_type(
      type: :avro_util.canonicalize_type_or_name(type),
      custom: :avro_util.canonicalize_custom_props(customProps)
    )
  end

  def resolve_fullname(array, ns) do
    update_items_type(
      array,
      fn t ->
        :avro.resolve_fullname(t, ns)
      end
    )
  end

  def update_items_type(r_avro_array_type(type: sT) = t, f) do
    r_avro_array_type(t, type: f.(sT))
  end

  def new(type) do
    new(type, [])
  end

  def encode(type, value, encodeFun) do
    itemsType = :avro_array.get_items_type(type)

    :lists.map(
      fn element ->
        encodeFun.(itemsType, element)
      end,
      value
    )
  end

  defp cast_items(_TargetType, [], acc) do
    {:ok, :lists.reverse(acc)}
  end

  defp cast_items(targetType, [item | h], acc) do
    case :avro.cast(targetType, item) do
      {:ok, value} ->
        cast_items(targetType, h, [value | acc])

      err ->
        err
    end
  end
end
