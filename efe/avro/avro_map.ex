defmodule :m_avro_map do
  use Bitwise
  def type(type) do
    type(type, [])
  end

  def type(type, customProps) do
    r_avro_map_type(type: :avro_util.canonicalize_type_or_name(type),
        custom: :avro_util.canonicalize_custom_props(customProps))
  end

  def resolve_fullname(map, ns) do
    f = fn t ->
             :avro.resolve_fullname(t, ns)
        end
    update_items_type(map, f)
  end

  def get_items_type(r_avro_map_type(type: subType)) do
    subType
  end

  def update_items_type(r_avro_map_type(type: iT) = t, f) do
    r_avro_map_type(t, type: f.(iT))
  end

  def to_term(map) do
    for {k, v} <- to_list(map) do
      {k, :avro.to_term(v)}
    end
  end

end