defmodule :m_avro_enum do
  use Bitwise
  require :r_avro_enum_type

  def type(name, symbols) do
    type(name, symbols, [])
  end

  def resolve_fullname(
        r_avro_enum_type(fullname: fullname, aliases: aliases) = t,
        ns
      ) do
    newFullname = :avro.build_type_fullname(fullname, ns)

    newAliases =
      :avro_util.canonicalize_aliases(
        aliases,
        ns
      )

    r_avro_enum_type(t, fullname: newFullname, aliases: newAliases)
  end

  def get_index(type, symbol) do
    get_index(:avro_util.canonicalize_name(symbol), r_avro_enum_type(type, :symbols), 0)
  end

  defp is_valid_symbol(type, symbol) do
    :lists.member(symbol, r_avro_enum_type(type, :symbols))
  end

  defp get_index(symbol, [symbol | _Symbols], index) do
    index
  end

  defp get_index(symbol, [_ | symbols], index) do
    get_index(symbol, symbols, index + 1)
  end
end
