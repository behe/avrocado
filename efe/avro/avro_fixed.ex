defmodule :m_avro_fixed do
  use Bitwise
  require :r_avro_value

  def type(name, size) do
    type(name, size, [])
  end

  def resolve_fullname(
        r_avro_fixed_type(fullname: fullName, aliases: aliases) = t,
        ns
      ) do
    newFullname = :avro.build_type_fullname(fullName, ns)

    newAliases =
      :avro_util.canonicalize_aliases(
        aliases,
        ns
      )

    r_avro_fixed_type(t, fullname: newFullname, aliases: newAliases)
  end

  def get_size(r_avro_fixed_type(size: size)) do
    size
  end

  defp integer_to_fixed(size, integer) do
    bin = :binary.encode_unsigned(integer)
    true = size >= :erlang.size(bin)
    padSize = (size - :erlang.size(bin)) * 8
    <<0::size(padSize), bin::binary>>
  end
end
