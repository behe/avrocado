defmodule :m_avro_binary_encoder do
  use Bitwise
  def encode(sc, type, input) do
    lkup = :avro_util.ensure_lkup_fun(sc)
    do_encode(lkup, type, input)
  end

  defp block(0, []) do
    [0]
  end

  defp block(count, payload) when is_binary(payload) do
    header = :erlang.iolist_to_binary([long(- count),
                                           long(:erlang.size(payload))])
    [header, payload, 0]
  end

  defp block(count, payload) do
    block(count, :erlang.iolist_to_binary(payload))
  end

  defp null() do
    <<>>
  end

  defp bool(false) do
    <<0>>
  end

  defp bool(true) do
    <<1>>
  end

  def int(int) do
    zzInt = zigzag(:int, int)
    varint(zzInt)
  end

  def long(long) do
    zzLong = zigzag(:long, long)
    varint(zzLong)
  end

  defp float(float) when is_float(float) do
    <<float :: size(32) - little - float>>
  end

  defp double(double) when is_float(double) do
    <<double :: size(64) - little - float>>
  end

  defp bytes(data) when is_binary(data) do
    [long(byte_size(data)), data]
  end

  def string(atom) when is_atom(atom) do
    string(:erlang.atom_to_binary(atom, :utf8))
  end

  def string(string) when is_list(string) do
    string(:erlang.iolist_to_binary(string))
  end

  def string(string) when is_binary(string) do
    [long(:erlang.size(string)), string]
  end

  def zigzag(:int, int) do
    int <<< 1 ^^^ (int >>> 31)
  end

  def zigzag(:long, int) do
    int <<< 1 ^^^ (int >>> 63)
  end

  defp varint(i) when i <= 127 do
    [i]
  end

  defp varint(i) do
    [128 + i &&& 127 | varint(i >>> 7)]
  end

end