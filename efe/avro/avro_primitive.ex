defmodule :m_avro_primitive do
  use Bitwise
  def null() do
    from_cast(cast(null_type(), :null))
  end

  def boolean(value) do
    from_cast(cast(boolean_type(), value))
  end

  def int(value) do
    from_cast(cast(int_type(), value))
  end

  def long(value) do
    from_cast(cast(long_type(), value))
  end

  def float(value) do
    from_cast(cast(float_type(), value))
  end

  def double(value) do
    from_cast(cast(double_type(), value))
  end

  def bytes(value) do
    from_cast(cast(bytes_type(), value))
  end

  def string(value) do
    from_cast(cast(string_type(), value))
  end

  defp from_cast({:ok, value}) do
    value
  end

  defp from_cast({:error, err}) do
    :erlang.error(err)
  end

end