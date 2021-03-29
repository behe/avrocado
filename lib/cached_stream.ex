defmodule Avrocado.CachedStream do
  defstruct cache: "", stream: []

  def read(%__MODULE__{cache: chunk, stream: stream}, size) when byte_size(chunk) >= size do
    <<value::binary-size(size), tail::binary>> = chunk
    {value, %__MODULE__{cache: tail, stream: stream}}
  end

  def read(%__MODULE__{cache: "", stream: stream}, size) do
    StreamSplit.take_and_drop(stream, 1)
    |> case do
      {[], []} ->
        {"", %__MODULE__{}}

      {[chunk], stream} ->
        read(%__MODULE__{cache: chunk, stream: stream}, size)
    end
  end

  def read(%__MODULE__{cache: chunk1, stream: stream}, size) do
    StreamSplit.take_and_drop(stream, 1)
    |> case do
      {[], []} ->
        {"", %__MODULE__{}}

      {[chunk2], stream} ->
        chunk = chunk1 <> chunk2
        read(%__MODULE__{cache: chunk, stream: stream}, size)
    end
  end
end
