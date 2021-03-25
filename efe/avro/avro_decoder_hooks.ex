defmodule :m_avro_decoder_hooks do
  use Bitwise
  require :r_avro_union_type

  def tag_unions() do
    &tag_unions/4
  end

  def print_debug_trace(printFun, maxHistoryLength) do
    :ok = erase_hist()

    fn t, sub, data, decodeFun ->
      print_trace_on_failure(t, sub, data, decodeFun, printFun, maxHistoryLength)
    end
  end

  def pretty_print_hist() do
    _ = :erlang.erase(:"$avro_decoder_pp_indentation")

    fn t, subInfo, data, decodeFun ->
      name = :avro.get_type_fullname(t)

      indentation =
        case :erlang.get(:"$avro_decoder_pp_indentation") do
          :undefined ->
            0

          indentati ->
            indentati
        end

      indentationStr = :lists.duplicate(indentation * 2, ?\s)

      toPrint = [
        indentationStr,
        name,
        case subInfo do
          '' ->
            ': '

          i when is_integer(i) ->
            [?., :erlang.integer_to_list(i), '\n']

          b when is_binary(b) ->
            [?., b, '\n']

          _ ->
            '\n'
        end
      ]

      :io.put_chars(:user, toPrint)
      _ = :erlang.put(:"$avro_decoder_pp_indentation", indentation + 1)
      decodeResult = decodeFun.(data)
      resultToPrint = get_pretty_print_result(decodeResult)
      _ = pretty_print_result(subInfo, resultToPrint, indentationStr)
      _ = :erlang.put(:"$avro_decoder_pp_indentation", indentation)
      decodeResult
    end
  end

  defp tag_unions(r_avro_union_type() = t, subInfo, decodeIn, decodeFun) do
    result = decodeFun.(decodeIn)
    name = get_union_member_name(t, subInfo)

    case result do
      {value, tail} when is_binary(tail) ->
        {maybe_tag(name, value), tail}

      value ->
        maybe_tag(name, value)
    end
  end

  defp tag_unions(_T, _SubInfo, decodeIn, decodeFun) do
    decodeFun.(decodeIn)
  end

  defp get_union_member_name(type, id) when is_integer(id) do
    {:ok, childType} = :avro_union.lookup_type(id, type)

    case is_binary(childType) do
      true ->
        childType

      false ->
        :avro.get_type_fullname(childType)
    end
  end

  defp get_union_member_name(_Type, name) when is_binary(name) do
    name
  end

  defp decode_and_add_trace(sub, data, decodeFun) do
    result = decodeFun.(data)

    value =
      case result do
        {v, tail} when is_binary(tail) ->
          v

        _ ->
          result
      end

    case sub === [] or value === [] do
      true ->
        add_hist({:pop, value})

      false ->
        add_hist(:pop)
    end

    result
  end

  defp erase_hist() do
    _ = :erlang.erase(:"$avro_decoder_hist")
    :ok
  end

  defp get_hist() do
    case :erlang.get(:"$avro_decoder_hist") do
      :undefined ->
        []

      s ->
        s
    end
  end

  defp add_hist(newOp) do
    :erlang.put(:"$avro_decoder_hist", [newOp | get_hist()])
    :ok
  end

  defp print_trace(printFun, histCount) do
    hist = :lists.reverse(get_hist())
    {stack, history} = format_trace(hist, _Stack = [], _History = [], histCount)
    printFun.(['avro type stack:\n', stack, '\n', 'decode history:\n', history])
  end

  defp format_trace([], stack, hist, _HistCount) do
    {:io_lib.format('~p', [:lists.reverse(stack)]), :lists.reverse(hist)}
  end

  defp format_trace([{:push, name, sub} | rest], stack, hist, histCount) do
    padding = :lists.duplicate(length(stack) * 2, ?\s)

    line =
      bin([
        padding,
        name,
        case sub do
          [] ->
            ''

          :none ->
            ''

          i when is_integer(i) ->
            ['.', :erlang.integer_to_list(i)]

          s when is_binary(s) ->
            ['.', s]
        end,
        '\n'
      ])

    newHist = :lists.sublist([line | hist], histCount)
    format_trace(rest, [{name, sub} | stack], newHist, histCount)
  end

  defp format_trace([{:pop, v} | rest], stack, hist, histCount) do
    padding = :lists.duplicate(length(stack) * 2, ?\s)
    line = bin([padding, :io_lib.format('~100000p', [v]), '\n'])
    newHist = :lists.sublist([line | hist], histCount)
    format_trace(rest, tl(stack), newHist, histCount)
  end

  defp format_trace([:pop | rest], stack, hist, histCount) do
    format_trace(rest, tl(stack), hist, histCount)
  end

  defp bin(ioData) do
    :erlang.iolist_to_binary(ioData)
  end

  defp pretty_print_result(_Sub = [], result, _IndentationStr) do
    :io.put_chars(:user, [:io_lib.print(result)])
  end

  defp pretty_print_result(_Sub, _Result, _IndentationStr) do
    :ok
  end
end
