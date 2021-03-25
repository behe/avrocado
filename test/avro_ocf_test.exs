defmodule Avro.OcfTest do
  use ExUnit.Case

  require Record
  Record.defrecord(:header, [:magic, :meta, :sync])

  defp test_data(file_name), do: "test/fixtures/" <> file_name

  test "interop test" do
    interop_ocf_file = test_data("interop.ocf")
    {header, schema, objects} = Avro.Ocf.decode_file(interop_ocf_file)
    # lkup = :avro.make_lkup_fun(schema)
    # my_file = test_data("interop.ocf.test")
    # {:ok, file_descriptor} = :file.open(my_file, [:write]) |> IO.inspect()

    {header1, schema1, objects1} = :avro_ocf.decode_file(interop_ocf_file)
    assert header(header, :magic) == header(header1, :magic)
    assert header(header, :meta) == header(header1, :meta)
    assert header(header, :sync) == header(header1, :sync)
    assert schema == schema1
    assert objects == objects1
  end
end
