defmodule Avrocado.OcfTest do
  use ExUnit.Case

  require Record
  Record.defrecord(:header, [:magic, :meta, :sync])

  defp test_data(file_name), do: "test/fixtures/" <> file_name

  test "read" do
    interop_ocf_file = test_data("interop.ocf")
    {header, _schema, _objects} = :avro_ocf.decode_file(interop_ocf_file)

    stream = File.stream!(interop_ocf_file, [], 3)
    cached_stream = %Avrocado.CachedStream{stream: stream}
    {magic, %Avrocado.CachedStream{cache: cache}} = Avrocado.CachedStream.read(cached_stream, 4)
    assert magic == header(header, :magic)
    assert cache == <<4, 22>>
  end

  test "interop" do
    interop_ocf_file = test_data("interop.ocf")
    {_header1, _schema1, objects1} = :avro_ocf.decode_file(interop_ocf_file)
    objects = Avrocado.Ocf.decode_file(interop_ocf_file) |> Enum.to_list() |> hd()

    assert objects == objects1
  end

  test "decode deflate file" do
    interop_ocf_file = test_data("interop_deflate.ocf")
    objects = Avrocado.Ocf.decode_file(interop_ocf_file) |> Enum.to_list() |> hd()
    assert "hey" == :proplists.get_value("stringField", hd(objects))
  end

  test "decode no codec file" do
    interop_ocf_file = test_data("interop_no_codec.ocf")
    [objects] = Avrocado.Ocf.decode_file(interop_ocf_file) |> Enum.to_list()
    assert "hey" == :proplists.get_value("stringField", hd(objects))
  end

  # # test "decoder hook" do
  # #   fields = [
  # #     :avro_record.define_field("f1", :int, []),
  # #     :avro_record.define_field("f2", :null, [])
  # #   ]

  # #   type = :avro_record.type("rec", fields, [{:namespace, "my.ocf.test"}])
  # #   header = :avro_ocf.make_header(type)
  # #   encoder = :avro.make_simple_encoder(type, [])
  # #   object = [{"f1", 1}, {"f2", :null}]
  # #   objects = [encoder.(object)]
  # #   bin = IO.iodata_to_binary(:avro_ocf.make_ocf(header, objects))
  # #   cached_stream = {[bin], []}

  # #   hook = fn typ, _, data, decode_fun ->
  # #     case :avro.get_type_name(typ) do
  # #       "null" -> {"modifiedNull", data}
  # #       _ -> decode_fun.(data)
  # #     end
  # #   end

  # #   options = :avro.make_decoder_options([{:hook, hook}])
  # #   {_, _, objs} = Avrocado.Ocf.decode_binary(cached_stream, options)
  # #   assert [[{"f1", 1} == {"f2", "modifiedNull"}]] == objs
  # # end

  test "decode one user" do
    interop_ocf_file = test_data("one_user1.ocf")
    objects = Avrocado.Ocf.decode_file(interop_ocf_file) |> Enum.to_list()

    assert objects == [
             [
               [
                 {"vimond_user_id", "1110101010"},
                 {"email", "test-1-1@example.test"},
                 {"contact_id", 11_111_111_111},
                 {"csn", "12345566"},
                 {"customer_id", "aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa"},
                 {"date_of_birth", 123_435_687},
                 {"postcode", "12345"},
                 {"forename", "test"},
                 {"surname", "test"},
                 {"country_code", "SWE"},
                 {"country_name", "Sweden"}
               ]
             ]
           ]
  end

  test "decode multiple users" do
    interop_ocf_file = test_data("multiple_users.ocf")
    objects = Avrocado.Ocf.decode_file(interop_ocf_file) |> Enum.to_list()

    assert objects == [
             [
               [
                 {"vimond_user_id", "1110101010"},
                 {"email", "test-1-1@example.test"},
                 {"contact_id", 11_111_111_111},
                 {"csn", "12345566"},
                 {"customer_id", "aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa"},
                 {"date_of_birth", 123_435_687},
                 {"postcode", "12345"},
                 {"forename", "test"},
                 {"surname", "test"},
                 {"country_code", "SWE"},
                 {"country_name", "Sweden"}
               ],
               [
                 {"vimond_user_id", "1210101010"},
                 {"email", "test-1-2@example.test"},
                 {"contact_id", 11_111_111_111},
                 {"csn", "12345566"},
                 {"customer_id", "aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa"},
                 {"date_of_birth", 123_435_687},
                 {"postcode", "12345"},
                 {"forename", "test"},
                 {"surname", "test"},
                 {"country_code", "SWE"},
                 {"country_name", "Sweden"}
               ],
               [
                 {"vimond_user_id", "1310101010"},
                 {"email", "test-1-3@example.test"},
                 {"contact_id", 11_111_111_111},
                 {"csn", "12345566"},
                 {"customer_id", "aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa"},
                 {"date_of_birth", 123_435_687},
                 {"postcode", "12345"},
                 {"forename", "test"},
                 {"surname", "test"},
                 {"country_code", "SWE"},
                 {"country_name", "Sweden"}
               ]
             ]
           ]
  end

  test "decode big orders" do
    interop_ocf_file = test_data("2021-03-22-orders.avro/run-1616369991301-part-r-00001")

    count =
      Avrocado.Ocf.decode_file(interop_ocf_file)
      |> Enum.reduce(0, fn objects, count ->
        count + Enum.count(objects)
      end)

    assert count == 161_819
  end

  @tag :skip
  test "decode big orders with erlavro" do
    interop_ocf_file = test_data("2021-03-22-orders.avro/run-1616369991301-part-r-00001")
    {_header, _schema, objects} = :avro_ocf.decode_file(interop_ocf_file)
    assert Enum.count(objects) == 161_819
  end
end
