inputs = %{
  interop: "test/fixtures/interop.ocf",
  # one_user: "test/fixtures/one_user1.ocf",
  # multiple_users: "test/fixtures/multiple_users.ocf",
  large_orders: "test/fixtures/2021-03-22-orders.avro/run-1616369991301-part-r-00001",
  empty_orders: "test/fixtures/2021-03-22-orders.avro/run-1616369991301-part-r-00100",
}

Benchee.run(%{
  "Avrocado"    => fn file ->
      Avrocado.Ocf.decode_file(file)
      |> Enum.reduce(0, fn chunk , acc ->
        acc + Enum.count(chunk)
      end)
 end,
  "erlavro" => fn file ->
    {_header, _schema, objects} = :avro_ocf.decode_file(file)
    objects
    |> Enum.reduce(0, fn chunk , acc ->
      acc + Enum.count(chunk)
    end)
   end
},  inputs: inputs
)
