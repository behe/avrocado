defmodule Avrocado.MixProject do
  use Mix.Project

  def project do
    [
      app: :avrocado,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      preferred_cli_env: ["test.watch": :test]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.2"},
      {:erlavro, "~> 2.9"},
      {:stream_split, github: "tallakt/stream_split"},
      {:mix_test_watch, "~> 1.0", only: :test},
      {:benchee, "~> 1.0", only: :dev}
    ]
  end
end
