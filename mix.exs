defmodule Kelvin.MixProject do
  use Mix.Project

  @source_url "https://github.com/NFIBrokerage/kelvin"
  @version_file Path.join(__DIR__, ".version")
  @external_resource @version_file
  @version (case Regex.run(~r/^v([\d\.\w-]+)/, File.read!(@version_file),
                   capture: :all_but_first
                 ) do
              [version] -> version
              nil -> "0.0.0"
            end)

  def project do
    [
      app: :kelvin,
      version: @version,
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      preferred_cli_env: [
        credo: :test,
        coveralls: :test,
        "coveralls.html": :test,
        bless: :test,
        test: :test
      ],
      test_coverage: [tool: ExCoveralls],
      package: package(),
      description: description(),
      source_url: @source_url,
      name: "Kelvin",
      docs: docs()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/fixtures"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    []
  end

  defp deps do
    [
      {:gen_stage, "~> 1.0"},
      {:extreme, "~> 1.0 and >= 1.0.5"},
      # docs
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      # test
      {:bless, "~> 1.0", only: :test},
      {:credo, "~> 1.0", only: :test},
      {:excoveralls, "~> 0.7", only: :test}
    ]
  end

  defp package do
    [
      name: "kelvin",
      files: ~w(lib .formatter.exs mix.exs README.md .version),
      licenses: [],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => @source_url <> "/blobs/main/CHANGELOG.md"
      }
    ]
  end

  defp description do
    "GenStage/Broadway producers for Extreme"
  end

  defp docs do
    [
      # do you reference other projects in your documentation? if so, add
      # them to the :deps key here. for an example, see
      # https://github.com/NFIBrokerage/projection/blob/5f406872d00156e2b94cfa9fae8e92a1aa4c177b/mix.exs#L88-L90
      deps: [],
      extras: [
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: Path.wildcard("guides/*.md")
      ]
    ]
  end
end
