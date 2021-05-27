defmodule MyLinearSupervisor do
  @moduledoc """
  A supervisor module which spawns a supervision tree for a linear subscription
  pipeline.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl Supervisor
  def init(opts) do
    producer_name = Keyword.fetch!(opts, :producer_name)

    producer_opts = [
      name: producer_name,
      connection: ExtremeClient,
      stream_name: Keyword.fetch!(opts, :stream_name),
      restore_stream_position!: Keyword.fetch!(opts, :restore_stream_position!)
    ]

    consumer_opts = [
      test_proc: Keyword.fetch!(opts, :test_proc),
      subscribe_to: [{producer_name, max_demand: 1}]
    ]

    children = [
      {Kelvin.LinearSubscription, producer_opts},
      {MyLinearConsumer, consumer_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
