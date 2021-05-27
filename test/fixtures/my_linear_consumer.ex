defmodule MyLinearConsumer do
  @moduledoc """
  A fixture consumer of events from a linear subscription
  """

  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    test_proc = Keyword.fetch!(opts, :test_proc)
    subscribe_to = Keyword.fetch!(opts, :subscribe_to)

    {:consumer, test_proc, subscribe_to: subscribe_to}
  end

  @impl GenStage
  def handle_events(events, _from, test_proc) do
    send(test_proc, {:events, events})

    {:noreply, [], test_proc}
  end
end