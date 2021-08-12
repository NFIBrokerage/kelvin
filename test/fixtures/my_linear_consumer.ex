defmodule MyInOrderConsumer do
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
    sleep_time = Keyword.fetch!(opts, :sleep_time)

    {:consumer, {test_proc, sleep_time}, subscribe_to: subscribe_to}
  end

  @impl GenStage
  def handle_events(events, _from, {test_proc, sleep_time}) do
    if sleep_time |> is_integer() do
      Process.sleep(sleep_time)
    end

    events = Enum.map(events, fn {_producer, event} -> event end)
    send(test_proc, {:events, events})

    {:noreply, [], {test_proc, sleep_time}}
  end
end
