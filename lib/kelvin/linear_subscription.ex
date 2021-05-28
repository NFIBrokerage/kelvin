defmodule Kelvin.LinearSubscription do
  @moduledoc """
  A subscription producer which processes events in order
  """

  use GenStage
  require Logger

  defstruct [:config, :subscription, :buffer, demand: 0]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl GenStage
  def init(opts) do
    state = %__MODULE__{config: Map.new(opts)}

    identifier =
      "#{inspect(__MODULE__)} (#{inspect(state.config[:name] || self())})"

    if do_function(state.config.subscribe_on_init?) do
      Logger.info("#{identifier} subscribing to '#{state.config.stream_name}'")

      GenStage.async_info(self(), :subscribe)
    else
      # coveralls-ignore-start
      Logger.info(
        "#{identifier} did not subscribe to '#{state.config.stream_name}'"
      )

      # coveralls-ignore-stop
    end

    {:producer, %__MODULE__{config: Map.new(opts)}}
  end

  @impl GenStage
  def handle_info(:subscribe, state) do
    case subscribe(state) do
      {:ok, sub} ->
        Process.link(sub)
        {:noreply, [], put_in(state.subscription, sub)}

      # coveralls-ignore-start
      {:error, reason} ->
        {:stop, reason, state}

        # coveralls-ignore-stop
    end
  end

  def handle_info(_info, state), do: {:noreply, [], state}

  @impl GenStage
  def handle_call({:on_event, event}, from, state) do
    case state.demand do
      0 ->
        {:noreply, [], put_in(state.buffer, {event, from})}

      demand ->
        {:reply, :ok, [event], put_in(state.demand, demand - 1)}
    end
  end

  @impl GenStage
  def handle_demand(demand, state) do
    case state.buffer do
      {event, from} ->
        GenStage.reply(from, :ok)

        {:noreply, [event],
         %__MODULE__{state | demand: demand - 1, buffer: nil}}

      _ ->
        {:noreply, [], put_in(state.demand, demand)}
    end
  end

  defp subscribe(state) do
    Extreme.RequestManager.read_and_stay_subscribed(
      state.config.connection,
      self(),
      {state.config.stream_name,
       do_function(state.config.restore_stream_position!) + 1, 256, true, false,
       :infinity}
    )
  end

  defp do_function(func) when is_function(func, 0), do: func.()

  defp do_function({m, f, a}) when is_atom(m) and is_atom(f) and is_list(a) do
    apply(m, f, a)
  end
end
