defmodule Kelvin.InOrderSubscriptionTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Extreme.Messages

  setup do
    stream_name = "Kelvin.Test-#{UUID.uuid4()}"
    producer_name = String.to_atom("Kelvin.InOrderProducer-#{UUID.uuid4()}")

    [stream_name: stream_name, producer_name: producer_name]
  end

  describe "given events have been written to a stream" do
    setup c do
      write_events(0..100, c.stream_name)
      :ok
    end

    test "a subscription reads all written events and new ones", c do
      opts = [
        producer_name: c.producer_name,
        stream_name: c.stream_name,
        restore_stream_position!: &restore_stream_position!/0,
        test_proc: self()
      ]

      start_supervised!({MyInOrderSupervisor, opts})

      for n <- 0..100 do
        assert_receive {:events, [event]}, 6_000
        assert event.event.data == to_string(n)
      end

      write_events(101..200, c.stream_name)

      for n <- 101..200 do
        assert_receive {:events, [event]}, 1_000
        assert event.event.data == to_string(n)
      end
    end

    test "a subscription catches up even if a tcp_closed occurs", c do
      opts = [
        producer_name: c.producer_name,
        stream_name: c.stream_name,
        restore_stream_position!: &restore_stream_position!/0,
        test_proc: self()
      ]

      start_supervised!({MyInOrderSupervisor, opts})

      for n <- 0..100 do
        assert_receive {:events, [event]}, 6_000
        assert event.event.data == to_string(n)
      end

      monitor_ref =
        ExtremeClient.Connection
        |> GenServer.whereis()
        |> Process.monitor()

      send(ExtremeClient.Connection, {:tcp_closed, ""})

      assert_receive {:DOWN, ^monitor_ref, _, _, _}

      # we're hardcoding the restore_stream_position! function so this will
      # restart from 0 instead of the current stream position as would be the
      # case in a real-life system
      for n <- 0..100 do
        assert_receive {:events, [event]}, 10_000
        assert event.event.data == to_string(n)
      end

      write_events(101..200, c.stream_name)

      for n <- 101..200 do
        assert_receive {:events, [event]}, 1_000
        assert event.event.data == to_string(n)
      end
    end
  end

  defp restore_stream_position!, do: -1

  defp write_events(range, stream) do
    range
    |> Enum.map(fn n ->
      Messages.NewEvent.new(
        event_id: Extreme.Tools.generate_uuid(),
        event_type: "kelvin_test_event",
        data_content_type: 1,
        metadata_content_type: 1,
        # valid JSON
        data: to_string(n),
        metadata: "{}"
      )
    end)
    |> ExtremeClient.append_events(stream)
  end
end
