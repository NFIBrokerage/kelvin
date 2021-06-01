defmodule ExtremeClient do
  @moduledoc """
  An Extreme client module for connecting to a local eventstore
  """

  use Extreme, otp_app: :kelvin

  alias Extreme.Messages

  def append_events(events, stream_id) do
    Messages.WriteEvents.new(
      event_stream_id: stream_id,
      expected_version: -2,
      events: events,
      require_master: false
    )
    |> execute()
  end
end
