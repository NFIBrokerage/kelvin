defmodule Kelvin do
  @moduledoc """
  GenStage/Broadway producers for Extreme
  """

  @doc """
  Determines the stream position of a subscription event from Extreme

  This function can be used by a GenStage consumer to a linear subscription in
  order to persist stream position.
  """
  # coveralls-ignore-start
  def stream_position(%{link: %{event_number: n}}) when is_number(n), do: n
  def stream_position(%{event: %{event_number: n}}) when is_number(n), do: n

  # coveralls-ignore-stop
end
