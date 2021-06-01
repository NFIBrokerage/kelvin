use Mix.Config

config :kelvin, ExtremeClient,
  db_type: :node,
  host: System.get_env("EVENTSTORE_HOST") || "localhost",
  port: 1113,
  username: "admin",
  password: "changeit",
  reconnect_delay: 2_000,
  max_attempts: :infinity
