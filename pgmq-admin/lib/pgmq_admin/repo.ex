defmodule PgmqAdmin.Repo do
  use Ecto.Repo,
    otp_app: :pgmq_admin,
    adapter: Ecto.Adapters.Postgres
end
