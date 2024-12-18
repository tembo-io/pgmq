defmodule PgmqAdmin.Repo.Migrations.CreatePgmqExtension do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE")
  end
end
