defmodule PgmqAdmin do
  @moduledoc """
  PgmqAdmin keeps the contexts that define your domain
  and business logic.

  Contexts are also responsible for managing your data, regardless
  if it comes from the database, an external API or others.
  """

  def insert_queue_a_messages() do
    for i <- 1..100_000 do
      json = Jason.encode!(%{"val" => i})
      Pgmq.send_message(PgmqAdmin.Repo, "queue_a", json)
    end
  end

  # def start_pipeline() do
  #  Supervisor.start_link([PgmqAdmin.QueueAPipeline], strategy: :one_for_one)
  # end
end
