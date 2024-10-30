defmodule PgmqAdmin.Queues do
  defmodule Queue do
    @type t() :: %__MODULE__{
            name: String.t(),
            is_unlogged: boolean(),
            is_partitioned: boolean(),
            created_at: DateTime.t(),
            metrics: term() | nil
          }
    defstruct [:name, :is_unlogged, :is_partitioned, :created_at, metrics: %{}]
  end

  defmodule Message do
    @type t() :: %__MODULE__{
            id: pos_integer(),
            read_count: non_neg_integer(),
            enqueued_at: DateTime.t(),
            vt: DateTime.t(),
            body: String.t()
          }

    defstruct [:id, :read_count, :enqueued_at, :vt, :body]
  end

  def get_queue!(name) do
    metrics =
      PgmqAdmin.Repo
      |> Pgmq.get_metrics(name)
      |> Map.put(:archive_length, get_archive_length(name))

    result =
      PgmqAdmin.Repo
      |> Pgmq.list_queues()
      |> Enum.find(fn q -> q.queue_name == name end)

    %Queue{
      name: result.queue_name,
      is_unlogged: result.is_unlogged,
      is_partitioned: result.is_partitioned,
      created_at: result.created_at,
      metrics: metrics
    }
  end

  # We need to list queues once and get all metrics once, then merge the data
  def list_queues() do
    metrics =
      PgmqAdmin.Repo
      |> Pgmq.get_metrics_all()
      |> Task.async_stream(&Map.put(&1, :archive_length, get_archive_length(&1.queue_name)))
      |> Map.new(&Map.pop!(elem(&1, 1), :queue_name))

    PgmqAdmin.Repo
    |> Pgmq.list_queues()
    |> Enum.map(fn q ->
      %Queue{
        name: q.queue_name,
        is_unlogged: q.is_unlogged,
        is_partitioned: q.is_partitioned,
        created_at: q.created_at,
        metrics: metrics[q.queue_name]
      }
    end)
  end

  defp get_archive_length(queue) do
    %Postgrex.Result{rows: [[size]]} =
      PgmqAdmin.Repo.query!("SELECT COUNT(1) FROM pgmq.\"a_#{queue}\"")

    size
  end

  def delete_queue(name) do
    PgmqAdmin.Repo
    |> Pgmq.drop_queue(name)
  end

  def create_queue(queue_params) do
    opts = [
      unlogged: queue_params[:is_unlogged],
      partitioned: queue_params[:is_partitioned],
      partition_interval: queue_params[:partition_interval],
      retention_interval: queue_params[:retention_interval]
    ]

    :ok = Pgmq.create_queue(PgmqAdmin.Repo, queue_params.name, opts)
    {:ok, get_queue!(queue_params.name)}
  end

  def peek_queue_messages(%Queue{name: name}, count) do
    %Postgrex.Result{rows: rows} =
      PgmqAdmin.Repo.query!(
        "SELECT * FROM pgmq.\"q_#{name}\" ORDER BY read_ct DESC, msg_id ASC LIMIT #{count}"
      )

    rows
    |> Enum.map(fn [msg_id, read_count, enqueued_at, vt, message] ->
      %Message{
        id: msg_id,
        read_count: read_count,
        enqueued_at: enqueued_at,
        vt: vt,
        body: message
      }
    end)
  end

  def peek_archive_messages(%Queue{name: name}, count) do
    %Postgrex.Result{rows: rows} =
      PgmqAdmin.Repo.query!(
        "SELECT msg_id, read_ct, message FROM pgmq.\"a_#{name}\" ORDER BY msg_id DESC LIMIT #{count}"
      )

    rows
    |> Enum.map(fn [msg_id, read_count, message] ->
      %Message{id: msg_id, read_count: read_count, body: message}
    end)
  end

  def unarchive_message(queue, msg_id) do
    import Ecto.Query

    archive_table = "a_#{queue}"
    queue_table = "q_#{queue}"

    archived_message_query =
      from(m in archive_table)
      |> where([m], m.msg_id == ^msg_id)
      |> put_query_prefix("pgmq")

    rows_to_insert_query =
      archived_message_query
      |> select([m], %{
        read_ct: m.read_ct,
        message: m.message,
        vt: fragment("CURRENT_TIMESTAMP")
      })

    {:ok, _} =
      PgmqAdmin.Repo.transaction(fn ->
        {1, nil} = PgmqAdmin.Repo.insert_all(queue_table, rows_to_insert_query, prefix: "pgmq")
        {1, nil} = PgmqAdmin.Repo.delete_all(archived_message_query)
      end)

    :ok
  end

  def archive_message(queue, msg_id) do
    Pgmq.archive_messages(PgmqAdmin.Repo, queue, [msg_id])
  end

  def delete_message(queue, msg_id) do
    Pgmq.delete_messages(PgmqAdmin.Repo, queue, [msg_id])
  end

  def delete_message_from_archive(queue, msg_id) do
    %Postgrex.Result{num_rows: 1} =
      PgmqAdmin.Repo.query!("DELETE from pgmq.\"a_#{queue}\" WHERE msg_id = $1", [msg_id])

    :ok
  end

  def send_messages(queue, messages) do
    Enum.each(messages, &Pgmq.send_message(PgmqAdmin.Repo, queue, &1))
  end
end
