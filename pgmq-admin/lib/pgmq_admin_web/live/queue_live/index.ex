defmodule PgmqAdminWeb.QueueLive.Index do
  use PgmqAdminWeb, :live_view

  alias PgmqAdmin.Queues
  alias PgmqAdmin.Queues.Queue

  @impl true
  def mount(_params, _session, socket) do
    queues = list_queues()
    aggregates = compute_aggregates(queues)

    socket =
      socket
      |> stream_configure(:queues, dom_id: fn q -> q.name end)
      |> stream(:queues, queues)
      |> assign(:aggregates, aggregates)

    {:ok, socket}
  end

  defp compute_aggregates(queues) do
    starting_agg = %{oldest_message: nil, queues: 0, pending_messages: 0, archived_messages: 0}

    Enum.reduce(queues, starting_agg, fn queue, acc ->
      %{
        acc
        | oldest_message:
            oldest(acc.oldest_message, {queue.name, queue.metrics.oldest_msg_age_sec}),
          queues: acc.queues + 1,
          pending_messages: acc.pending_messages + queue.metrics.queue_length,
          archived_messages: acc.archived_messages + queue.metrics.archive_length
      }
    end)
    |> Map.update!(:oldest_message, fn oldest ->
      case oldest do
        nil -> nil
        {queue, age} -> %{queue: queue, age: age}
      end
    end)
  end

  defp oldest(nil, {_q2, nil}), do: nil
  defp oldest(nil, {q2, a2}), do: {q2, a2}
  defp oldest({q1, a1}, {_q2, nil}), do: {q1, a1}

  defp oldest({q1, a1}, {q2, a2}) do
    if a1 >= a2 do
      {q1, a1}
    else
      {q2, a2}
    end
  end

  defp list_queues() do
    Queues.list_queues()
    |> Enum.sort(fn q1, q2 -> q1.metrics.total_messages >= q2.metrics.total_messages end)
  end

  @impl true
  def handle_params(params, _url, socket) do
    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :new, _params) do
    socket
    |> assign(:page_title, "New Queue")
    |> assign(:queue, %{})
  end

  defp apply_action(socket, :index, _params) do
    socket
    |> assign(:page_title, "Queues")
    |> assign(:queue, nil)
  end

  @impl true
  def handle_info({PgmqAdminWeb.QueueLive.FormComponent, {:saved, queue}}, socket) do
    {:noreply, stream_insert(socket, :queues, queue)}
  end

  @impl true
  def handle_event("delete", %{"id" => name}, socket) do
    :ok = Queues.delete_queue(name)
    {:noreply, stream_delete(socket, :queues, %Queue{name: name})}
  end
end
