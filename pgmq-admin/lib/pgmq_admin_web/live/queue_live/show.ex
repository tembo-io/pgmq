defmodule PgmqAdminWeb.QueueLive.Show do
  use PgmqAdminWeb, :live_view

  alias PgmqAdmin.Queues

  @impl true
  def mount(_params, _session, socket) do
    {:ok, socket}
  end

  @impl true
  def handle_params(%{"id" => id}, _, socket) do
    queue = Queues.get_queue!(id)
    queue_messages = Queues.peek_queue_messages(queue, 10)

    {:noreply,
     socket
     |> assign(:page_title, page_title(socket.assigns.live_action))
     |> assign(:queue, queue)
     |> assign(:queue_messages, queue_messages)
     |> assign(:archived_messages, Queues.peek_archive_messages(queue, 10))}
  end

  @impl true
  def handle_event("delete-queue", _params, socket) do
    :ok = Queues.delete_queue(socket.assigns.queue.name)

    socket =
      socket
      |> put_flash(:info, "Queue deleted successfully")
      |> push_navigate(to: "/queues")

    {:noreply, socket}
  end

  @impl true
  def handle_event("purge", _params, socket) do
    PgmqAdmin.Repo.query!("select * from pgmq.purge_queue($1)", [socket.assigns.queue.name])

    socket
    |> put_flash(:info, "Queue purged successfully")
    |> noreply_push_navigate()
  end

  def handle_event("delete", %{"msg_id" => message_id, "source" => "queue"}, socket) do
    message_id = String.to_integer(message_id)
    :ok = PgmqAdmin.Queues.delete_message(socket.assigns.queue.name, message_id)

    socket
    |> put_flash(:info, "Message deleted successfully")
    |> noreply_push_navigate()
  end

  def handle_event("delete", %{"msg_id" => message_id, "source" => "archive"}, socket) do
    message_id = String.to_integer(message_id)
    :ok = PgmqAdmin.Queues.delete_message_from_archive(socket.assigns.queue.name, message_id)

    socket
    |> put_flash(:info, "Message deleted successfully")
    |> noreply_push_navigate()
  end

  def handle_event("archive", %{"msg_id" => message_id}, socket) do
    message_id = String.to_integer(message_id)
    :ok = PgmqAdmin.Queues.archive_message(socket.assigns.queue.name, message_id)

    socket
    |> put_flash(:info, "Message archived successfully")
    |> noreply_push_navigate()
  end

  def handle_event("unarchive", %{"msg_id" => message_id}, socket) do
    message_id = String.to_integer(message_id)
    :ok = PgmqAdmin.Queues.unarchive_message(socket.assigns.queue.name, message_id)

    socket
    |> put_flash(:info, "Message unarchived successfully")
    |> noreply_push_navigate()
  end

  @impl true
  def handle_info(
        {PgmqAdminWeb.QueueLive.NewMessageFormComponent, {:messages_sent, count}},
        socket
      ) do
    if count == 1 do
      socket
      |> put_flash(:info, "Message sent successfully")
      |> noreply_push_navigate()
    else
      socket
      |> put_flash(:info, "#{count} messages sent successfully")
      |> noreply_push_navigate()
    end
  end

  defp noreply_push_navigate(socket) do
    {:noreply, push_navigate(socket, to: "/queues/#{socket.assigns.queue.name}")}
  end

  defp page_title(:show), do: "Show Queue"
  defp page_title(:new_message), do: "New message"
end
