defmodule PgmqAdminWeb.QueueLive.NewMessageFormComponent do
  use PgmqAdminWeb, :live_component

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.header>
        <%= @title %>
      </.header>
      <.simple_form
        for={@form}
        id="new_messages"
        as={:new_messages}
        phx-target={@myself}
        phx-change="validate"
        phx-submit="save"
      >
        <p>
          Write the message JSON in the textarea. If batch-mode is enabled, a JSON
          array is expected, and each item of the array will be published as a separate
          message.
        </p>
        <.input field={@form[:batch_mode]} type="checkbox" label="Batch mode" />
        <.input field={@form[:content]} type="textarea" label="Message(s) content" />
        <:actions>
          <.button phx-disable-with="Publishing...">Publish message(s)</.button>
        </:actions>
      </.simple_form>
    </div>
    """
  end

  defp changeset(new_messages) do
    {new_messages, %{batch_mode: :boolean, content: :string}}
    |> Ecto.Changeset.change()
  end

  defp validate(new_messages, params) do
    {new_messages, %{batch_mode: :boolean, content: :string}}
    |> Ecto.Changeset.cast(params, [:batch_mode, :content])
    |> Map.put(:action, :validate)
  end

  @impl true
  def update(assigns, socket) do
    new_messages = %{}
    changeset = changeset(new_messages)

    {:ok,
     socket
     |> assign(:new_messages, new_messages)
     |> assign(assigns)
     |> assign_form(changeset)}
  end

  @impl true
  def handle_event("validate", %{"new_messages" => params}, socket) do
    changeset = validate(socket.assigns.new_messages, params)
    {:noreply, assign_form(socket, changeset)}
  end

  def handle_event("save", %{"new_messages" => params}, socket) do
    changeset = validate(socket.assigns.new_messages, params)

    case extract_messages(changeset) do
      {:ok, messages} ->
        PgmqAdmin.Queues.send_messages(socket.assigns.queue.name, messages)
        notify_parent({:messages_sent, length(messages)})
        {:noreply, socket}

      {:error, :invalid_json} ->
        changeset = Ecto.Changeset.add_error(changeset, :content, "Invalid JSON")
        {:noreply, assign_form(socket, changeset)}

      {:error, :not_a_json_array} ->
        changeset =
          Ecto.Changeset.add_error(changeset, :content, "Not a JSON array when using batch mode")

        {:noreply, assign_form(socket, changeset)}
    end
  end

  defp extract_messages(changeset) do
    batch_mode = Ecto.Changeset.get_field(changeset, :batch_mode, false)
    content = Ecto.Changeset.get_field(changeset, :content, "")

    case {Jason.decode(content), batch_mode} do
      {{:ok, payload}, true} when is_list(payload) ->
        {:ok, payload}

      {{:ok, payload}, false} ->
        {:ok, [payload]}

      {{:ok, _}, true} ->
        {:error, :not_a_json_array}

      {{:error, _}, _} ->
        {:error, :invalid_json}
    end
  end

  defp assign_form(socket, %Ecto.Changeset{} = changeset) do
    assign(socket, :form, to_form(changeset, as: :new_messages))
  end

  defp notify_parent(msg), do: send(self(), {__MODULE__, msg})
end
