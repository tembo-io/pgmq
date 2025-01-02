defmodule PgmqAdminWeb.QueueLive.FormComponent do
  use PgmqAdminWeb, :live_component

  alias PgmqAdmin.Queues

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.header>
        <%= @title %>
      </.header>
      <.simple_form
        for={@form}
        id="queue-form"
        as={:queue}
        phx-target={@myself}
        phx-change="validate"
        phx-submit="save"
      >
        <.input field={@form[:name]} label="Queue name" />
        <.input field={@form[:is_partitioned]} type="checkbox" label="Partitioned" />
        <.input field={@form[:is_unlogged]} type="checkbox" label="Unlogged" />
        <%= if Ecto.Changeset.get_field(@form.source, :is_partitioned, false) do %>
          <.input field={@form[:partition_interval]} label="Partition interval" />
          <.input field={@form[:retention_interval]} label="Retention interval" />
        <% end %>
        <:actions>
          <.button phx-disable-with="Saving...">Save Queue</.button>
        </:actions>
      </.simple_form>
    </div>
    """
  end

  defp changeset(queue) do
    {queue,
     %{
       name: :string,
       is_unlogged: :boolean,
       is_partitioned: :boolean,
       partition_interval: :string,
       retention_interval: :string
     }}
    |> Ecto.Changeset.change()
  end

  defp change_queue(queue, queue_params) do
    {queue,
     %{
       name: :string,
       is_unlogged: :boolean,
       is_partitioned: :boolean,
       partition_interval: :string,
       retention_interval: :string
     }}
    |> Ecto.Changeset.cast(queue_params, [
      :name,
      :is_unlogged,
      :is_partitioned,
      :partition_interval,
      :retention_interval
    ])
    |> Ecto.Changeset.validate_required(:name)
    |> Ecto.Changeset.validate_change(:name, fn :name, _name ->
      # TODO: validate if smaller than max size and has only appropriate characters
      []
    end)
    |> Ecto.Changeset.validate_length(:name, min: 3, max: 43)
    |> validate_not_partitioned_and_unlogged()
    |> validate_conditionally_required()
    |> Map.put(:action, :validate)
  end

  defp validate_not_partitioned_and_unlogged(changeset) do
    partitioned = Ecto.Changeset.get_field(changeset, :is_partitioned, false)
    unlogged = Ecto.Changeset.get_field(changeset, :is_unlogged, false)

    if partitioned and unlogged do
      Ecto.Changeset.add_error(changeset, :is_unlogged, "Can't be both partitioned and unlogged")
    else
      changeset
    end
  end

  defp validate_conditionally_required(changeset) do
    partitioned = Ecto.Changeset.get_field(changeset, :is_partitioned, false)

    if partitioned do
      Ecto.Changeset.validate_required(changeset, [:partition_interval, :retention_interval])
    else
      changeset
    end
  end

  @impl true
  def update(%{queue: queue} = assigns, socket) do
    changeset = changeset(queue)

    {:ok,
     socket
     |> assign(assigns)
     |> assign_form(changeset)}
  end

  @impl true
  def handle_event("validate", %{"queue" => queue_params}, socket) do
    changeset = change_queue(socket.assigns.queue, queue_params)
    {:noreply, assign_form(socket, changeset)}
  end

  def handle_event("save", %{"queue" => queue_params}, socket) do
    save_queue(socket, socket.assigns.action, queue_params)
  end

  defp save_queue(socket, :new, queue_params) do
    changeset = change_queue(socket.assigns.queue, queue_params)

    if changeset.valid? do
      changeset
      |> Ecto.Changeset.apply_changes()
      |> Queues.create_queue()
      |> case do
        {:ok, queue} ->
          notify_parent({:saved, queue})

          {:noreply,
           socket
           |> put_flash(:info, "Queue created successfully")
           |> push_patch(to: socket.assigns.patch)}

        {:error, :already_exists} ->
          {:noreply, put_flash(socket, :danger, "Queue already exists")}
      end
    else
      {:noreply, assign_form(socket, changeset)}
    end
  end

  defp assign_form(socket, %Ecto.Changeset{} = changeset) do
    assign(socket, :form, to_form(changeset, as: :queue))
  end

  defp notify_parent(msg), do: send(self(), {__MODULE__, msg})
end
