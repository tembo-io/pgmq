defmodule PgmqAdminWeb.QueueLiveTest do
  use PgmqAdminWeb.ConnCase

  import Phoenix.LiveViewTest
  import PgmqAdmin.QueuesFixtures

  @create_attrs %{}
  @update_attrs %{}
  @invalid_attrs %{}

  defp create_queue(_) do
    queue = queue_fixture()
    %{queue: queue}
  end

  describe "Index" do
    setup [:create_queue]

    test "lists all queues", %{conn: conn} do
      {:ok, _index_live, html} = live(conn, ~p"/queues")

      assert html =~ "Listing Queues"
    end

    test "saves new queue", %{conn: conn} do
      {:ok, index_live, _html} = live(conn, ~p"/queues")

      assert index_live |> element("a", "New Queue") |> render_click() =~
               "New Queue"

      assert_patch(index_live, ~p"/queues/new")

      assert index_live
             |> form("#queue-form", queue: @invalid_attrs)
             |> render_change() =~ "can&#39;t be blank"

      assert index_live
             |> form("#queue-form", queue: @create_attrs)
             |> render_submit()

      assert_patch(index_live, ~p"/queues")

      html = render(index_live)
      assert html =~ "Queue created successfully"
    end

    test "updates queue in listing", %{conn: conn, queue: queue} do
      {:ok, index_live, _html} = live(conn, ~p"/queues")

      assert index_live |> element("#queues-#{queue.id} a", "Edit") |> render_click() =~
               "Edit Queue"

      assert_patch(index_live, ~p"/queues/#{queue}/edit")

      assert index_live
             |> form("#queue-form", queue: @invalid_attrs)
             |> render_change() =~ "can&#39;t be blank"

      assert index_live
             |> form("#queue-form", queue: @update_attrs)
             |> render_submit()

      assert_patch(index_live, ~p"/queues")

      html = render(index_live)
      assert html =~ "Queue updated successfully"
    end

    test "deletes queue in listing", %{conn: conn, queue: queue} do
      {:ok, index_live, _html} = live(conn, ~p"/queues")

      assert index_live |> element("#queues-#{queue.id} a", "Delete") |> render_click()
      refute has_element?(index_live, "#queues-#{queue.id}")
    end
  end

  describe "Show" do
    setup [:create_queue]

    test "displays queue", %{conn: conn, queue: queue} do
      {:ok, _show_live, html} = live(conn, ~p"/queues/#{queue}")

      assert html =~ "Show Queue"
    end

    test "updates queue within modal", %{conn: conn, queue: queue} do
      {:ok, show_live, _html} = live(conn, ~p"/queues/#{queue}")

      assert show_live |> element("a", "Edit") |> render_click() =~
               "Edit Queue"

      assert_patch(show_live, ~p"/queues/#{queue}/show/edit")

      assert show_live
             |> form("#queue-form", queue: @invalid_attrs)
             |> render_change() =~ "can&#39;t be blank"

      assert show_live
             |> form("#queue-form", queue: @update_attrs)
             |> render_submit()

      assert_patch(show_live, ~p"/queues/#{queue}")

      html = render(show_live)
      assert html =~ "Queue updated successfully"
    end
  end
end
