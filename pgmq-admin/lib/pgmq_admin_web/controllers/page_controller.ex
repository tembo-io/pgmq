defmodule PgmqAdminWeb.PageController do
  use PgmqAdminWeb, :controller

  def redirect_to_queues(conn, _params) do
    redirect(conn, to: "/queues")
  end
end
