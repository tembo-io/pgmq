defmodule PgmqAdminWeb.Router do
  use PgmqAdminWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, {PgmqAdminWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", PgmqAdminWeb do
    pipe_through :browser
    get "/", PageController, :redirect_to_queues

    live "/queues", QueueLive.Index, :index
    live "/queues/new", QueueLive.Index, :new
    live "/queues/:id", QueueLive.Show, :show
    live "/queues/:id/new_message", QueueLive.Show, :new_message
  end

  if Application.compile_env(:pgmq_admin, :dev_routes) do
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: PgmqAdminWeb.Telemetry
    end
  end
end
