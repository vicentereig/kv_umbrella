defmodule KVServer do
  require Logger

  def accept(port) do
    {:ok, socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info("Accepting connections on port #{port}...")
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    Logger.info("Client connected")
    {:ok, pid} = Task.Supervisor.start_child(KVServer.TaskSupervisor, fn -> serve(client) end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end

  defp serve(socket) do
    socket
    |> read_line
    |> parse_command
    |> write_line(socket)

    serve(socket)
  end

  defp read_line(socket) do
    :gen_tcp.recv(socket, 0)
  end

  defp parse_command({:ok, line}) do
    case KVServer.Command.parse(line) do
      {:ok, command} -> KVServer.Command.run(command)
      {:error, _} = err -> err
    end
  end

  defp parse_command({:error, _} = err) do
    err
  end

  defp write_line({:ok, text}, socket) do
    :gen_tcp.send(socket, text)
  end

  defp write_line({:error, :unknown_command}, socket) do
    :gen_tcp.send(socket, "UNKNOWN COMMAND\r\n")
  end

  defp write_line({:error, :not_found}, socket) do
    :gen_tcp.send(socket, "NOT FOUND\r\n")
  end

  defp write_line({:error, :closed}, _socket) do
    exit(:shutdown)
  end

  defp write_line({:error, error}, socket) do
    :gen_tcp.send(socket, "ERROR\r\n")
    exit(error)
  end
end
