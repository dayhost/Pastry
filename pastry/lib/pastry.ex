defmodule Pastry do
  def main(args) do
    IO.puts "======start node======"
    Pastry.Controller.start_nodes()
  end
end
