defmodule Pastry do
  def main(args) do
    IO.puts "======start node======"
    Pastry.Controller.start_nodes(10, 5)
  end
end
