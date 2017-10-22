defmodule Pastry do
  def main(args) do
    IO.puts "======start node======"
    Pastry.Controller.start_nodes(3, 3)
  end
end
