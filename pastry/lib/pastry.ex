defmodule Pastry do
  def main(args) do
    IO.puts "======start node======"
    [numNode, numRequests] = args
    numNode = String.to_integer(numNode)
    numRequests = String.to_integer(numRequests)
    Pastry.Controller.start_nodes(numNode, numRequests)
  end
end
