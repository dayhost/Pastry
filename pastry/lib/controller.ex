defmodule Pastry.Controller do
    def start_nodes(max_node_num, send_count) do
        {:ok, node_status} = Pastry.ControllerStatus.start_link()
        arg_b = 4
        Enum.map(
            1,...,max_node_num,
            fn(x) ->
                prox_node = Pastry.ControllerStatus.get_random_node(node_status)
                {:ok, pid} = spawn_link(fn -> Pastry.Node.init(prox_node, x, arg_b, send_count) end)
                Pastry.ControllerStatus.add_node(node_status, pid)
            end
        )
        random_send_msg(node_status, max_node_num, arg_b)
    end

    def random_send_msg(node_status, max_node_num, arg_b) do
        :timer.sleep(1000)
        node_pid = Pastry.ControllerStatus.get_random_node(node_status)
        if Process.alive?(node_pid)==false do
            Pastry.ControllerStatus.remove_node(node_pid)
        else
            destID = :random.uniform(max_node_num)
            destID = Pastry.Utilies.get_node_id(destID)
            send(node_pid, {:normal, {destID, "hello", -1}})
        end
        random_send_msg(node_status, max_node_num, arg_b)
    end
end

defmodule Pastry.ControllerStatus do
    use GenServer
    def start_link() do
        GenServer.start_link(__MODULE__, :ok, [])
    end

    # user API
    def add_node(server, node) do
        GenServer.cast(server, {:add_node, node})
    end

    def get_random_node(server) do
        GenServer.call(server, {:get_random_node})
    end

    def remove_node(server, node) do
        GenServer.cast(server, {:remove_node, node})
    end

    # callback function
    def init(:ok) do
        {:ok, []}
    end

    def handle_call({:get_random_node}, _from, status) do
        len = length(status)
        if len>0 do
            if len>1 do
                random_number = :rand.uniform(len) - 1
            else
                random_number = 0
            end
            {rand_node, status} = List.pop_at(status, random_number)
            status = List.insert_at(status, random_number, rand_node)
        else
            rand_node = :no_node
        end
        {:reply, rand_node, status}
    end

    def handle_cast({:add_node, node}, status) do
        {:noreply, List.insert_at(status, -1, node)}
    end

    def handle_cast({:remove_node, node}, status) do
        {:noreply, List.delete(status, node)}
    end
end