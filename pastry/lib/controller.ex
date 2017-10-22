defmodule Pastry.Controller do
    def start_nodes(max_node_num, send_count) do
        {:ok, node_status} = Pastry.ControllerStatus.start_link()
        arg_b = 4
        Enum.map(
            1,...,max_node_num,
            fn(x) ->
                prox_node = Pastry.ControllerStatus.get_random_node(node_status)
                {:ok, pid} = spawn_link(fn -> Pastry.Node.init(prox_node, x, arg_b, send_count, node_status) end)
                Pastry.ControllerStatus.add_node(node_status, pid)
            end
        )
        random_send_msg(node_status, max_node_num, arg_b)
    end

    def random_send_msg(node_status, max_node_num, arg_b) do
        :timer.sleep(1000)
        node_pid = Pastry.ControllerStatus.get_random_node(node_status)
        if node_pid != :no_node do
            if Process.alive?(node_pid)==false do
                Pastry.ControllerStatus.remove_node(node_pid)
            else
                destID = :random.uniform(max_node_num)
                destID = Pastry.Utilies.get_node_id(destID)
                send(node_pid, {:normal, {destID, "hello", -1}})
                Pastry.ControllerStatus.add_msg_counter(node_status)
            end
            random_send_msg(node_status, max_node_num, arg_b)
        else
            avg_hop = Pastry.ControllerStatus.get_avg_hop(node_status)
            IO.puts "avg hop of the network is #{avg_hop}"
        end
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

    def add_msg_counter(server) do
        GenServer.cast(server, {:add_msg_counter})
    end

    def add_hop_counter(server, hop_num) do
        GenServer.cast(server, {:add_hop_counter, hop_num})
    end

    def get_avg_hop(server) do
        GenServer.call(server, {:get_avg_hop})
    end

    # callback function
    def init(:ok) do
        {:ok, %{"nodes"=>[], "total_hop"=>0, "total_msg"=>0}
    end

    def handle_call({:get_random_node}, _from, status) do
        node_list = Map.get(status, "nodes")
        len = length(node_list)
        if len>0 do
            if len>1 do
                random_number = :rand.uniform(len) - 1
            else
                random_number = 0
            end
            {rand_node, node_list} = List.pop_at(node_list, random_number)
        else
            rand_node = :no_node
        end
        {:reply, rand_node, status}
    end

    def handle_cast({:add_node, node}, status) do
        {:noreply, Map.update(status, "nodes", &(List.insert_at(&1, -1, node)))}
    end

    def handle_cast({:remove_node, node}, status) do
        {:noreply, Map.update(status, "nodes", &(List.delete(&1, node))}
    end

    def handle_cast({:add_msg_counter}, status) do
        {:noreply, Map.update(status, "total_msg", &(&1+1)}
    end

    def handle_cast({:add_hop_counter, hop_num}, status) do
        {:noreply, Map.update(status, "total_hop", &(&1+hop_num))}
    end

    def handle_call({:get_avg_hop}, _from, status) do
        total_msg = Map.get(status, "total_msg")
        total_hop = Map.get(status, "total_jop")
        {:reply, total_hop/total_msg, status}
    end
end