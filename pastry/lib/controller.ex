defmodule Pastry.Controller do
    def start_nodes(max_node_num, send_count) do
        {:ok, node_status} = Pastry.ControllerStatus.start_link()
        arg_b = 4
        Enum.map(
            1..max_node_num,
            fn(x) ->
                prox_node_tuple = Pastry.ControllerStatus.get_random_prox_node(node_status)
                self_id = Pastry.Utilies.get_node_id(x, arg_b)
                pid = spawn_link(fn -> Pastry.Node.init(prox_node_tuple, x, self_id, arg_b, send_count, node_status) end)
                self_node_tuple = {self_id, x, pid}
                Pastry.ControllerStatus.add_node(node_status, self_node_tuple)
                :timer.sleep(1000)
            end
        )
        :timer.sleep(5000)
        random_send_msg(node_status, max_node_num, arg_b)
    end

    def random_send_msg(node_status, max_node_num, arg_b) do
        :timer.sleep(1000)
        {send_node_tuple, recv_node_tuple} = Pastry.ControllerStatus.get_random_node_pair(node_status)
        if(Kernel.is_nil(send_node_tuple)||Kernel.is_nil(recv_node_tuple)) do
            avg_hop = Pastry.ControllerStatus.get_avg_hop(node_status)
            IO.puts "avg hop of the network is #{avg_hop}"
        else
            if(Process.alive?(elem(send_node_tuple,2))==false)do
                Pastry.ControllerStatus.remove_node(node_status, send_node_tuple)
            else
                if(Process.alive?(elem(recv_node_tuple,2))==false) do
                    Pastry.ControllerStatus.remove_node(node_status, recv_node_tuple)
                else
                    send(elem(send_node_tuple,2), {:normal, {elem(recv_node_tuple, 0), "hello", 0}})
                end
            end
            random_send_msg(node_status, max_node_num, arg_b)
        end
        # node_tuple = Pastry.ControllerStatus.get_random_recv_node(node_status, dest_int)
        # if node_tuple != nil do
        #     if Process.alive?(elem(node_tuple,2))==false do
        #         Pastry.ControllerStatus.remove_node(node_status, node_tuple)
        #     else
        #         dest_int = :random.uniform(max_node_num)
        #         destID = Pastry.Utilies.get_node_id(dest_int, arg_b)
        #         send(elem(node_tuple,2), {:normal, {destID, "hello", 1}})
        #         Pastry.ControllerStatus.add_msg_counter(node_status)
        #     end
        #     random_send_msg(node_status, max_node_num, arg_b)
        # else
        #     avg_hop = Pastry.ControllerStatus.get_avg_hop(node_status)
        #     IO.puts "avg hop of the network is #{avg_hop}"
        # end
    end
end

defmodule Pastry.ControllerStatus do
    use GenServer
    def start_link() do
        GenServer.start_link(__MODULE__, :ok, [])
    end

    # user API
    def add_node(server, node_tuple) do
        GenServer.cast(server, {:add_node, node_tuple})
    end

    def get_random_prox_node(server) do
        GenServer.call(server, {:get_random_prox_node})
    end

    def get_random_node_pair(server)do
        GenServer.call(server, {:get_random_node_pair})
    end

    def remove_node(server, node_tuple) do
        GenServer.cast(server, {:remove_node, node_tuple})
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
        {:ok, %{"node_tuples"=>[], "total_hop"=>0, "total_msg"=>0}}
    end

    def handle_call({:get_random_prox_node}, _from, status) do
        node_tuple_list = Map.get(status, "node_tuples")
        len = length(node_tuple_list)
        if len>0 do
            random_number = :rand.uniform(len) - 1
            {rand_node_tuple, node_list} = List.pop_at(node_tuple_list, random_number)
        else
            rand_node_tuple = nil
        end
        {:reply, rand_node_tuple, status}
    end

    def handle_call({:get_random_node_pair}, _from, status) do
        node_tuple_list = Map.get(status, "node_tuples")
        send_node_tuple = nil
        recv_node_tuple = nil
        len = length(node_tuple_list)
        if len>1 do
            {send_node_tuple, rest_list} = List.pop_at(node_tuple_list, :rand.uniform(len) - 1)
            {recv_node_tuple, rest_list} = List.pop_at(rest_list, :rand.uniform(length(rest_list)) - 1)
        end
        {:reply, {send_node_tuple, recv_node_tuple}, status}
    end

    def handle_cast({:add_node, node_tuple}, status) do
        {:noreply, Map.update!(status, "node_tuples", &(List.insert_at(&1, -1, node_tuple)))}
    end

    def handle_cast({:remove_node, node_tuple}, status) do
        {:noreply, Map.update!(status, "node_tuples", &(List.delete(&1, node_tuple)))}
    end

    def handle_cast({:add_msg_counter}, status) do
        {:noreply, Map.update!(status, "total_msg", &(&1+1))}
    end

    def handle_cast({:add_hop_counter, hop_num}, status) do
        {:noreply, Map.update!(status, "total_hop", &(&1+hop_num))}
    end

    def handle_call({:get_avg_hop}, _from, status) do
        total_msg = Map.get(status, "total_msg")
        total_hop = Map.get(status, "total_hop")
        IO.puts "#{total_msg}, #{total_hop}"
        {:reply, total_hop/total_msg, status}
    end
end