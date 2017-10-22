defmodule Pastry.Node do
    def init(prox_node, node_num, arg_b, send_num, msg_aggr_server) do
        #start init
        self_id = Pastry.Utilies.get_node_id(node_num, arg_b)
        # create its own db
        {:ok, server_pid} = Pastry.Table.start_link(self_id, node_num, self(), arg_b)
        IO.puts "get an empty node and start to init"
        # start to get routing info
        # send start to send init info
        if (prox_node!=:no_node) do
            Pastry.Table.update_neighbor(server_pid, %{"neighbor"=>[(self_pid, node_num, prox_node)])
            send(prox_node, {:init, {self_id, self(), 0}})
        end
        #start to recieve table from nodes
        receive_data(server_pid, arg_b, send_num, msg_aggr_server)
    end

    # loop and wait for data, if get data spwan a worker to process route logic
    def receive_data(server_pid, arg_b, send_num, msg_aggr_server) do
        receive do
            {:init, {destID, new_node_pid, hop_num}} ->
                # get init message from other node
                # message {:init, {destID, new_node_pid, hop_num}}
                self_id = get_self_node_id(server_pid)
                self_table = get_self_table(server_pid)
                if (Pastry.Utilies.node_id_diff(self_id, destID)<2) do
                    IO.puts ":init message get the destination"
                else
                    {:ok, next_node_address} = route_logic(destID, server_pid)
                    send(next_node_address, {:init, {destID, new_node_pid, hop_num+1}})
                end
                send(next_node_address, {:update, {self_table, self_id, hop_num}})
            {:update, {new_table, sourceID, hop_num}} ->
                # get update table from old node
                # message {:update, {%{table}, sourceID, hop_num}}
                update_leaf(server_pid, new_table)
                if hop_num == 1 do
                    update_neighbor(server_pid, new_table)
                end
                # this will triggle with condition
                send_table_to_all_neighbor(server_pid, arg_b)
            {:join, {new_table}} ->
                # update its table
                update_leaf(server_pid, new_table)
                if hop_num == 1 do
                    update_neighbor(server_pid, new_table)
                end
            {:normal, {destID, msg, hop_num}} ->
                # get normal(forward) message from other node
                # check if it want to terminate
                send_count = Pastry.Table.get_send_count`er(server_pid)
                if(send_count<send_num) do
                    self_id = get_self_node_id(server_pid)
                    if (self_id==destID) do
                        IO.puts ":nromal message get the destination"
                        # return num of hop used in send msg
                        Pastry.ControllerStatus.add_msg_counter(msg_aggr_server, hop)
                    else
                        {:ok, next_node_address} = route_logic(destID, server_pid)
                        send(next_node_address, {:normal, {destID, msg, hop_num+1}})
                        Pastry.Table.set_send_counter(server_pid, send_count+1)
                    end
                else
                    IO.puts "=========#{Kernel.inspect(self())} stoped ==========="
                    Process.exit(self())
                end
        end
        receive_data(server_pid, arg_b)
    end

    def route_logic(destID, server_pid) do
        next_node_address = Pastry.Table.get_next_from_leaf(server_pid, destID)
        com_len = Pastry.Utilies.shl(self_id, destID, 0)
        if (next_node_address==nil) do
            self_id = get_self_node_id(server_pid)
            next_node_address = Pastry.Table.get_next_from_routing(server_pid, destID, com_len)
        end
        if (next_node_address==nil) do
            next_node_address = Pastry.Table.get_next_from_all(server_pid, destID, com_len)
        end
        next_node_address
    end

    def update_leaf(server_pid, table) do
        result_table = %{}
        result_table = Map.put_new(result_table, "leaf", Map.get(table, "leaf"))
        result_table = Map.put_new(result_table, "routing", Map.get(table, "routing"))
        result_table = Map.put_new(result_table, "source_node", Map.get(table, "source_node"))
        Pastry.Table.update_leaf(server_pid, result_table)
        update_neighbor(server_pid, table)
    end

    def update_neighbor(server_pid, table) do
        result_table = %{}
        result_table = Map.put_new(result_table, "neighbor", Map.get(table, "neighbor"))
        Pastry.Table.update_neighbor(server_pid, result_table)
    end

    def get_self_table(server_pid) do
        Pastry.Table.get_self_table(server_pid)
    end

    def get_self_node_id(server_pid) do
        Pastry.Table.get_self_node_id(server_pid)
    end

    def send_table_to_all_neighbor(server_pid, arg_b) do
        if check_cretria(server_pid, arg_b) do
            self_table = get_self_table(server_pid)
            routing_list = Pastry.Utilies.flat_routing_table(Map.get(self_table, "routing"))
            send_table_list(routing_list, self_table)
            send_table_list(Map.get(self_table, "leaf"), self_table)
            send_table_list(Map.get(self_table, "neighbor"), self_table)
        end
    end

    defp check_cretria(server_pid, arg_b) do
        counter = Pastry.Table.get_counter(server_pid)
        thresdhold = :math.log2(arg_b)
        flag = false
        if(counter>thresdhold) do
            flag = true
        end
        timer = Pastry.Table.get_timestamp(server_pid)
        if(timer!=nil)do
            Pastry.Table.set_timestamp(server_pid, System.system_time(:millisecond))
        else
            now = System.system_time(:millisecond)
            if ((now-timer)/1000>5) do
                flag = true
            end
        end
        flag
    end

    defp send_table_list(list, self_table) do
        Enum.map(
            list,
            fn(x) ->
                send(Kernel.elem(x, 1), {:join, {self_table}})
            end
        )
    end
end