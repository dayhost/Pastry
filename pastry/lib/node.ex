defmodule Pastry.Node do
    def init(prox_node, node_num, arg_b, send_num) do
        #start init
        self_id = Pastry.Utilies.get_node_id(node_num, arg_b)
        # create its own db
        {:ok, server_pid} = Pastry.Table.start_link(node_num, arg_b)
        IO.puts "get an empty node and start to init"
        # start to get routing info
        # send start to send init info
        if (prox_node!=:no_node) do
            Pastry.Table.update_neighbor(server_pid, %{"neighbor"=>[prox_node])
            send(prox_node, {:init, {prox_node, self_id, 0}})
        end
        #start to recieve table from nodes
        receive_data(server_pid, arg_b, send_num)
    end

    # loop and wait for data, if get data spwan a worker to process route logic
    def receive_data(server_pid, arg_b, send_num) do
        receive do
            {:init, {destID, new_node_pid, hop_num}} ->
                # get init message from other node
                # message {:init, {destID, new_node_pid, hop_num}}
                {:ok, next_node_address} = route_logic(destID, server_pid)
                self_table = get_self_table(server_pid)
                self_id = get_self_node_id(server_pid)
                send(next_node_address, {:init, {destID, new_node_pid, hop_num+1}})
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
                send_count = Pastry.Table.get_send_counter(server_pid)
                if(send_count<send_num) do
                    {:ok, next_node_address} = route_logic(destID)
                    send(next_node_address, {:normal, {destID, msg, hop_num+1}})
                    Pastry.Table.set_send_counter(server_pid, send_count+1)
                else
                    IO.puts "=========#{Kernel.inspect(self())} stoped ==========="
                    Process.exit(self())
                end
        end
        receive_data(server_pid, arg_b)
    end

    def route_logic(destID, server_pid) do
        next_node_address = Pastry.Table.get_next_from_leaf(destID)
        if (next_node_address==nil) do
            self_id = get_self_node_id(server_pid)
            com_len = Pastry.Utilies.shl(destID, self_id, 0)
            next_node_address = Pastry.Table.get_next_from_routing(destID, com_len)
        end
        if (next_node_address==nil) do
            next_node_address = Pastry.Table.get_next_from_all(destID)
        end
        next_node_address
    end

    def update_leaf(server_pid, table) do
        result_table = %{}
        result_table = Map.put_new(result_table, "leaf", Map.get(table, "leaf"))
        result_table = Map.put_new(result_table, "routing", Map.get(table, "routing"))
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
            send_table_map(Map.get(self_table, "routing"), self_table)
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

    defp send_table_map(map, self_table) do
        Enum.each(
            map,
            fn(key, value) ->
                Enum.map(
                    value,
                    fn(x) ->
                        send(x, {:join, {self_table}})
                    end
                )
            end
        )
    end

    defp send_table_list(list, self_table) do
        Enum.map(
            list,
            fn(x) ->
                send(x, {:join, {self_table}})
            end
        )
    end
end