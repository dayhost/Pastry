defmodule Pastry.Node do
    def init(prox_node_tuple, node_num, self_id, arg_b, send_num, msg_aggr_server) do
        # create its own db
        {:ok, server_pid} = Pastry.Table.start_link(self_id, node_num, self(), arg_b)
        IO.puts "get an empty node and start to init and prox is #{inspect(prox_node_tuple)}"
        # start to get routing info
        # send start to send init info
        if (prox_node_tuple==nil) do
            #start to recieve table from nodes
            receive_data(server_pid, arg_b, send_num, msg_aggr_server)
        else
            Pastry.Table.update_neighbor(server_pid, %{"neighbor" => [prox_node_tuple]})
            send(Kernel.elem(prox_node_tuple, 2), {:init, {self_id, self(), 1}})
            IO.puts "#{inspect(self())} send message to #{inspect(prox_node_tuple)}"
            #start to recieve table from nodes
            receive_data(server_pid, arg_b, send_num, msg_aggr_server)
        end
    end

    # loop and wait for data, if get data spwan a worker to process route logic
    def receive_data(server_pid, arg_b, send_num, msg_aggr_server) do
        receive do
            {:init, {destID, new_node_pid, hop_num}} ->
                # get init message from other node
                # message {:init, {destID, new_node_pid, hop_num}}
                self_id = get_self_node_str(server_pid)
                self_table = get_self_table(server_pid)
                node_id_int = Pastry.Utilies.id_to_number(destID, arg_b)
                IO.puts "#{inspect(self())} get :init message from #{inspect(new_node_pid)}"
                if (Pastry.Utilies.node_id_diff(self_id, destID, arg_b)<2) do
                    IO.puts ":init message get the destination"
                else
                    next_node_address = route_logic(destID, server_pid, self_id)
                    IO.puts "#{inspect(self())} Next node #{inspect(next_node_address)}"
                    send(elem(next_node_address, 2), {:init, {destID, new_node_pid, hop_num+1}})
                end
                
                if hop_num == 1 do
                    #IO.puts "#{inspect(self())} gets init msg from #{inspect(new_node_pid)}  "
                    Pastry.Table.update_neighbor(server_pid,%{"neighbor"=>[{destID, node_id_int, new_node_pid}]})
                    Pastry.Table.insert_leaf(server_pid, {destID, node_id_int, new_node_pid})
                else
                    Pastry.Table.insert_leaf(server_pid, {destID, node_id_int, new_node_pid})
                end
                
                IO.puts "\n#{inspect(self())}: #{inspect(Pastry.Table.get_self_table(server_pid))}========"
                send(new_node_pid, {:update, {self_table, self_id, hop_num}})
            {:update, {new_table, sourceID, hop_num}} ->
                # get update table from old node
                # message {:update, {%{table}, sourceID, hop_num}}
                self_id = get_self_node_str(server_pid)
                update_leaf(server_pid, new_table, self_id)
                if hop_num == 1 do
                    update_neighbor(server_pid, new_table)
                end
                # this will triggle with condition
                #send_table_to_all_neighbor(server_pid, arg_b)
                set_timer_for_send(server_pid, arg_b)
            {:join, {new_table}} ->
                # update its table
                self_id = get_self_node_str(server_pid)
                update_leaf(server_pid, new_table, self_id)
                update_neighbor(server_pid, new_table)
            {:normal, {destID, msg, hop_num}} ->
                # get normal(forward) message from other node
                # check if it want to terminate
                # IO.puts "Receive normal msg."
                send_count = Pastry.Table.get_send_counter(server_pid)
                if(send_count<send_num) do
                    self_id = get_self_node_str(server_pid)
                    if (self_id==destID) do
                        IO.puts ":nromal message get the destination and hop num is #{hop_num}"
                        # return num of hop used in send msg
                        Pastry.ControllerStatus.add_hop_counter(msg_aggr_server, hop_num)
                    else
                        if(hop_num==0)do
                            Pastry.ControllerStatus.add_msg_counter(msg_aggr_server)
                        end
                        next_node_address = route_logic(destID, server_pid, self_id)
                        IO.puts "next_node_address: #{inspect(next_node_address)}"
                        send(elem(next_node_address, 2), {:normal, {destID, msg, hop_num+1}})
                        Pastry.Table.add_send_counter(server_pid)
                    end
                else
                    IO.puts "=========#{Kernel.inspect(self())} stoped ==========="
                    Process.exit(self(), :normal)
                end
        end
        receive_data(server_pid, arg_b, send_num, msg_aggr_server)
    end

    def route_logic(destID, server_pid, self_id) do
        next_node_address = Pastry.Table.get_next_from_leaf(server_pid, destID)
        com_len = Pastry.Utilies.shl(self_id, destID, 0)
        if (next_node_address==nil) do
            self_id = get_self_node_str(server_pid)
            next_node_address = Pastry.Table.get_next_from_routing(server_pid, destID, com_len)
        end
        if (next_node_address==nil) do
            next_node_address = Pastry.Table.get_next_from_all(server_pid, destID, com_len)
        end
        next_node_address
    end

    def update_leaf(server_pid, table, self_id) do
        result_table = %{}
        result_table = Map.put_new(result_table, "leaf", Map.get(table, "leaf"))
        result_table = Map.put_new(result_table, "routing", Map.get(table, "routing"))
        result_table = Map.put_new(result_table, "source_node", Map.get(table, "source_node"))
        Pastry.Table.update_leaf(server_pid, result_table, self_id)
    end

    def update_neighbor(server_pid, table) do
        result_table = %{}
        result_table = Map.put_new(result_table, "neighbor", Map.get(table, "neighbor"))
        Pastry.Table.update_neighbor(server_pid, result_table)
    end

    def get_self_table(server_pid) do
        Pastry.Table.get_self_table(server_pid)
    end

    def get_self_node_str(server_pid) do
        Pastry.Table.get_self_node_str(server_pid)
    end

    def send_table_to_all_neighbor(server_pid, arg_b) do
        #if check_cretria(server_pid, arg_b) do
        IO.puts "+++++++++++send table to all neighbor"
        self_table = get_self_table(server_pid)
        routing_list = Pastry.Utilies.flat_nested_dict(Map.get(self_table, "routing"))
        leaf_list = Pastry.Utilies.flat_nested_dict(Map.get(self_table, "leaf"))
        neighbor_list = Map.get(self_table, "neighbor")
        total_list = routing_list ++ leaf_list ++ neighbor_list
        total_list = Enum.uniq(total_list)
        total_list = List.delete(total_list, nil)
        send_table_list(total_list, self_table)
        #end
    end

    def set_timer_for_send(server_pid, arg_b) do
        timer = Pastry.Table.get_time_stamp(server_pid)
        if (timer==0) do
            Pastry.Table.set_time_stamp(server_pid, System.system_time(:millisecond))
            :timer.apply_after(1000, Pastry.Node, :send_table_to_all_neighbor, [server_pid, arg_b])
        end
    end

    # defp check_cretria(server_pid, arg_b) do
    #     counter = Pastry.Table.get_recv_counter(server_pid)
    #     Pastry.Table.set_recv_counter(server_pid, counter + 1)
    #     thresdhold = :math.log2(arg_b)
    #     flag = false
    #     if(counter>thresdhold) do
    #         flag = true
    #         Pastry.Table.set_recv_counter(server_pid, 0)
    #         Pastry.Table.set_timestamp(server_pid, nil)
    #     end
    #     timer = Pastry.Table.get_timestamp(server_pid)
    #     if(timer!=nil)do
    #         Pastry.Table.set_timestamp(server_pid, System.system_time(:millisecond))
    #     else
    #         now = System.system_time(:millisecond)
    #         if ((now-timer)/1000>5) do
    #             flag = true
    #             Pastry.Table.set_recv_counter(server_pid, 0)
    #             Pastry.Table.set_timestamp(server_pid, nil)
    #         end
    #     end
    #     flag
    # end

    defp send_table_list(list, self_table) do
        Enum.map(
            list,
            fn(x) ->
                send(Kernel.elem(x, 2), {:join, {self_table}})
            end
        )
    end
end