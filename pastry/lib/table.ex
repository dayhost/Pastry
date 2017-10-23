defmodule Pastry.Table do
    use GenServer, restart: :transient
    
    @doc """
    Starts the node. 
    : node_id (string)
    """
    def start_link(node_id_str, node_id_int, self_pid, b) do
        GenServer.start_link(__MODULE__, [:ok, node_id_str, node_id_int, self_pid, b], [])
    end

    @doc """
    Insert the node id in the leaf and routing table.
    : node_tuple (node_id_str, node_id_int, node_pid)
    """ 
    def insert_leaf(pid, node_tuple) do
        GenServer.cast(pid, {:insert_leaf, node_tuple})
    end

    @doc """
    Insert the node id in the neighbor set
    : insert_neighbor_list: a list consists of node_tuple (node_id_str, node_id_int, node_pid)
    """
    def insert_neightbor(pid, insert_neighbor_list) do
        GenServer.cast(pid, {:insert_neightbor, insert_neighbor_list})
    end

    @doc """
    Update the leaf set and routing in the current table
    : sourcenode_leaf_routing_map (map)
    """
    def update_leaf(pid, sourcenode_leaf_routing_map) do
        source_node_tuple = Map.get(sourcenode_leaf_routing_map, "source_node") 
        leaf_list = Map.get(sourcenode_leaf_routing_map, "leaf")
        rout_dict = Map.get(sourcenode_leaf_routing_map, "routing") 
        insert_leaf(pid, source_node_tuple)
        if length(leaf_list) != nil do
            Enum.map(leaf_list, fn x -> insert_leaf(pid, x) end)
        end

        rout_list = Map.values(rout_dict)
        if length(rout_list) != nil do
            Enum.map(rout_list, fn x -> insert_leaf(pid, x) end)
        end
    end

    @doc """
    Update the leaf set and routing in the current table
    : neighbor_map (map)
    """
    def update_neighbor(pid, neighbor_map) do
        insert_neighbor_list = Map.get(neighbor_map, "neighbor")
        if length(insert_neighbor_list) != nil do
            Pastry.Table.insert_neighbor(pid, insert_neighbor_list)
        end
    end

    def get_self_node_str(pid) do
        GenServer.call(pid, {:get_self_node_str})
    end

    def get_next_from_leaf(pid, target_node_str) do
        GenServer.call(pid, {:get_next_from_leaf, target_node_str})
    end

    def get_next_from_routing(pid, target_node_str, common_length) do
        GenServer.call(pid, {:get_next_from_routing, target_node_str, common_length})
    end

    def get_next_from_all(pid, target_node_str, common_length) do
        Genserver.call(pid, {:get_next_from_all, target_node_str, common_length})
    end

    def get_self_table(pid) do
        GenServer.call(pid, {:get_self_table})
    end

    def get_send_counter(pid) do
        GenServer.call(pid, {:get_send_counter})
    end

    def set_recv_counter(pid, recv_num) do
        GenServer.cast(pid, {:set_recv_counter, recv_num})
    end

    def get_recv_counter(pid) do
        GenServer.call(pid, {:get_recv_counter})
    end

    def set_send_counter(pid, send_num) do
        GenServer.cast(pid, {:set_send_counter, send_num})
    end

    def get_time_stamp(pid) do
        GenServer.call(pid, {:get_time_stamp})
    end

    def set_time_stamp(pid, time_stamp) do
        GenServer.cast(pid, {:set_time_stamp, time_stamp})
    end

    ## Server Callbacks
    def init([:ok, node_id_str, node_id_int, self_node_pid, b]) do
        tmp = :math.pow(2, b) |> round
        l = tmp
        m = 2 * tmp
        num_routing_col = tmp
        num_routing_row = l # :math.log(num_nodes) / :math.log(tmp)
        state = %{"node_id_str" => node_id_str, "node_id_int" => node_id_int, "self_node_pid" => self_node_pid, "leaf" => %{"small" => [], "large" => []} , 
        "routing" => {}, "neighbor" => [], "leaf_size" => l, "routing_size" => [num_routing_row, num_routing_col], 
        "neighbor_size" => m, "time_stamp" => nil, "recv_counter" => 0,"send_counter" => 0, "arg_b" => b}
        {:ok, state}
    end

    defp insert_routing(insert_node_tuple, self_node_id_str, routing_dict, num_routing_col) do
        insert_node_id_str = elem(insert_node_tuple, 0)
        length_shared_prefix = Pastry.Utilies.shl(self_node_id_str, insert_node_id_str, 0)

        {common_prefix, rest} = String.split_at(insert_node_id_str, length_shared_prefix)
        {next_digit, rest} = String.split_at(rest, 1)
        next_digit = Integer.parse(next_digit)

        case Map.has_key?(routing_dict, length_shared_prefix) do
            true ->
                # Routing dict has already stored the key and list
                target_routing_row = Map.get(routing_dict, length_shared_prefix)
                target_routing_col = Enum.at(target_routing_row, next_digit)
                if target_routing_col != nil do
                    # Already store value with the same next digit
                    poped_routing_tuple = target_routing_col
                    # TODO: insert the poped_routing_item into neighbor set
                end
                new_row = List.replace_at(target_routing_row, next_digit, node_id_tuple)
            false ->
                # Create a new row in the routing map with length_shared_prefix as key
                new_row = Enum.flat_map(1..num_routing_col, fn x -> [nil] end)
                new_row = List.replace_at(new_row, next_digit, insert_node_tuple)
            Map.update(routing_dict, length_shared_prefix, new_row, &(&1=new_row))
        end
        routing_dict
    end

    def handle_call({:get_self_node_str}, _from, state) do
        {:reply, Map.get(state, "node_id_str"), state}
    end

    def handle_cast({:insert_leaf, node_tuple}, state) do
        [node_id_str, node_id_int, node_pid] = node_tuple
        self_node_id_str = Map.get(state, "node_id_str")
        self_node_id_int = Map.get(state, "node_id_int")
        leaf_set = Map.get(state, "leaf")

        case node_id_int <= self_node_id_int do
            true ->
                leaf_flag = "small"
            false ->
                leaf_flag = "large"
        end

        side_leaf_set = Map.get(leaf_set, leaf_flag)
        if length(side_leaf_set) < Map.get(state, "leaf_size") / 2 do
            # leaf set is not full, directly insert into leaf set
            side_leaf_set = [node_tuple] ++ side_leaf_set
            # sorting according to the node_id_int
            side_leaf_set = Enum.sort(side_leaf_set, &(elem(&1, 1) < elem(&2, 1)))  
            leaf_set = Map.update!(leaf_set, leaf_flag, fn x -> side_leaf_set end)
            state = Map.update!(state, "leaf", fn x -> leaf_set end)
        else
            # the leaf set is full
            [num_routing_row, num_routing_col] = Map.get(state, "routing_size") 
            routing_dict = Map.get(state, "routing")
            pop_node_tuple = nil
            if elem(List.first(side_leaf_set), 1) < node_id_int && node_id_int < elem(List.last(side_leaf_set), 1) do
                # node_id is within range of side leaf set
                side_leaf_set = [node_tuple] ++ side_leaf_set
                side_leaf_set = Enum.sort(side_leaf_set, &(elem(&1, 1) < elem(&2, 1))) 
                case leaf_flag do
                    "small" ->
                        {pop_node_tuple, side_leaf_set} = List.pop_at(side_leaf_set, 0)
                    "large" ->
                        {pop_node_tuple, side_leaf_set} = List.pop_at(side_leaf_set, -1)
                end
                leaf_set = Map.update!(leaf_set, leaf_flag, fn x -> side_leaf_set end)
                state = Map.update!(state, "leaf", fn x -> leaf_set end)
                # insert the pop_value into the routing table
                self_node_str = Map.get(state, "node_id_str")
                routing_dict = Pastry.Table.insert_routing(pop_node_tuple, self_node_str, routing_dict)
            else
                # node_id is not within range of side leaf set
                routing_dict = Pastry.Table.insert_routing(pop_node_tuple, routing_dict)
            end
            state = Map.update(state, "routing", fn x -> routing_dict end)
        end
        {:noreply, state}
    end

    def handle_cast({:insert_neighbor, insert_neighbor_list}, state) do
        neighbor_list = Map.get(state, "neighbor")
        neighbor_size = Map.get(state, "neighbor_size")
        neighbor_list = neighbor_list ++ insert_neighbor_list
        diff = length(neighbor_list) - neighbor_size
        if diff > 0 do
            Pastry.Utilies.pop_list(neighbor_list, diff)
        end     
        state = Map.update(state, "neighbor", fn x -> neighbor_list end)
        {:noreply, state}
    end

    def handle_call({:get_next_from_leaf, target_node_str}, _from, state) do
        leaf_map = Map.get(state, "leaf")
        target_node_int = Pastry.Utilies.id_to_number(target_node_str, Map.get(state, "arg_b"))
        self_node_id_int = Map.get(state, "node_id_int")
        matched_dest = 
            case target_node_int < self_node_id_int do
                true ->
                    check_same_pattern(Map.get(leaf_map, "small"), target_node_str)
                false ->
                    check_same_pattern(Map.get(leaf_map, "large"), target_node_str)
            end
        {:reply, matched_dest, state}
    end

    def check_same_pattern(tuple_list, target_node_str) do
        if length(tuple_list) > 0 do
            {first_tuple, rest_tuple_list} = List.pop_at(tuple_list, 0)
            case elem(first_tuple, 0) == target_node_str do
                true -> 
                    first_tuple
                false ->
                    check_same_pattern(rest_tuple_list, target_node_str)
            end
        else
            nil
        end
    end

    def handle_call({:get_next_from_routing, target_node_str, common_length}, _from, state) do
        routing_map = Map.get(state, "routing")
        case Map.has_key?(routing_map, common_length) do
            true ->
                routing_row = Map.get(routing_map, common_length)
                {common_prefix, rest} = String.split_at(target_node_str, common_length)
                {next_digit, rest} = String.split_at(rest, 1)
                next_digit = Integer.parse(next_digit)
                matched_dest = Enum.at(routing_row, next_digit)
            false ->
                matched_dest = nil
        end
        {:reply, matched_dest, state}
    end

    def handle_call({:get_next_from_all, target_node_str, common_length}, _from, state) do
        all_list = Map.values(Map.get(state, "leaf")) ++ Map.values(Map.get(state, "routing")) ++ Map.get(state, "neighbor")
        target_node_int = Pastry.Utilies.id_to_number(target_node_str, Map.get(state, "arg_b"))      
        self_node_int = Map.get(state, "self_node_int")
        matched_dest = find_closest_in_all(all_list, self_node_int, target_node_str, target_node_int, common_length)
        {:reply, matched_dest}
    end

    def find_closest_in_all(tuple_list, self_node_int, target_node_str, target_node_int, common_length) do
        if length(tuple_list) > 1 do
            {first_tuple, rest_tuple_list} = List.pop_at(tuple_list, 0)
            common_length_td = Pastry.Utilies.shl(elem(first_tuple, 0), target_node_str, 0)
            dist_td = abs(elem(first_tuple, 1) - target_node_int)
            dist_ad = abs(self_node_int - target_node_int)
            if common_length_td >= common_length && dist_td < dist_ad do
                first_tuple
            else
                find_closest_in_all(rest_tuple_list, self_node_int, target_node_str, target_node_int, common_length)
            end
        else
            List.first(tuple_list)
        end
    end

    def handle_call({:get_self_table}, _from, state) do
        self_node_str = Map.get(state, "node_id_str")
        self_node_int = Map.get(state, "node_id_int")
        self_node_pid = Map.get(state, "node_pid")
        self_node_map = %{"source_node" => {self_node_str, self_node_int, self_node_pid}}
        fetch_keys = ["leaf", "routing", "neighbor"]
        msg = Map.take(state, fetch_keys)
        msg = Map.merge(msg,self_node_map)
        {:reply, msg, state}
    end

    def handle_call({:get_recv_counter}, _from, state) do      
        {:reply, Map.get(state, "recv_counter"), state}
    end

    def handle_call({:get_send_counter}, _from, state) do
        {:reply, Map.get(state, "send_counter"), state}
    end

    def handle_call({:get_time_stamp}, _from, state) do
        {:reply, Map.get(state, "time_stamp"), state}
    end

    def handle_cast({:set_recv_counter, recv_num}, state) do
        state = Map.update!(state, "recv_counter", &(&1=recv_num))
        {:noreply, state}
    end

    def handle_cast({:set_send_counter, send_num}, state) do
        state = Map.update!(state, "send_counter", &(&1=send_num))
        {:noreply, state}
    end

    def handle_cast({:set_time_stamp, time_stamp}, state) do
        state = Map.update!(state, "time_stamp", &(&1=time_stamp))
        {:noreply, state}
    end

end