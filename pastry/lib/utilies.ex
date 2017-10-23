defmodule Pastry.Utilies do

    def get_node_id(node_num, arg_b) do
        base = Kernel.round(:math.pow(2, arg_b))
        id = convert_num_to_based_str(base, "", node_num)
        add_prefix(id)
    end

    defp add_prefix(id) do
        if String.length(id)<8 do
            id = "0"<>id
            add_prefix(id)
        else
            id
        end
    end

    defp convert_num_to_based_str(base, suffix, rem) do
        if(Kernel.div(rem, base)==0) do
            Integer.to_string(rem)<>suffix
        else
            result = Kernel.div(rem, base)
            rem = Kernel.rem(rem, base)
            suffix = Integer.to_string(rem)<>suffix
            convert_num_to_based_str(base, suffix, result)
        end
    end

    def shl(template, target, match_predix) do
        """
        Args:
            template: the source string 
            target: the input string in which find the longest prefix
            match_predix: current length of common prefix
        """
        prefix = String.slice(template, 0, match_predix)
        if String.starts_with?(target, prefix) do
            shl(template, target, match_predix+1)
        else
            match_predix - 1
        end 
    end

    def flat_nested_dict(nested_dict) do
        routing_list = Map.values(nested_dict)
        Enum.flat_map(routing_list, fn x -> x end)
    end

    def node_id_diff(id1, id2, arg_b) do
        num1 = id_to_number(id1, arg_b)
        num2 = id_to_number(id2, arg_b)
        Kernel.abs(num1-num2)
    end

    def id_to_number(id, arg_b) do
        re_id = String.reverse(id)
        base = Kernel.round(:math.pow(2, arg_b))
        id_length = String.length(id)
        get_num_from_str(re_id, base, id_length, 0)
    end

    defp get_num_from_str(re_id, base, id_length, sum) do
        if(String.length(re_id)!=0) do
            pos = id_length - String.length(re_id)
            {char_at, re_id} = String.next_grapheme(re_id)
            num_at = String.to_integer(char_at)
            sum = sum + num_at * Kernel.round(:math.pow(base, pos))
            get_num_from_str(re_id, base, id_length, sum)
        else
            sum
        end
    end

    def pop_list(current_list, diff) do
        if diff > 0 do
            list_size = length(current_list)
            random_number = :rand.uniform(list_size) - 1
            {_, current_list} = List.pop_at(current_list, random_number)
            pop_list(current_list, diff-1)
        else
            current_list
        end   
    end
end