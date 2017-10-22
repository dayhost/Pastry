defmodule Pastry.Utilies do

    def get_node_id(node_num, arg_b) do
        base = Kernel.round(:math.pow(2, arg_b))
        id = convert_num_to_based_str(base, "", node_num)
        add_prefix(id)
    end

    defp add_prefix(id) do
        if String.length(id)<8 do
            id = "0"<>id
        else
            id
        end
    end

    defp convert_num_to_based_str(base, prefix, rem) do
        if(Kernel.div(rem, base)==0) do
            prefix<>Integer.to_string(rem)
        else
            result = Kernel.div(rem, base)
            rem = Kernel.rem(rem, base)
            prefix = prefix<>Integer.to_string(rem)
            convert_num_to_based_str(base, prefix, result)
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
        end
        match_predix - 1
    end

    def flat_routing_table(routing_dict) do
        routing_list = Map.values(routing_dict)
        Enum.flat_map(routing_list, fn x -> x end)
    end
end