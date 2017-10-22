defmodule Pastry.Utilies do
    
    def get_node_id(suffix) do
        if String.length(suffix)<=6 do
            suffix = "0"<>suffix
            get_node_id(suffix)
        else
            suffix
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
end