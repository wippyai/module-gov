local logger = require("logger")
local json = require("json")
local treesitter = require("treesitter")

-- Create a named logger for this linter
local log = logger:named("wippy.gov.linters.method")

-- Constants for operations and entry kinds
local CONST = {
    OPERATIONS = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    },
    KINDS = {
        FUNCTION = "function.lua"
    }
}

-- Function to extract method name from return statement using Tree-sitter
local function extract_method_from_source(source)
    if not source or source == "" then
        return nil, "Empty source code"
    end
    
    -- Parse the Lua code
    local tree, parse_err = treesitter.parse("lua", source)
    if not tree then
        return nil, "Parse error: " .. (parse_err or "Unknown parsing error")
    end
    
    -- If syntax errors, we can't reliably extract the method
    if tree:root_node():has_error() then
        tree:close()
        return nil, "Syntax errors in source code"
    end
    
    -- Find the last return statement in the file (the module export)
    -- Create a query to find all return statements
    local query_str = [[
        (return_statement) @return_stmt
    ]]
    
    local query, query_err = treesitter.query("lua", query_str)
    if not query then
        tree:close()
        return nil, "Query error: " .. (query_err or "Unknown query error")
    end
    
    -- Execute the query on the root node
    local matches = query:matches(tree:root_node(), source)
    
    -- No return statements found
    if #matches == 0 then
        query:close()
        tree:close()
        return nil, "No return statement found in source code"
    end
    
    -- Get the last return statement (module export)
    local last_match = matches[#matches]
    local last_return_node = nil
    
    for _, capture in ipairs(last_match.captures) do
        if capture.name == "return_stmt" then
            last_return_node = capture.node
            break
        end
    end
    
    if not last_return_node then
        query:close()
        tree:close()
        return nil, "Could not extract return statement node"
    end
    
    -- Now analyze the return value
    local return_value = nil
    
    -- Get the expression list child (what's being returned)
    local expr_list = nil
    for i = 0, last_return_node:named_child_count() - 1 do
        local child = last_return_node:named_child(i)
        if child:kind() == "expression_list" then
            expr_list = child
            break
        end
    end
    
    if not expr_list or expr_list:named_child_count() == 0 then
        query:close()
        tree:close()
        return nil, "Empty return statement"
    end
    
    -- Get the first expression in the list (what's being returned)
    local first_expr = expr_list:named_child(0)
    
    -- Case 1: Direct return of an identifier (e.g., "return handler")
    if first_expr:kind() == "identifier" then
        return_value = first_expr:text(source)
        query:close()
        tree:close()
        return return_value
    end
    
    -- Case 2: Return of a table constructor (e.g., "return { handler = handler }")
    if first_expr:kind() == "table_constructor" then
        -- If it's a table, look for the first field
        if first_expr:named_child_count() > 0 then
            -- Count the fields to see if there's only one
            local field_count = 0
            local field_name = nil
            
            for i = 0, first_expr:named_child_count() - 1 do
                local child = first_expr:named_child(i)
                if child:kind() == "field" then
                    field_count = field_count + 1
                    
                    -- Get the field name (key)
                    if field_count == 1 and child:named_child_count() >= 1 then
                        local name_node = child:named_child(0)
                        if name_node:kind() == "identifier" then
                            field_name = name_node:text(source)
                        end
                    end
                end
            end
            
            -- If there's only one field, use its name as the method
            if field_count == 1 and field_name then
                return_value = field_name
            end
        end
    end
    
    -- Clean up resources
    query:close()
    tree:close()
    
    -- Return the method name or error
    if return_value then
        return return_value
    else
        return nil, "Could not determine method from return statement"
    end
end

-- Main run function for the linter
local function run(args)
    log:info("Starting method linter for function.lua entries")
    
    -- Basic validation
    if not args or not args.changeset then
        return {
            success = false,
            message = "No changeset provided"
        }
    end
    
    -- Track validation issues
    local validation_issues = {}
    local modified_entries = 0
    
    -- Process each operation in the changeset
    for i, op in ipairs(args.changeset) do
        -- Only process function.lua entries
        if op.entry and op.entry.kind == CONST.KINDS.FUNCTION then
            -- Skip delete operations
            if op.kind == CONST.OPERATIONS.DELETE then
                goto continue
            end
            
            -- Check for entry data
            if not op.entry.data then
                table.insert(validation_issues, {
                    id = op.entry.id or "unknown",
                    type = "validation",
                    message = "Entry data is missing"
                })
                goto continue
            end
            
            -- Get the entry ID for logging
            local entry_id = op.entry.id or "unknown"
            
            -- Check if method is specified
            if not op.entry.data.method or op.entry.data.method == "" then
                log:info("Entry missing method: " .. entry_id)
                
                -- Try to extract method from source
                local source = op.entry.data.source
                local method, err = extract_method_from_source(source)
                
                if method then
                    -- Found a method, add it to the entry
                    log:info("Determined method '" .. method .. "' for entry: " .. entry_id)
                    op.entry.data.method = method
                    modified_entries = modified_entries + 1
                    
                    table.insert(validation_issues, {
                        id = entry_id,
                        type = "warning",
                        message = "Added missing method '" .. method .. "' based on source code analysis"
                    })
                else
                    -- Could not determine method, this is an error
                    table.insert(validation_issues, {
                        id = entry_id,
                        type = "error",
                        message = "Missing required 'method' field and could not determine it from source: " .. (err or "unknown error")
                    })
                end
            end
        end
        
        ::continue::
    end
    
    -- Check if we have any errors (not just warnings)
    local has_errors = false
    for _, issue in ipairs(validation_issues) do
        if issue.type == "error" then
            has_errors = true
            break
        end
    end
    
    -- If we have errors, return failure
    if has_errors then
        -- Create formatted error message
        local error_msg = "Method validation failed for function.lua entries:\n"
        for _, issue in ipairs(validation_issues) do
            if issue.type == "error" then
                error_msg = error_msg .. string.format("- Entry %s: %s\n",
                    issue.id,
                    issue.message
                )
            end
        end
        
        return {
            success = false,
            message = error_msg,
            details = validation_issues
        }
    end
    
    -- Return success with changeset and any warnings
    return {
        success = true,
        changeset = args.changeset,
        details = validation_issues,
        message = string.format("Method validation successful. Modified %d entries.", modified_entries)
    }
end

-- Export the run function
return { run = run }