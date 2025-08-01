local logger = require("logger")
local treesitter = require("treesitter")

-- Create a named logger for this pre-processor
local log = logger:named("wippy.gov.lua.parse")

-- Constants for operation and entry types
local CONST = {
    OPERATIONS = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    },
    KINDS = {
        FUNCTION = "function.lua",
        LIBRARY = "library.lua",
        PROCESS = "process.lua",
        WORKFLOW = "workflow.lua"
    }
}

-- Main run function for pre-processing
local function run(args)
    log:info("Starting Lua code pre-processor")

    -- Basic validation
    if not args or not args.changeset then
        return {
            success = false,
            message = "No changeset provided"
        }
    end

    -- Track validation errors
    local validation_errors = {}

    -- Track required modules PER ENTRY with their actual require statement
    local requires_by_entry = {}

    -- Process each changeset operation
    for i, op in ipairs(args.changeset) do
        -- Check if this is a Lua code entry
        if op.entry and (op.entry.kind == CONST.KINDS.FUNCTION or
                op.entry.kind == CONST.KINDS.LIBRARY or
                op.entry.kind == CONST.KINDS.PROCESS or
                op.entry.kind == CONST.KINDS.WORKFLOW) then
            -- Skip delete operations
            if op.kind == CONST.OPERATIONS.DELETE then
                goto continue
            end

            -- Check for entry data
            if not op.entry.data then
                table.insert(validation_errors, {
                    id = op.entry.id or "unknown",
                    type = "validation",
                    message = "Entry data is missing"
                })
                goto continue
            end

            -- Get the source of the entry
            local source = op.entry.data.source

            -- Check if source is empty
            if not source or source == "" then
                table.insert(validation_errors, {
                    id = op.entry.id or "unknown",
                    type = "validation",
                    message = "Source cannot be empty"
                })
                goto continue
            end

            -- Validate Lua syntax using treesitter
            local tree, parse_err = treesitter.parse("lua", source)

            -- Check for parse errors
            if not tree then
                table.insert(validation_errors, {
                    id = op.entry.id or "unknown",
                    type = "validation",
                    message = "Parse error: " .. (parse_err or "Unknown parsing error")
                })
                goto continue
            end

            -- Check if the parsed tree has syntax errors
            if tree:root_node():has_error() then
                -- Create a cursor to find error nodes
                local cursor = tree:walk()
                local error_nodes = {}

                -- Function to find error nodes
                local function find_error_nodes(node)
                    if node:is_error() or node:has_error() then
                        table.insert(error_nodes, {
                            type = node:kind(),
                            start = node:start_point(),
                            text = node:text(source):sub(1, 20) .. "..."
                        })
                    end

                    for i = 0, node:child_count() - 1 do
                        find_error_nodes(node:child(i))
                    end
                end

                -- Find all error nodes
                find_error_nodes(tree:root_node())

                -- Create detailed error message
                local error_details = ""
                for _, err_node in ipairs(error_nodes) do
                    error_details = error_details .. string.format(
                        "\n  • Error at line %d, col %d: %s (near '%s')",
                        err_node.start.row + 1,
                        err_node.start.column + 1,
                        err_node.type,
                        err_node.text
                    )
                end

                tree:close()
                table.insert(validation_errors, {
                    id = op.entry.id or "unknown",
                    type = "validation",
                    message = "Lua syntax error detected:" .. error_details
                })
                goto continue
            end

            -- If we got this far, parsing was successful
            -- Initialize an entry-specific require map
            requires_by_entry[op.entry.id] = {}
            local entry_requires = requires_by_entry[op.entry.id]

            -- Function to find require statements
            local function find_requires(node)
                -- Check if this is a function call
                if node:kind() == "function_call" then
                    -- Get the function name node
                    local func_name = node:child(0)
                    if func_name and func_name:kind() == "identifier" and func_name:text(source) == "require" then
                        -- This is a require statement, get the full statement text
                        local statement_text = node:text(source)

                        -- Also extract the module name without quotes
                        local args = node:child(1)
                        if args and args:named_child_count() > 0 then
                            local arg = args:named_child(0)
                            if arg and arg:kind() == "string" then
                                -- Extract the module name without quotes
                                local module_text = arg:text(source)
                                -- Remove the quotes
                                module_text = module_text:sub(2, -2)
                                -- Add to this entry's requires if not already present
                                if not entry_requires[module_text] then
                                    entry_requires[module_text] = statement_text
                                end
                            end
                        end
                    end
                end

                -- Recursively check all children
                for i = 0, node:named_child_count() - 1 do
                    find_requires(node:named_child(i))
                end
            end

            -- Find all require statements in the tree
            find_requires(tree:root_node())

            -- Clean up
            tree:close()

            -- Log the requires found for this entry
            log:info("Found requires for entry: " .. op.entry.id, entry_requires)
        end

        ::continue::
    end

    -- Return cumulative errors if any validation failed
    if #validation_errors > 0 then
        -- Create formatted error message
        local error_msg = "Lua code validation failed:\n"
        for i, err in ipairs(validation_errors) do
            error_msg = error_msg .. string.format("- Entry %s: %s\n",
                err.id,
                err.message
            )
        end

        return {
            success = false,
            message = error_msg,
            details = validation_errors
        }
    end

    log:info("Done lua pre-preprocessing", requires_by_entry)

    -- Return success with changeset and collected requires PER ENTRY
    return {
        success = true,
        changeset = args.changeset,
        requires_by_entry = requires_by_entry,
        message = "Successfully processed and validated Lua sources"
    }
end

-- Export the run function
return { run = run }