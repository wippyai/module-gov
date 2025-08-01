local logger = require("logger")
local json = require("json")

-- Create a named logger for this linter
local log = logger:named("wippy.gov.linters.empty_deps")

-- Constants for operations and entry kinds
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

-- Helper function to check if a table is empty
local function is_empty(t)
    if not t then
        return true
    end
    
    if type(t) ~= "table" then
        return false
    end
    
    return next(t) == nil and #t == 0
end

-- Main run function for the linter
local function run(args)
    log:info("Starting empty dependencies cleaner")
    
    -- Basic validation
    if not args or not args.changeset then
        return {
            success = false,
            message = "No changeset provided"
        }
    end
    
    -- Track changes made
    local cleaned_entries = {}
    
    -- Process each operation in the changeset
    for _, op in ipairs(args.changeset) do
        -- Check if this is a Lua code entry
        local is_lua_entry = op.entry and (
            op.entry.kind == CONST.KINDS.FUNCTION or
            op.entry.kind == CONST.KINDS.LIBRARY or
            op.entry.kind == CONST.KINDS.PROCESS or
            op.entry.kind == CONST.KINDS.WORKFLOW
        )
        
        if not is_lua_entry then
            goto continue
        end
        
        -- Skip delete operations
        if op.kind == CONST.OPERATIONS.DELETE then
            goto continue
        end
        
        -- Check for entry data
        if not op.entry.data then
            goto continue
        end
        
        local entry_id = op.entry.id or "unknown"
        local changes_made = false
        local changes = {}
        
        -- Check modules array
        if op.entry.data.modules and is_empty(op.entry.data.modules) then
            op.entry.data.modules = nil
            changes_made = true
            table.insert(changes, "modules")
            log:info("Removed empty modules array from " .. entry_id)
        end
        
        -- Check imports table
        if op.entry.data.imports and is_empty(op.entry.data.imports) then
            op.entry.data.imports = nil
            changes_made = true
            table.insert(changes, "imports")
            log:info("Removed empty imports table from " .. entry_id)
        end
        
        -- Record changes if any were made
        if changes_made then
            table.insert(cleaned_entries, {
                id = entry_id,
                changes = changes
            })
        end
        
        ::continue::
    end
    
    -- Return success with changeset and summary
    return {
        success = true,
        changeset = args.changeset,
        message = string.format("Cleaned empty dependencies in %d entries", #cleaned_entries),
        cleaned_entries = cleaned_entries
    }
end

-- Export the run function
return { run = run }