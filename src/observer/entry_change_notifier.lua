local logger = require("logger")
local json = require("json")
local time = require("time")

-- Create a named logger for this listener
local log = logger:named("wippy.gov.observer.change_notifier")

-- Constants for entry types and topics
local CONST = {
    -- Central topic to publish to
    CENTRAL_TOPIC = "wippy.central",

    -- Operation types (matching registry operations)
    OPERATIONS = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    },

    -- Entity mappings - which kinds and meta.types to monitor
    ENTITY_MAPPINGS = {
        -- Map kinds to entry types
        KINDS = {
        },

        -- Map meta.types to entry types, todo: add custom registry type for custom entries
        META_TYPES = {
            ["view.page"] = "pages",
            ["agent.gen1"] = "agents",
            ["llm.model"] = "models",
        }
    }
}

-- Helper function to determine entry type from registry entry
local function get_entry_type(entry)
    if not entry then return nil end

    -- Check kind mapping first
    if entry.kind and CONST.ENTITY_MAPPINGS.KINDS[entry.kind] then
        return CONST.ENTITY_MAPPINGS.KINDS[entry.kind]
    end

    -- Check meta.type mapping
    if entry.meta and entry.meta.type and CONST.ENTITY_MAPPINGS.META_TYPES[entry.meta.type] then
        return CONST.ENTITY_MAPPINGS.META_TYPES[entry.meta.type]
    end

    -- No mapping found
    return nil
end

-- Helper function to publish entry change event
local function publish_entry_change(entry_type, entry_id)
    local time_now = time.now():format("2006-01-02 15:04:05")

    -- Send the event to wippy.central
    process.send(CONST.CENTRAL_TOPIC, entry_type, { id = entry_id })

    -- Also log a confirmation message
    log:info("Entity change notification sent", {
        topic = entry_type,
        id = entry_id,
        time = time_now
    })
end

-- Main listener function
local function run(args)
    log:info("Entry change notifier received notification")

    -- Validate arguments
    if not args or not args.changeset or not args.result then
        log:warn("Invalid arguments provided to entry change notifier")
        return {
            success = false,
            message = "Invalid arguments"
        }
    end

    local changeset = args.changeset
    local result = args.result
    local request_id = args.request_id or "unknown"

    -- Only process successful operations
    if not result.success then
        log:debug("Skipping notification for failed operation", {
            request_id = request_id,
            error = result.error or "Unknown error"
        })
        return {
            success = true
        }
    end

    log:info("Processing entry changes", {
        operations_count = #changeset,
        request_id = request_id
    })

    -- Process each operation
    for _, op in ipairs(changeset) do
        -- Handle different operation types
        if op.kind == CONST.OPERATIONS.CREATE and op.entry then
            local entry_type = get_entry_type(op.entry)
            if entry_type then
                publish_entry_change(entry_type, op.entry.id)
            end
        elseif op.kind == CONST.OPERATIONS.UPDATE and op.entry then
            local entry_type = get_entry_type(op.entry)
            if entry_type then
                publish_entry_change(entry_type, op.entry.id)
            end
        elseif op.kind == CONST.OPERATIONS.DELETE and op.entry then
            -- For delete operations, we might not have the full entry details
            -- but we should still have the entry ID
            local entry_type = get_entry_type(op.entry)
            if not entry_type then
                entry_type = "registry" -- Fallback to generic type
            end
            publish_entry_change(entry_type, op.entry.id)
        end
    end

    log:info("Entity change notification complete", {
        request_id = request_id,
        time = time.now():format("2006-01-02 15:04:05")
    })

    return {
        success = true
    }
end

return { run = run }