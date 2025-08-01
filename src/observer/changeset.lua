local registry = require("registry")
local json = require("json")
local time = require("time")
local logger = require("logger")

-- Create a named logger for this module
local log = logger:named("wippy.gov.observer")

-- Constants for operation types
local CONST = {
    OPERATIONS = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    }
}

-- Run function that will be called by the listener registry
local function run(args)
    log:debug("Changeset observer received notification")

    -- Validate arguments
    if not args or not args.changeset or not args.result then
        log:warn("Invalid arguments provided to changeset observer")
        return {
            success = false,
            message = "Invalid arguments"
        }
    end

    local changeset = args.changeset
    local result = args.result
    local request_id = args.request_id or "unknown"

    -- Create a summary of the operations
    local summary = {
        timestamp = time.now():unix(),
        request_id = request_id,
        version = result.version,
        operation_counts = {
            create = 0,
            update = 0,
            delete = 0,
            total = #changeset
        },
        namespaces = {}
    }

    -- Count operations by type and namespace
    for _, op in ipairs(changeset) do
        if op.kind == CONST.OPERATIONS.CREATE then
            summary.operation_counts.create = summary.operation_counts.create + 1
        elseif op.kind == CONST.OPERATIONS.UPDATE then
            summary.operation_counts.update = summary.operation_counts.update + 1
        elseif op.kind == CONST.OPERATIONS.DELETE then
            summary.operation_counts.delete = summary.operation_counts.delete + 1
        end

        -- Extract namespace from entry
        if op.entry and op.entry.id then
            local namespace = op.entry.id:match("(.+):.+")
            if namespace then
                if not summary.namespaces[namespace] then
                    summary.namespaces[namespace] = {
                        create = 0,
                        update = 0,
                        delete = 0
                    }
                end

                if op.kind == CONST.OPERATIONS.CREATE then
                    summary.namespaces[namespace].create = summary.namespaces[namespace].create + 1
                elseif op.kind == CONST.OPERATIONS.UPDATE then
                    summary.namespaces[namespace].update = summary.namespaces[namespace].update + 1
                elseif op.kind == CONST.OPERATIONS.DELETE then
                    summary.namespaces[namespace].delete = summary.namespaces[namespace].delete + 1
                end
            end
        end
    end

    -- Log the summary
    log:info("Registry change summary", summary)

    return {
        success = true
    }
end

return { run = run }