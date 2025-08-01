local registry = require("registry")
local json = require("json")
local funcs = require("funcs")
local logger = require("logger")

-- Create a named logger for this module
local log = logger:named("wippy.gov.change.listener_registry")

-- Main module
local listener_registry = {}

-- Get all registered listeners
function listener_registry.get_listeners()
    -- Find all listeners from registry
    local entries = registry.find({
        ["meta.type"] = "registry.listener"
    })

    if not entries then
        log:warn("No registry listeners found")
        return {}
    end

    -- Sort listeners by priority (highest first)
    table.sort(entries, function(a, b)
        local a_priority = (a.meta and a.meta.priority) or 0
        local b_priority = (b.meta and b.meta.priority) or 0
        return a_priority < b_priority
    end)

    log:info("Loaded listeners", { count = #entries })
    return entries
end

-- Run all listeners with entire changeset - don't wait for or process responses
function listener_registry.run_listeners(changeset, result, request_id, user_id)
    log:debug("Running listeners for changeset", {
        count = #changeset,
        request_id = request_id,
        user_id = user_id
    })

    -- Get all listeners
    local listeners = listener_registry.get_listeners()
    if not listeners or #listeners == 0 then
        log:debug("No listeners found, skipping notification", {
            request_id = request_id,
            user_id = user_id
        })
        return
    end

    -- Create function executor
    local executor = funcs.new()

    -- Run each listener with entire batch
    for _, listener in ipairs(listeners) do
        log:debug("Running listener", {
            listener = listener.id,
            changeset_count = #changeset,
            request_id = request_id,
            user_id = user_id
        })

        -- Call the listener without waiting for response, including user_id
        local _, err = executor:call(listener.id, {
            changeset = changeset,
            result = result,
            request_id = request_id,
            user_id = user_id
        })

        if err ~= nil then
            log:error("Listener error", {
                listener = listener.id,
                error = err,
                request_id = request_id,
                user_id = user_id
            })
        end
    end
end

return listener_registry