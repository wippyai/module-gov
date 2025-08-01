local registry = require("registry")
local json = require("json")
local funcs = require("funcs")
local logger = require("logger")

-- Create a named logger for this module
local log = logger:named("wippy.gov.change.processor_registry")

-- Main module
local processor_registry = {}

-- Get all registered processors
function processor_registry.get_processors()
    -- Find all processors from registry
    local entries = registry.find({
        ["meta.type"] = "registry.processor"
    })

    if not entries then
        log:warn("No registry processors found")
        return {}
    end

    -- Sort processors by priority (lowest first)
    table.sort(entries, function(a, b)
        local a_priority = (a.meta and a.meta.priority) or 0
        local b_priority = (b.meta and b.meta.priority) or 0
        return a_priority < b_priority
    end)

    log:info("Loaded processors", { count = #entries })
    return entries
end

-- Process entire changeset with context preservation
function processor_registry.process_changeset(changeset, request_id, options, user_id)
    log:debug("Processing changeset", {
        count = #changeset,
        request_id = request_id,
        user_id = user_id,
        has_options = options ~= nil
    })

    -- Get all processors
    local processors = processor_registry.get_processors()
    if not processors or #processors == 0 then
        log:debug("No processors found, skipping processing", {
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = true,
            changeset = changeset,
            options = options, -- preserve options including token/ticket
            user_id = user_id, -- preserve user_id
            request_id = request_id,
            details = {}
        }
    end

    -- Create function executor
    local executor = funcs.new()

    -- Initialize context with all input parameters including user_id
    local context = {
        changeset = changeset,
        request_id = request_id,
        user_id = user_id,
        options = options
    }

    -- Initialize result structure with success flag and empty details array
    local result = {
        success = true,
        changeset = changeset,
        options = options, -- preserve options including token/ticket
        user_id = user_id, -- preserve user_id
        request_id = request_id,
        details = {}
    }

    for _, processor in ipairs(processors) do
        log:debug("Applying processor", {
            processor = processor.id,
            changeset_count = #context.changeset,
            request_id = context.request_id,
            user_id = context.user_id
        })

        -- Call the processor with the complete context object
        local proc_result, err = executor:call(processor.id, context)

        if err then
            log:error("Processor execution failed", {
                processor = processor.id,
                error = err,
                request_id = context.request_id,
                user_id = context.user_id
            })

            return {
                success = false,
                changeset = context.changeset,
                options = context.options,
                user_id = context.user_id,
                error = err,
                message = "Processor execution failed: " .. processor.id,
                request_id = context.request_id,
                details = result.details
            }
        end

        -- Processor can return nil to indicate no changes
        if proc_result then
            -- Accumulate details if provided by the processor
            if proc_result.details and #proc_result.details > 0 then
                for _, detail in ipairs(proc_result.details) do
                    table.insert(result.details, detail)
                end
            end

            if not proc_result.success then
                log:warn("Processor reported failure", {
                    processor = processor.id,
                    message = proc_result.message or "Unknown error",
                    request_id = context.request_id,
                    user_id = context.user_id
                })

                return {
                    success = false,
                    changeset = proc_result.changeset or context.changeset,
                    options = proc_result.options or context.options, -- preserve options including token/ticket
                    user_id = proc_result.user_id or context.user_id, -- preserve user_id
                    message = proc_result.message or "Processing failed",
                    request_id = context.request_id,
                    details = result.details
                }
            end

            -- Update context and result with processor result fields
            for k, v in pairs(proc_result) do
                if k ~= "success" and k ~= "message" then
                    context[k] = v
                    result[k] = v
                end
            end

            -- restore if options or user_id were modified
            context.options = options
            context.user_id = user_id
        end
    end

    return result
end

return processor_registry