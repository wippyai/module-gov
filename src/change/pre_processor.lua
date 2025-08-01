local registry = require("registry")
local json = require("json")
local time = require("time")
local logger = require("logger")
local processor_registry = require("processor_registry")

-- Create a named logger for this worker
local log = logger:named("wippy.gov.change.pre")

-- Constants
local CONST = {
    OPERATIONS = {
        APPLY_CHANGES = "apply_changes",
        APPLY_VERSION = "apply_version"
    },
    OP = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    }
}

-- Validate changeset
local function validate_changeset(changeset)
    log:debug("Validating changeset", { count = #changeset })

    local details = {}

    if not changeset or #changeset == 0 then
        table.insert(details, {
            id = "item:0",
            type = "validation",
            message = "No changeset items provided"
        })
        return nil, "No changeset items provided", details
    end

    local invalid_count = 0

    for i, item in ipairs(changeset) do
        if not item.kind or not item.entry then
            invalid_count = invalid_count + 1
            table.insert(details, {
                id = "item:" .. i,
                type = "validation",
                message = "Missing kind or entry field"
            })
        elseif item.kind ~= CONST.OP.CREATE and
            item.kind ~= CONST.OP.UPDATE and
            item.kind ~= CONST.OP.DELETE then
            invalid_count = invalid_count + 1

            -- Use entry ID if available, otherwise index
            local detail_id = (item.entry and item.entry.id) or ("item:" .. i)
            table.insert(details, {
                id = detail_id,
                type = "validation",
                message = "Invalid operation kind: " .. tostring(item.kind)
            })
        elseif item.kind == CONST.OP.DELETE and not item.entry.id then
            invalid_count = invalid_count + 1
            table.insert(details, {
                id = "item:" .. i,
                type = "validation",
                message = "Delete operation missing ID"
            })
        end
    end

    -- If all items are invalid, return error
    if invalid_count == #changeset then
        return nil, "No valid changeset items provided", details
    end

    -- Return the original changeset and details
    return changeset, nil, details
end

-- Validate version ID
local function validate_version_id(version_id)
    log:debug("Validating version ID", { version_id = version_id })

    local details = {}

    if not version_id or type(version_id) ~= "string" then
        table.insert(details, {
            id = "version:input",
            type = "validation",
            message = "Invalid version_id provided"
        })
        return nil, "Invalid version_id provided", details
    end

    -- Check if the version exists in the registry
    local history = registry.history()
    if not history then
        table.insert(details, {
            id = "version:history",
            type = "validation",
            message = "Failed to access registry history"
        })
        return nil, "Failed to access registry history", details
    end

    local version, err = history:get_version(version_id)
    if not version then
        table.insert(details, {
            id = "version:" .. version_id,
            type = "validation",
            message = "Version not found: " .. version_id
        })
        return nil, "Version not found: " .. version_id, details
    end

    return version_id, nil, details
end

-- Main run function for pre-processing
local function run(args)
    log:info("Starting pre-processing phase")

    -- Initialize details array
    local details = {}

    -- Validate arguments
    if not args then
        log:error("No arguments provided")
        return {
            success = false,
            message = "No arguments provided",
            error = "Missing required arguments",
            details = details,
            request_id = nil
        }
    end

    local request_id = args.request_id
    local user_id = args.user_id

    log:info("Processing request", {
        request_id = request_id,
        user_id = user_id,
        has_changeset = args.changeset ~= nil,
        has_version_id = args.version_id ~= nil,
        has_options = args.options ~= nil
    })

    -- Determine which operation to pre-process
    if args.changeset then
        -- Validate the changeset
        local validated_changeset, err, validation_details = validate_changeset(args.changeset)

        -- Aggregate validation details
        if validation_details and #validation_details > 0 then
            for _, detail in ipairs(validation_details) do
                table.insert(details, detail)
            end
        end

        if not validated_changeset then
            log:error("Failed to validate changeset", {
                error = err,
                request_id = request_id,
                user_id = user_id
            })
            return {
                success = false,
                message = "Failed to validate changeset",
                error = err,
                details = details,
                options = args.options, -- Preserve options
                user_id = user_id, -- Preserve user_id
                request_id = request_id
            }
        end

        -- Apply processors to changeset, passing options for authentication and user_id
        local proc_result = processor_registry.process_changeset(validated_changeset, request_id, args.options, user_id)

        if not proc_result.success then
            log:error("Processing changeset failed", {
                error = proc_result.error,
                request_id = request_id,
                user_id = user_id
            })

            -- Make sure to return options and user_id in error case
            return {
                success = false,
                message = proc_result.message or "Changeset processing failed",
                details = proc_result.details,
                options = proc_result.options or args.options, -- Preserve options
                user_id = user_id, -- Preserve user_id
                request_id = request_id
            }
        end

        -- Return success with processed changeset and all details
        return {
            success = true,
            message = "Changeset validated and processed successfully",
            changeset = proc_result.changeset,
            options = proc_result.options or args.options, -- Use processor options or fallback
            user_id = user_id, -- Preserve user_id
            details = proc_result.details,
            request_id = request_id
        }
    elseif args.version_id then
        -- Validate version ID
        local validated_version, err, validation_details = validate_version_id(args.version_id)

        if not validated_version then
            log:error("Failed to validate version ID", {
                error = err,
                request_id = request_id,
                user_id = user_id
            })
            return {
                success = false,
                message = "Failed to validate version ID",
                error = err,
                details = validation_details or {},
                options = args.options, -- Preserve options
                user_id = user_id, -- Preserve user_id
                request_id = request_id
            }
        end

        -- Return success with validated version ID and details
        return {
            success = true,
            message = "Version ID validated successfully",
            version_id = validated_version,
            options = args.options, -- Preserve options
            user_id = user_id, -- Preserve user_id
            details = validation_details or {},
            request_id = request_id
        }
    else
        log:error("Invalid arguments - requires changeset or version_id", {
            request_id = request_id,
            user_id = user_id
        })

        -- Add an error detail
        table.insert(details, {
            id = "request:" .. (request_id or "unknown"),
            type = "validation",
            message = "Must provide either changeset or version_id"
        })

        return {
            success = false,
            message = "Invalid arguments provided",
            error = "Must provide either changeset or version_id",
            details = details,
            options = args.options, -- Preserve options
            user_id = user_id, -- Preserve user_id
            request_id = request_id
        }
    end
end

-- Export the run function
return { run = run }