local logger = require("logger")
local json = require("json")

-- Create a named logger for this linter
local log = logger:named("wippy.gov.linters.kinds")

-- Constants for operations
local CONST = {
    OPERATIONS = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    }
}

-- List of all known entry kinds from the specification
local KNOWN_KINDS = {
    -- Filesystem Components
    ["fs.directory"] = true,
    ["cloudstorage.s3"] = true,

    -- HTTP Components
    ["http.service"] = true,
    ["http.router"] = true,
    ["http.endpoint"] = true,
    ["http.static"] = true,

    -- Security Components
    ["security.token_store"] = true,
    ["security.policy"] = true,

    -- Data Storage Components
    ["store.memory"] = true,
    ["db.sql.sqlite"] = true,
    ["db.sql.postgres"] = true,
    ["db.sql.mysql"] = true,

    -- Process Components
    ["process.host"] = true,
    ["process.service"] = true,

    -- Lua Components
    ["function.lua"] = true,
    ["library.lua"] = true,
    ["process.lua"] = true,
    ["workflow.lua"] = true,

    -- Template Components
    ["template.set"] = true,
    ["template.jet"] = true,

    -- Dynamic configs
    ["registry.entry"] = true,
    ["ns.definition"] = true,

    ["exec.native"] = true,
    ["process.host"] = true,

    ["contract.definition"] = true,
    ["contract.binding"] = true,
}

-- Check if a kind is known
local function is_known_kind(kind)
    return KNOWN_KINDS[kind] == true
end

-- Get related or similar kinds suggestions
local function get_similar_kinds(kind)
    if not kind then
        return {}
    end

    local similar = {}
    local prefix = kind:match("^([^.]+)%.")

    if prefix then
        -- Find related kinds with the same prefix
        for known_kind, _ in pairs(KNOWN_KINDS) do
            if known_kind:match("^" .. prefix .. "%.") then
                table.insert(similar, known_kind)
            end
        end
    end

    return similar
end

-- Main run function for the kind linter
local function run(args)
    log:info("Starting entry kind linter")

    -- Basic validation
    if not args or not args.changeset then
        return {
            success = false,
            message = "No changeset provided"
        }
    end

    -- Track validation issues
    local validation_issues = {}
    local invalid_entries = 0

    -- Process each operation in the changeset
    for _, op in ipairs(args.changeset) do
        -- Skip delete operations
        if op.kind == CONST.OPERATIONS.DELETE then
            goto continue
        end

        -- Check for entry data
        if not op.entry then
            table.insert(validation_issues, {
                id = "unknown",
                type = "validation",
                message = "Entry is missing"
            })
            goto continue
        end

        -- Get the entry ID and kind
        local entry_id = op.entry.id or "unknown"
        local entry_kind = op.entry.kind

        -- Log the entry being processed
        log:info("Checking entry: " .. entry_id .. " (kind: " .. (entry_kind or "nil") .. ")")

        -- Check if the kind is missing
        if not entry_kind or entry_kind == "" then
            table.insert(validation_issues, {
                id = entry_id,
                type = "error",
                message = "Entry kind is missing or empty"
            })
            invalid_entries = invalid_entries + 1
            goto continue
        end

        -- Check if the kind is known
        if not is_known_kind(entry_kind) then
            -- Get similar kinds for suggestions
            local similar_kinds = get_similar_kinds(entry_kind)
            local suggestions = ""

            if #similar_kinds > 0 then
                suggestions = " Did you mean one of: " .. json.encode(similar_kinds) .. "?"
            end

            table.insert(validation_issues, {
                id = entry_id,
                type = "error",
                message = "Unknown entry kind: '" .. entry_kind .. "'." .. suggestions,
                invalid_kind = entry_kind,
                similar_kinds = similar_kinds
            })
            invalid_entries = invalid_entries + 1
        end

        ::continue::
    end

    -- Return result with issues if any
    if invalid_entries > 0 then
        -- Create formatted error message
        local error_msg = "Entry kind validation failed with " .. invalid_entries .. " invalid entries:\n"
        for i, issue in ipairs(validation_issues) do
            error_msg = error_msg .. string.format("- Entry %s: %s\n",
                issue.id,
                issue.message
            )
        end

        return {
            success = false,
            message = error_msg,
            details = validation_issues
        }
    end

    -- If we reach here, all kinds are valid
    return {
        success = true,
        changeset = args.changeset,
        message = "Successfully validated all entry kinds"
    }
end

-- Export the run function
return { run = run }