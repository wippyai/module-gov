local registry = require("registry")
local json = require("json")
local loader = require("loader")
local env = require("env")

-- Main module
local uploader = {}

---------------------------
-- Constants
---------------------------

-- Operation types
uploader.OP = {
    CREATE = "entry.create",
    UPDATE = "entry.update",
    DELETE = "entry.delete"
}

---------------------------
-- Helper Functions
---------------------------

-- Debug print helper
local function debug_print(name, value)
    local status, result = pcall(function()
        if type(value) == "table" then
            return json.encode(value)
        else
            return tostring(value)
        end
    end)

    if status then
        print("[DEBUG] " .. name .. ": " .. result)
    else
        print("[DEBUG] " .. name .. ": <unable to serialize: " .. tostring(value) .. ">")
    end
end

-- Get the source directory from environment or options
local function get_source_directory(options)
    -- Return from options if provided
    if options and options.directory then
        return options.directory
    end

    -- Try to get from environment variable
    local app_src, err = env.get("APP_SRC")
    if not err and app_src and #app_src > 0 then
        return app_src
    end

    -- If no environment variable found, return error
    return nil, "Source directory not provided in options and APP_SRC environment variable not set"
end

-- Get the filesystem identifier from environment or options
local function get_filesystem_id(options)
    -- Return from options if provided
    if options and options.filesystem then
        return options.filesystem
    end

    -- Try to get from environment variable
    local app_fs, err = env.get("APP_FS")
    if not err and app_fs and #app_fs > 0 then
        return app_fs
    end

    -- If no environment variable found, return error
    return nil, "Filesystem ID not provided in options and APP_FS environment variable not set"
end

-- Format changeset operations for output
local function format_changeset(changeset)
    local formatted = {}
    for _, op in ipairs(changeset) do
        local formatted_op = {
            kind = op.kind, -- create, update, or delete
            entry = {
                id = op.entry.id,
                namespace = op.entry.namespace or (op.entry.id and registry.parse_id(op.entry.id).ns),
                name = op.entry.name or (op.entry.id and registry.parse_id(op.entry.id).name),
                kind = op.entry.kind,
                meta = op.entry.meta or {}
            }
        }

        -- Copy other fields from the entry
        for k, v in pairs(op.entry) do
            if k ~= "id" and k ~= "namespace" and k ~= "name" and k ~= "kind" and k ~= "meta" then
                formatted_op.entry[k] = v
            end
        end

        -- Handle large source content if present
        if formatted_op.entry.source and type(formatted_op.entry.source) == "string" and #formatted_op.entry.source > 1000 then
            formatted_op.entry.source = "... [source content omitted, length: " ..
                #formatted_op.entry.source .. " bytes]"
        end

        table.insert(formatted, formatted_op)
    end

    return formatted
end

---------------------------
-- Core Functions
---------------------------

-- Get current registry entries
function uploader.get_registry_entries()
    local snapshot, err = registry.snapshot()
    if not snapshot then
        return nil, "Failed to get registry snapshot: " .. tostring(err)
    end

    return snapshot:entries()
end

-- Get filesystem entries
function uploader.get_filesystem_entries(options)
    options = options or {}

    -- Get filesystem ID from environment or options
    local fs_id, fs_err = get_filesystem_id(options)
    if not fs_id then
        return nil, fs_err
    end

    -- Create a loader instance for the filesystem
    local loader_instance, err = loader.new(fs_id)
    if not loader_instance then
        return nil, "Failed to create loader for filesystem '" .. fs_id .. "': " .. tostring(err)
    end

    -- Get source directory from environment or options
    local directory, dir_err = get_source_directory(options)
    if not directory then
        return nil, dir_err
    end

    -- Use the loader instance to load entries from the specified directory
    local filesystemEntries, err = loader_instance:load_directory(directory, {})
    if not filesystemEntries then
        return nil, "Failed to load entries from directory '" .. directory .. "': " .. tostring(err)
    end

    return filesystemEntries
end

-- Compare registry entries with filesystem entries
function uploader.compare_entries(currentEntries, targetEntries)
    local changeset, err = registry.build_delta(currentEntries, targetEntries)
    if not changeset then
        return nil, "Failed to build delta: " .. tostring(err)
    end

    local formatted_changeset = format_changeset(changeset)

    return {
        changeset = changeset,
        formatted_changeset = formatted_changeset,
        count = #formatted_changeset,
        has_changes = #changeset > 0
    }
end

-- Check if there are differences between registry and filesystem
function uploader.has_changes(options)
    options = options or {}

    -- If this is a check immediately after upload, we know there are no changes
    if options.after_upload then
        return {
            success = true,
            has_changes = false,
            count = 0,
            changes = {}
        }
    end

    -- Get current registry entries
    local currentEntries, err = uploader.get_registry_entries()
    if not currentEntries then
        return {
            success = false,
            message = err
        }
    end

    -- Get filesystem entries
    local filesystemEntries, err = uploader.get_filesystem_entries(options)
    if not filesystemEntries then
        return {
            success = false,
            message = err
        }
    end

    -- Build delta between current registry entries and filesystem entries
    local comparison, err = uploader.compare_entries(currentEntries, filesystemEntries)
    if not comparison then
        return {
            success = false,
            message = err
        }
    end

    return {
        success = true,
        has_changes = comparison.has_changes,
        count = comparison.count,
        changes = comparison.formatted_changeset
    }
end

-- Upload entries from filesystem to registry - now only builds and returns the changeset
function uploader.upload(options)
    options = options or {}

    -- Get current registry entries
    local currentEntries, err = uploader.get_registry_entries()
    if not currentEntries then
        return {
            success = false,
            message = err
        }
    end

    -- Get filesystem entries
    local filesystemEntries, err = uploader.get_filesystem_entries(options)
    if not filesystemEntries then
        return {
            success = false,
            message = err
        }
    end

    -- Build delta between current registry entries and filesystem entries
    local comparison, err = uploader.compare_entries(currentEntries, filesystemEntries)
    if not comparison then
        return {
            success = false,
            message = err
        }
    end

    -- If no changes needed, return early
    if not comparison.has_changes then
        return {
            success = true,
            message = "No changes needed, filesystem and registry are in sync",
            changeset = comparison.formatted_changeset,
            count = 0,
            stats = {
                create = 0,
                update = 0,
                delete = 0
            }
        }
    end

    -- Calculate operation stats for reporting
    local stats = {
        create = 0,
        update = 0,
        delete = 0
    }

    for _, op in ipairs(comparison.changeset) do
        if op.kind == uploader.OP.CREATE then
            stats.create = stats.create + 1
        elseif op.kind == uploader.OP.UPDATE then
            stats.update = stats.update + 1
        elseif op.kind == uploader.OP.DELETE then
            stats.delete = stats.delete + 1
        end
    end

    -- Return the changeset without applying it
    return {
        success = true,
        message = "Successfully built changeset from filesystem",
        changeset = comparison.changeset,
        formatted_changeset = comparison.formatted_changeset,
        count = #comparison.formatted_changeset,
        stats = stats
    }
end

return uploader
