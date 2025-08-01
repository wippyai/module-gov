local logger = require("logger")
local json = require("json")
local deps_lib = require("lua_dependency_lib")

-- Create a named logger for this dependency manager
local log = logger:named("wippy.gov.lua.deps")

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

-- Safe string replacement without pattern matching
local function plain_replace(source, find_text, replace_text)
    local result = ""
    local current_pos = 1
    local find_start, find_end = string.find(source, find_text, current_pos, true)

    while find_start do
        result = result .. string.sub(source, current_pos, find_start - 1) .. replace_text
        current_pos = find_end + 1
        find_start, find_end = string.find(source, find_text, current_pos, true)
    end

    result = result .. string.sub(source, current_pos)
    return result
end

-- Main run function for dependency management
local function run(args)
    log:info("Starting Lua dependency manager")

    -- Basic validation
    if not args or not args.changeset then
        return {
            success = false,
            message = "No changeset provided"
        }
    end

    -- Track validation issues and dependency changes
    local validation_issues = {}
    local dependency_changes = {}

    -- Check for requires_by_entry data from previous processor
    if not args.requires_by_entry then
        log:warn("No requires_by_entry data from pre-processor, using deprecated format if available")
        if args.requires then
            log:warn("Using deprecated flat requires format")
        end
    else
        log:info("Found requires_by_entry data from pre-processor")
    end

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
            table.insert(validation_issues, {
                id = op.entry.id or "unknown",
                type = "validation",
                message = "Entry data is missing"
            })
            goto continue
        end

        local entry_id = op.entry.id
        log:info("Processing entry: " .. entry_id)

        -- Track changes for this entry
        local entry_changes = {
            id = entry_id,
            added_modules = {},
            added_imports = {},
            removed_modules = {},
            source_transforms = {}
        }

        -- Extract the base namespace from the entry ID
        local base_namespace = deps_lib.extract_namespace(entry_id)

        -- Initialize modules if not present
        if not op.entry.data.modules then
            op.entry.data.modules = {}
        end

        -- Handle imports - must be either nil or a map (not an array)
        -- If we have no imports, set to nil explicitly
        if op.entry.data.imports == nil or
           (type(op.entry.data.imports) == "table" and not next(op.entry.data.imports)) then
            op.entry.data.imports = nil
        end

        -- Get entry-specific requires
        local entry_requires = nil
        if args.requires_by_entry and args.requires_by_entry[entry_id] then
            -- Use the entry-specific requires if available
            entry_requires = args.requires_by_entry[entry_id]
            log:info("Using entry-specific requires for: " .. entry_id)
        elseif args.requires then
            -- Fall back to global requires (for backward compatibility)
            entry_requires = args.requires
            log:info("Falling back to global requires for: " .. entry_id)
        end

        -- Skip if no requires data for this entry
        if not entry_requires or not next(entry_requires) then
            log:info("No requires found for entry: " .. entry_id)
            goto continue
        end

        -- Create lookup tables for declared dependencies
        local declared_modules = {}
        for _, module in ipairs(op.entry.data.modules) do
            declared_modules[module] = true
        end

        local declared_imports = {}
        if op.entry.data.imports then
            for alias, _ in pairs(op.entry.data.imports) do
                declared_imports[alias] = true
            end
        end

        -- Find missing dependencies
        local missing = deps_lib.find_missing_dependencies(
            entry_requires,  -- Use the entry-specific requires
            declared_modules,
            declared_imports,
            base_namespace
        )

        -- Add missing standard modules to module list
        for _, module in ipairs(missing.modules) do
            table.insert(op.entry.data.modules, module)
            table.insert(entry_changes.added_modules, module)
            log:info("Added missing module: " .. module)
        end

        -- If we have registry or local modules to add as imports, make sure imports is initialized
        if (#missing.registry > 0 or #missing.local_modules > 0) and not op.entry.data.imports then
            op.entry.data.imports = {}
        end

        -- Track module->alias mappings for source transformation
        local module_aliases = {}

        -- Process registry modules
        for _, module in ipairs(missing.registry) do
            local module_name = deps_lib.extract_module_name(module)

            -- First check if this module already exists in imports (by value)
            local existing_alias = nil
            if op.entry.data.imports then
                for a, m in pairs(op.entry.data.imports) do
                    if m == module then
                        existing_alias = a
                        break
                    end
                end
            end

            if existing_alias then
                -- Use the existing alias
                module_aliases[module] = existing_alias
                log:info("Using existing registry module import: " .. existing_alias .. " -> " .. module)
            else
                -- Generate a new alias
                local alias = module_name

                -- Handle alias conflicts
                local alias_counter = 1
                while op.entry.data.imports and op.entry.data.imports[alias] do
                    alias = module_name .. "_" .. alias_counter
                    alias_counter = alias_counter + 1
                end

                -- Add as import and track alias mapping
                if not op.entry.data.imports then
                    op.entry.data.imports = {}
                end
                op.entry.data.imports[alias] = module
                module_aliases[module] = alias

                -- Add to changes summary
                table.insert(entry_changes.added_imports, {
                    alias = alias,
                    module = module
                })

                log:info("Added registry module as import: " .. alias .. " -> " .. module)
            end
        end

        -- Process local namespace modules
        for _, module in ipairs(missing.local_modules) do
            local full_module = base_namespace .. ":" .. module

            -- First check if this module already exists in imports (by value)
            local existing_alias = nil
            if op.entry.data.imports then
                for a, m in pairs(op.entry.data.imports) do
                    if m == full_module then
                        existing_alias = a
                        break
                    end
                end
            end

            if existing_alias then
                -- Use the existing alias
                module_aliases[full_module] = existing_alias
                log:info("Using existing local module import: " .. existing_alias .. " -> " .. full_module)
            else
                -- Generate a new alias
                local alias = module

                -- Handle alias conflicts
                local alias_counter = 1
                while op.entry.data.imports and op.entry.data.imports[alias] do
                    alias = module .. "_" .. alias_counter
                    alias_counter = alias_counter + 1
                end

                -- Add as import and track alias mapping
                if not op.entry.data.imports then
                    op.entry.data.imports = {}
                end
                op.entry.data.imports[alias] = full_module
                module_aliases[full_module] = alias

                -- Add to changes summary
                table.insert(entry_changes.added_imports, {
                    alias = alias,
                    module = full_module
                })

                log:info("Added local module as import: " .. alias .. " -> " .. full_module)
            end
        end

        -- Transform source code to use aliases
        if next(module_aliases) and op.entry.data.source then
            local original_source = op.entry.data.source
            local transformed_source = original_source

            -- Transform each require statement to use the appropriate alias
            for module, require_stmt in pairs(entry_requires) do
                if module_aliases[module] then
                    local alias = module_aliases[module]
                    local new_require = string.format('require("%s")', alias)
                    transformed_source = plain_replace(transformed_source, require_stmt, new_require)

                    -- Record the transformation for the summary
                    table.insert(entry_changes.source_transforms, {
                        from = require_stmt,
                        to = new_require
                    })

                    log:info(string.format("Transformed require statement: %s -> %s",
                                          require_stmt, new_require))
                end
            end

            -- Only update if changes were made
            if transformed_source ~= original_source then
                op.entry.data.source = transformed_source
                entry_changes.source_transformed = true
                log:info("Transformed source code to use aliases")
            end
        end

        -- Add entry changes to summary if anything changed
        if #entry_changes.added_modules > 0 or
           #entry_changes.added_imports > 0 or
           #entry_changes.removed_modules > 0 or
           entry_changes.source_transformed then
            table.insert(dependency_changes, entry_changes)
        end

        -- Add appropriate warnings for added dependencies
        if #missing.modules > 0 then
            table.insert(validation_issues, {
                id = entry_id,
                type = "warning",
                message = "Added missing modules: " .. json.encode(missing.modules)
            })
        end

        if #missing.registry > 0 then
            table.insert(validation_issues, {
                id = entry_id,
                type = "warning",
                message = "Added registry modules as imports: " .. json.encode(missing.registry)
            })
        end

        if #missing.local_modules > 0 then
            table.insert(validation_issues, {
                id = entry_id,
                type = "warning",
                message = "Added local modules as imports: " .. json.encode(missing.local_modules)
            })
        end

        ::continue::
    end

    -- Return result with dependency changes summary
    return {
        success = true,
        changeset = args.changeset,
        details = validation_issues,
        resolved_dependencies = dependency_changes,
        message = "Successfully managed Lua dependencies with source transformation"
    }
end

-- Export the run function
return { run = run }