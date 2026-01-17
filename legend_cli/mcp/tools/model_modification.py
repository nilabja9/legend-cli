"""Model modification MCP tools for Legend CLI.

Provides tools for reading and modifying existing Pure entities
in SDLC workspaces.
"""

import json
import re
from typing import Any, Dict, List, Optional

from mcp.types import Tool

from ..context import MCPContext
from ..errors import SDLCError, EntityNotFoundError, ModificationError


def get_tools() -> List[Tool]:
    """Return all model modification tools."""
    return [
        Tool(
            name="read_entity",
            description="Read an existing Pure entity from SDLC workspace. Returns the entity content including Pure code.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "entity_path": {
                        "type": "string",
                        "description": "Full entity path (e.g., 'model::domain::Person')"
                    }
                },
                "required": ["project_id", "workspace_id", "entity_path"]
            }
        ),
        Tool(
            name="read_entities",
            description="List all entities in an SDLC workspace with their paths and types.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "filter_type": {
                        "type": "string",
                        "description": "Optional: filter by classifier type (e.g., 'Class', 'Mapping', 'Database')"
                    }
                },
                "required": ["project_id", "workspace_id"]
            }
        ),
        Tool(
            name="add_property",
            description="Add a new property to an existing Pure class.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "class_path": {
                        "type": "string",
                        "description": "Full class path (e.g., 'model::domain::Person')"
                    },
                    "property_name": {
                        "type": "string",
                        "description": "Name of the new property"
                    },
                    "property_type": {
                        "type": "string",
                        "description": "Pure type (e.g., 'String', 'Integer', 'Date', or custom class path)"
                    },
                    "multiplicity": {
                        "type": "string",
                        "description": "Multiplicity (e.g., '[1]', '[0..1]', '[*]')",
                        "default": "[1]"
                    },
                    "documentation": {
                        "type": "string",
                        "description": "Optional documentation for the property"
                    }
                },
                "required": ["project_id", "workspace_id", "class_path", "property_name", "property_type"]
            }
        ),
        Tool(
            name="remove_property",
            description="Remove a property from an existing Pure class.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "class_path": {
                        "type": "string",
                        "description": "Full class path"
                    },
                    "property_name": {
                        "type": "string",
                        "description": "Name of the property to remove"
                    }
                },
                "required": ["project_id", "workspace_id", "class_path", "property_name"]
            }
        ),
        Tool(
            name="create_class",
            description="Create a new Pure class in the workspace.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "class_path": {
                        "type": "string",
                        "description": "Full class path (e.g., 'model::domain::NewClass')"
                    },
                    "properties": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string"},
                                "multiplicity": {"type": "string", "default": "[1]"},
                                "doc": {"type": "string"}
                            },
                            "required": ["name", "type"]
                        },
                        "description": "List of properties to add to the class"
                    },
                    "extends": {
                        "type": "string",
                        "description": "Optional: parent class path for inheritance"
                    },
                    "documentation": {
                        "type": "string",
                        "description": "Optional: class documentation"
                    },
                    "commit_message": {
                        "type": "string",
                        "description": "Commit message",
                        "default": "Create class via MCP"
                    }
                },
                "required": ["project_id", "workspace_id", "class_path"]
            }
        ),
        Tool(
            name="create_association",
            description="Create a new Pure Association between two classes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "association_path": {
                        "type": "string",
                        "description": "Full association path (e.g., 'model::domain::Person_Address')"
                    },
                    "first_class": {
                        "type": "string",
                        "description": "First class path"
                    },
                    "first_property": {
                        "type": "string",
                        "description": "Property name on first class"
                    },
                    "first_multiplicity": {
                        "type": "string",
                        "description": "Multiplicity for first property",
                        "default": "[*]"
                    },
                    "second_class": {
                        "type": "string",
                        "description": "Second class path"
                    },
                    "second_property": {
                        "type": "string",
                        "description": "Property name on second class"
                    },
                    "second_multiplicity": {
                        "type": "string",
                        "description": "Multiplicity for second property",
                        "default": "[0..1]"
                    },
                    "commit_message": {
                        "type": "string",
                        "description": "Commit message",
                        "default": "Create association via MCP"
                    }
                },
                "required": ["project_id", "workspace_id", "association_path", "first_class", "first_property", "second_class", "second_property"]
            }
        ),
        Tool(
            name="create_function",
            description="Create a new Pure function or query.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "function_path": {
                        "type": "string",
                        "description": "Full function path (e.g., 'model::functions::getActiveUsers')"
                    },
                    "parameters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string"},
                                "multiplicity": {"type": "string", "default": "[1]"}
                            },
                            "required": ["name", "type"]
                        },
                        "description": "Function parameters"
                    },
                    "return_type": {
                        "type": "string",
                        "description": "Return type"
                    },
                    "return_multiplicity": {
                        "type": "string",
                        "description": "Return multiplicity",
                        "default": "[*]"
                    },
                    "body": {
                        "type": "string",
                        "description": "Function body (Pure expression)"
                    },
                    "commit_message": {
                        "type": "string",
                        "description": "Commit message",
                        "default": "Create function via MCP"
                    }
                },
                "required": ["project_id", "workspace_id", "function_path", "return_type", "body"]
            }
        ),
        Tool(
            name="delete_entity",
            description="Delete an entity from the SDLC workspace.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "entity_path": {
                        "type": "string",
                        "description": "Full entity path to delete"
                    }
                },
                "required": ["project_id", "workspace_id", "entity_path"]
            }
        ),
        Tool(
            name="update_entity",
            description="Update an existing entity's content.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "entity_path": {
                        "type": "string",
                        "description": "Full entity path"
                    },
                    "pure_code": {
                        "type": "string",
                        "description": "New Pure code for the entity"
                    },
                    "commit_message": {
                        "type": "string",
                        "description": "Commit message",
                        "default": "Update entity via MCP"
                    }
                },
                "required": ["project_id", "workspace_id", "entity_path", "pure_code"]
            }
        ),
    ]


async def read_entity(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    entity_path: str,
) -> str:
    """Read an entity from SDLC."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        with SDLCClient() as client:
            entity = client.get_entity(project_id, workspace_id, entity_path)

        # Update context
        ctx.set_sdlc_context(project_id, workspace_id)

        # Try to convert to Pure grammar for readability
        pure_code = None
        try:
            with EngineClient() as engine:
                model_data = {"_type": "data", "elements": [entity.get("content", {})]}
                pure_code = engine.json_to_grammar(model_data)
        except Exception:
            pass

        return json.dumps({
            "status": "success",
            "entity_path": entity_path,
            "classifier": entity.get("classifierPath"),
            "content": entity.get("content"),
            "pure_code": pure_code,
            "message": f"Retrieved entity: {entity_path}"
        })

    except Exception as e:
        if "404" in str(e):
            raise EntityNotFoundError(entity_path)
        raise SDLCError(f"Failed to read entity: {str(e)}")


async def read_entities(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    filter_type: Optional[str] = None,
) -> str:
    """List all entities in workspace."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            entities = client.list_entities(project_id, workspace_id)

        # Update context
        ctx.set_sdlc_context(project_id, workspace_id)

        # Filter by type if specified
        if filter_type:
            entities = [
                e for e in entities
                if filter_type.lower() in e.get("classifierPath", "").lower()
            ]

        # Group by type
        by_type: Dict[str, List[str]] = {}
        for e in entities:
            classifier = e.get("classifierPath", "unknown")
            # Extract short name from classifier
            short_type = classifier.split("::")[-1] if "::" in classifier else classifier
            if short_type not in by_type:
                by_type[short_type] = []
            by_type[short_type].append(e.get("path"))

        return json.dumps({
            "status": "success",
            "project_id": project_id,
            "workspace_id": workspace_id,
            "entities": [
                {
                    "path": e.get("path"),
                    "classifier": e.get("classifierPath"),
                }
                for e in entities
            ],
            "by_type": by_type,
            "count": len(entities),
            "message": f"Found {len(entities)} entities"
        })

    except Exception as e:
        raise SDLCError(f"Failed to read entities: {str(e)}")


async def add_property(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    class_path: str,
    property_name: str,
    property_type: str,
    multiplicity: str = "[1]",
    documentation: Optional[str] = None,
) -> str:
    """Add a property to an existing class."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        with SDLCClient() as client:
            entity = client.get_entity(project_id, workspace_id, class_path)

        content = entity.get("content", {})

        # Add the new property to the content
        if "properties" not in content:
            content["properties"] = []

        # Check if property already exists
        for prop in content.get("properties", []):
            if prop.get("name") == property_name:
                return json.dumps({
                    "status": "exists",
                    "class_path": class_path,
                    "property_name": property_name,
                    "message": f"Property '{property_name}' already exists in class"
                })

        # Parse multiplicity
        mult_match = re.match(r'\[(\d+|\*)(?:\.\.(\d+|\*))?\]', multiplicity)
        if mult_match:
            lower = 0 if mult_match.group(1) == '*' else int(mult_match.group(1))
            upper_str = mult_match.group(2) if mult_match.group(2) else mult_match.group(1)
            upper = None if upper_str == '*' else int(upper_str)
        else:
            lower, upper = 1, 1

        # Build new property
        new_property = {
            "name": property_name,
            "multiplicity": {"lowerBound": lower},
            "genericType": {
                "rawType": {
                    "_type": "packageableType",
                    "fullPath": property_type if "::" in property_type else property_type
                }
            }
        }
        if upper is not None:
            new_property["multiplicity"]["upperBound"] = upper

        if documentation:
            new_property["taggedValues"] = [{
                "tag": {"profile": "meta::pure::profiles::doc", "value": "doc"},
                "value": documentation
            }]

        content["properties"].append(new_property)

        # Update entity
        with SDLCClient() as client:
            entities = [{
                "path": class_path,
                "classifierPath": entity.get("classifierPath"),
                "content": content
            }]
            client.update_entities(
                project_id, workspace_id, entities,
                message=f"Add property {property_name} to {class_path}"
            )

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "class_path": class_path,
            "property_name": property_name,
            "property_type": property_type,
            "multiplicity": multiplicity,
            "message": f"Added property '{property_name}' to class '{class_path}'"
        })

    except Exception as e:
        if "404" in str(e):
            raise EntityNotFoundError(class_path)
        raise ModificationError(f"Failed to add property: {str(e)}")


async def remove_property(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    class_path: str,
    property_name: str,
) -> str:
    """Remove a property from a class."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            entity = client.get_entity(project_id, workspace_id, class_path)

        content = entity.get("content", {})
        properties = content.get("properties", [])

        # Find and remove the property
        original_count = len(properties)
        content["properties"] = [p for p in properties if p.get("name") != property_name]

        if len(content["properties"]) == original_count:
            return json.dumps({
                "status": "not_found",
                "class_path": class_path,
                "property_name": property_name,
                "message": f"Property '{property_name}' not found in class"
            })

        # Update entity
        with SDLCClient() as client:
            entities = [{
                "path": class_path,
                "classifierPath": entity.get("classifierPath"),
                "content": content
            }]
            client.update_entities(
                project_id, workspace_id, entities,
                message=f"Remove property {property_name} from {class_path}"
            )

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "class_path": class_path,
            "property_name": property_name,
            "message": f"Removed property '{property_name}' from class '{class_path}'"
        })

    except Exception as e:
        if "404" in str(e):
            raise EntityNotFoundError(class_path)
        raise ModificationError(f"Failed to remove property: {str(e)}")


async def create_class(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    class_path: str,
    properties: Optional[List[Dict[str, Any]]] = None,
    extends: Optional[str] = None,
    documentation: Optional[str] = None,
    commit_message: str = "Create class via MCP",
) -> str:
    """Create a new Pure class."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        # Parse class path
        parts = class_path.rsplit("::", 1)
        package = parts[0] if len(parts) > 1 else ""
        class_name = parts[-1]

        # Build Pure code
        lines = []

        if documentation:
            escaped_doc = documentation.replace("'", "\\'")
            lines.append(f"Class {{meta::pure::profiles::doc.doc = '{escaped_doc}'}} {class_path}")
        else:
            lines.append(f"Class {class_path}")

        if extends:
            lines[-1] += f" extends {extends}"

        lines.append("{")

        if properties:
            for prop in properties:
                prop_name = prop.get("name")
                prop_type = prop.get("type")
                multiplicity = prop.get("multiplicity", "[1]")
                prop_doc = prop.get("doc")

                if prop_doc:
                    escaped_prop_doc = prop_doc.replace("'", "\\'")
                    lines.append(f"  {{meta::pure::profiles::doc.doc = '{escaped_prop_doc}'}} {prop_name}: {prop_type}{multiplicity};")
                else:
                    lines.append(f"  {prop_name}: {prop_type}{multiplicity};")

        lines.append("}")
        pure_code = "\n".join(lines)

        # Parse and push
        with EngineClient() as engine:
            entities = engine.parse_pure_code(pure_code)

        with SDLCClient() as client:
            client.update_entities(project_id, workspace_id, entities, message=commit_message)

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "class_path": class_path,
            "pure_code": pure_code,
            "property_count": len(properties) if properties else 0,
            "message": f"Created class '{class_path}'"
        })

    except Exception as e:
        raise ModificationError(f"Failed to create class: {str(e)}")


async def create_association(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    association_path: str,
    first_class: str,
    first_property: str,
    second_class: str,
    second_property: str,
    first_multiplicity: str = "[*]",
    second_multiplicity: str = "[0..1]",
    commit_message: str = "Create association via MCP",
) -> str:
    """Create a new Pure Association."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        # Build Pure code
        pure_code = f"""Association {association_path}
{{
  {first_property}: {first_class}{first_multiplicity};
  {second_property}: {second_class}{second_multiplicity};
}}"""

        # Parse and push
        with EngineClient() as engine:
            entities = engine.parse_pure_code(pure_code)

        with SDLCClient() as client:
            client.update_entities(project_id, workspace_id, entities, message=commit_message)

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "association_path": association_path,
            "first_class": first_class,
            "second_class": second_class,
            "pure_code": pure_code,
            "message": f"Created association '{association_path}'"
        })

    except Exception as e:
        raise ModificationError(f"Failed to create association: {str(e)}")


async def create_function(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    function_path: str,
    return_type: str,
    body: str,
    parameters: Optional[List[Dict[str, Any]]] = None,
    return_multiplicity: str = "[*]",
    commit_message: str = "Create function via MCP",
) -> str:
    """Create a new Pure function."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        # Build parameter string
        param_str = ""
        if parameters:
            param_parts = []
            for param in parameters:
                p_name = param.get("name")
                p_type = param.get("type")
                p_mult = param.get("multiplicity", "[1]")
                param_parts.append(f"{p_name}: {p_type}{p_mult}")
            param_str = ", ".join(param_parts)

        # Build Pure code
        pure_code = f"""function {function_path}({param_str}): {return_type}{return_multiplicity}
{{
  {body}
}}"""

        # Parse and push
        with EngineClient() as engine:
            entities = engine.parse_pure_code(pure_code)

        with SDLCClient() as client:
            client.update_entities(project_id, workspace_id, entities, message=commit_message)

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "function_path": function_path,
            "pure_code": pure_code,
            "message": f"Created function '{function_path}'"
        })

    except Exception as e:
        raise ModificationError(f"Failed to create function: {str(e)}")


async def delete_entity(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    entity_path: str,
) -> str:
    """Delete an entity from SDLC."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            client.delete_entity(project_id, workspace_id, entity_path)

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "entity_path": entity_path,
            "message": f"Deleted entity '{entity_path}'"
        })

    except Exception as e:
        if "404" in str(e):
            raise EntityNotFoundError(entity_path)
        raise SDLCError(f"Failed to delete entity: {str(e)}")


async def update_entity(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    entity_path: str,
    pure_code: str,
    commit_message: str = "Update entity via MCP",
) -> str:
    """Update an existing entity."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        # Parse Pure code
        with EngineClient() as engine:
            entities = engine.parse_pure_code(pure_code)

        if not entities:
            raise ModificationError("No valid entities found in Pure code")

        # Find the entity matching the path
        matching_entity = None
        for e in entities:
            if e.get("path") == entity_path:
                matching_entity = e
                break

        if not matching_entity:
            # Use the first entity but update its path
            matching_entity = entities[0]
            matching_entity["path"] = entity_path

        # Push to SDLC
        with SDLCClient() as client:
            client.update_entities(project_id, workspace_id, [matching_entity], message=commit_message)

        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "entity_path": entity_path,
            "classifier": matching_entity.get("classifierPath"),
            "message": f"Updated entity '{entity_path}'"
        })

    except Exception as e:
        raise ModificationError(f"Failed to update entity: {str(e)}")
