from jsonschema import validate, ValidationError
from fastapi import HTTPException
from .tools import TOOLS_BY_NAME


def validate_tool_arguments(tool_name: str, arguments: dict) -> None:
    """Validate arguments against the tool's JSON schema. Raises HTTP 400 on failure."""
    tool = TOOLS_BY_NAME.get(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool '{tool_name}' not found")

    try:
        validate(instance=arguments, schema=tool["inputSchema"])
    except ValidationError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid arguments for '{tool_name}': {e.message}"
        )
