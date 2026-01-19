"""Tests for codeError handling in Engine client and MCP tools."""

import pytest
from unittest.mock import patch, MagicMock

from legend_cli.mcp.errors import EngineParseError
from legend_cli.engine_client import EngineClient
from tests.mcp.fixtures.mock_engine import (
    create_mock_engine_with_code_error,
    create_mock_engine_with_invalid_syntax,
    MockEngineClientWithCodeError,
)


class TestEngineParseError:
    """Tests for the EngineParseError exception class."""

    def test_basic_creation(self):
        """Test basic EngineParseError creation."""
        error = EngineParseError(
            message="Unexpected token",
            source_info={"startLine": 5, "startColumn": 10, "endLine": 5, "endColumn": 20},
        )

        assert error.message == "Unexpected token"
        assert error.code == "ENGINE_PARSE_ERROR"
        assert error.source_info["startLine"] == 5

    def test_formatted_location_single_line(self):
        """Test location formatting for single-line errors."""
        error = EngineParseError(
            message="Error",
            source_info={"startLine": 5, "startColumn": 10, "endLine": 5, "endColumn": 20},
        )

        location = error.get_formatted_location()
        assert location == "Line 5, columns 10-20"

    def test_formatted_location_multi_line(self):
        """Test location formatting for multi-line errors."""
        error = EngineParseError(
            message="Error",
            source_info={"startLine": 5, "startColumn": 1, "endLine": 10, "endColumn": 15},
        )

        location = error.get_formatted_location()
        assert location == "Lines 5-10"

    def test_formatted_location_empty_source_info(self):
        """Test location formatting with no source info."""
        error = EngineParseError(message="Error")
        location = error.get_formatted_location()
        assert location == ""

    def test_user_friendly_message_with_location(self):
        """Test user-friendly message includes location."""
        error = EngineParseError(
            message="Unexpected token",
            source_info={"startLine": 5, "startColumn": 10, "endLine": 5, "endColumn": 20},
        )

        message = error.get_user_friendly_message()
        assert "Unexpected token" in message
        assert "Line 5, columns 10-20" in message

    def test_user_friendly_message_without_location(self):
        """Test user-friendly message without location."""
        error = EngineParseError(message="Unknown error")
        message = error.get_user_friendly_message()
        assert message == "Unknown error"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        error = EngineParseError(
            message="Error",
            source_info={"startLine": 5},
            raw_error={"original": "data"},
        )

        result = error.to_dict()
        assert result["code"] == "ENGINE_PARSE_ERROR"
        assert result["message"] == "Error"
        assert "source_info" in result["details"]
        assert "raw_error" in result["details"]


class TestMockEngineWithCodeError:
    """Tests for the MockEngineClient with codeError support."""

    def test_default_code_error(self):
        """Test mock returns default codeError."""
        mock = create_mock_engine_with_code_error(
            error_message="Test error",
            start_line=1,
            start_column=1,
            end_line=1,
            end_column=10,
        )

        result = mock.grammar_to_json("any code")
        assert "codeError" in result
        assert result["codeError"]["message"] == "Test error"
        assert result["codeError"]["sourceInformation"]["startLine"] == 1

    def test_pattern_based_code_error(self):
        """Test mock returns codeError for specific patterns."""
        mock = create_mock_engine_with_invalid_syntax()

        # Should trigger codeError for INVALID keyword
        result = mock.grammar_to_json("Class Test { INVALID }")
        assert "codeError" in result
        assert "INVALID" in result["codeError"]["message"]

    def test_valid_code_returns_entities(self):
        """Test mock returns entities for valid code."""
        mock = create_mock_engine_with_invalid_syntax()

        # Should return entities for valid code (no INVALID keyword)
        result = mock.grammar_to_json("Class Test { id: Integer[1]; }")
        assert "codeError" not in result or result.get("codeError") is None
        assert "modelDataContext" in result


class TestEngineClientCodeErrorDetection:
    """Tests for codeError detection in EngineClient."""

    def test_grammar_to_json_raises_on_code_error(self, mock_engine_code_error_response):
        """Test that grammar_to_json raises EngineParseError on codeError."""
        with patch.object(EngineClient, "grammar_to_json") as mock_method:
            # Make the mock raise EngineParseError when called
            mock_method.side_effect = EngineParseError(
                message=mock_engine_code_error_response["codeError"]["message"],
                source_info=mock_engine_code_error_response["codeError"]["sourceInformation"],
                raw_error=mock_engine_code_error_response["codeError"],
            )

            client = EngineClient()
            with pytest.raises(EngineParseError) as exc_info:
                client.grammar_to_json("invalid code")

            assert "unexpected token" in exc_info.value.message.lower()
            assert exc_info.value.source_info["startLine"] == 10

    def test_extract_entities_raises_on_code_error(self, mock_engine_code_error_response):
        """Test that extract_entities raises EngineParseError if codeError present."""
        client = EngineClient()

        with pytest.raises(EngineParseError) as exc_info:
            client.extract_entities(mock_engine_code_error_response)

        assert exc_info.value.source_info["startLine"] == 10

    def test_debug_parse_response_captures_code_error(self, mock_engine_code_error_response):
        """Test that debug_parse_response includes codeError info."""
        with patch.object(EngineClient, "grammar_to_json") as mock_method:
            mock_method.side_effect = EngineParseError(
                message=mock_engine_code_error_response["codeError"]["message"],
                source_info=mock_engine_code_error_response["codeError"]["sourceInformation"],
                raw_error=mock_engine_code_error_response["codeError"],
            )

            client = EngineClient()
            result = client.debug_parse_response("invalid code")

            assert result["error"] is not None
            assert result["code_error"] is not None
            assert result["error_location"] is not None
            assert "Line 10" in result["error_location"]


class TestCodeErrorIntegration:
    """Integration tests for codeError handling across the system."""

    def test_error_flow_from_engine_to_mcp_response(self, mock_engine_code_error_response):
        """Test that codeError flows correctly from Engine to MCP error response."""
        # Simulate the full flow: Engine returns codeError -> EngineParseError raised
        # -> MCP tool catches and formats response

        error = EngineParseError(
            message=mock_engine_code_error_response["codeError"]["message"],
            source_info=mock_engine_code_error_response["codeError"]["sourceInformation"],
            raw_error=mock_engine_code_error_response["codeError"],
        )

        # Format as MCP would
        formatted = {
            "error": error.message,
            "location": error.get_formatted_location(),
            "source_info": error.source_info,
        }

        assert "unexpected token" in formatted["error"].lower()
        assert "Line 10" in formatted["location"]
        assert formatted["source_info"]["startColumn"] == 5

    def test_multiline_error_formatting(self, mock_engine_code_error_multiline):
        """Test formatting of multi-line codeError."""
        error = EngineParseError(
            message=mock_engine_code_error_multiline["codeError"]["message"],
            source_info=mock_engine_code_error_multiline["codeError"]["sourceInformation"],
            raw_error=mock_engine_code_error_multiline["codeError"],
        )

        location = error.get_formatted_location()
        assert "Lines 5-10" in location
