import json
import os
import logging
from typing import List, Optional

from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# Define the Pydantic models
class Actor(BaseModel):
    name: str = Field(..., description="The name of the actor.")
    role: str = Field(..., description="The role of the actor in the event.")


class EventResponse(BaseModel):
    main_actors: List[Actor] = Field(
        ..., description="A list of main actors involved in the event."
    )
    other_actors: List[Actor] = Field(
        ..., description="A list of other actors involved in the event."
    )
    category: str = Field(
        ...,
        description="Category of the event.",
        enum=["Political Turmoil", "New Product Announced", "Leadership Change", "Housing Issues", "Others"]
    )


class NewsProcessor:
    client = None

    def __init__(self,
                 api_key: Optional[str] = None,
                 org_id: Optional[str] = None,
                 project: Optional[str] = None):
        """Initialize NewsProcessor with OpenAI credentials.
        
        Args:
            api_key: Optional OpenAI API key. If not provided, will look for OPENAI_API_KEY env var
            org_id: Optional OpenAI organization ID. If not provided, will look for OPENAI_ORG_ID env var
            project: Optional project identifier. If not provided, will look for OPENAI_PROJECT env var
        """
        self.client = OpenAI(
            api_key=api_key or os.getenv('OPENAI_API_KEY'),
            organization=org_id or os.getenv('OPENAI_ORG_ID'),
            project=project or os.getenv('OPENAI_PROJECT')
        )

        if not self.client.api_key:
            raise ValueError(
                "OpenAI API key must be provided either through constructor or OPENAI_API_KEY environment variable")


    def analyze_text(self, text: str) -> Optional[EventResponse]:
        # Convert the Pydantic model to OpenAI-compatible JSON Schema
        function_schema = {
            "name": "extract_event",
            "description": "Extracts actors from a news article and classifies event type.",
            "parameters": EventResponse.model_json_schema()
        }

        prompt_messages = [
            {
                "role": "system",
                "content": """You are a structured information extraction engine specialized in named entity recognition and event analysis.

When extracting actors, follow these strict rules:
1. Always use specific named entities, not generic terms like "researchers", "scientists", "US", "M&A", "Co-op", etc.
2. For organizations and institutions use full official names when available
3. For companies and individuals avoid generic terms like "researchers" or "scientists" and use their full names when available
4. If a specific name cannot be determined, use the most specific available identifier.

Remember: The goal is to identify specific, named entities that can be tracked across articles and their roles, not generic categories."""
            },
            {
                "role": "user",
                "content": text
            }
        ]

        try:
            response = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=prompt_messages,
                tools=[{"type": "function", "function": function_schema}],
                tool_choice={"type": "function", "function": {"name": "extract_event"}},
                temperature=0.3
            )

            # Extract function call arguments from the assistant's reply
            tool_calls = response.choices[0].message.tool_calls
            if not tool_calls:
                logger.warning("⚠️ Extracting actors and event category from the news article probably failed.")
                return None

            arguments = tool_calls[0].function.arguments
            if isinstance(arguments, str):
                arguments = json.loads(arguments)  # Ensure it's a dict

            return EventResponse(**arguments)

        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(f"❌ Failed to parse function call output: {e}")
            return None
