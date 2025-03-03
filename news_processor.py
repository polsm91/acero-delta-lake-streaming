import json
import os
from typing import List, Optional

from openai import OpenAI
from pydantic import BaseModel, Field


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
        enum=["Political Turmoil", "New Product Announced", "Leadership Change", "Housing Issues",  "Others"]
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

    def upload_file(self):
        self.validate("training_set.jsonl")
        self.client.files.create(
            file=open("training_set.jsonl", "rb"),
            purpose="fine-tune"
        )

    def fine_tune_with_file(self):
        self.client.fine_tuning.jobs.create(
            training_file="file-argC59EYP8biP1pnFw6OR4Kh",
            model="gpt-3.5-turbo",
            suffix="news-impact"
        )

    def validate(self, filename):
        with open(filename) as file:
            try:
                json.load(file)  # put JSON-data to a variable
            except json.decoder.JSONDecodeError as e:
                print("Invalid JSON")  # in case json is invalid
                print(e)

    def analyze_text(self, text):
        response = self.client.beta.chat.completions.parse(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "text",
                            "text": "Identify the actors involved in the news article. Then, analyze the content and classify the news into these categories: Political Turmoil, New Product Announced, Leadership Change, Housing Issues, or Others."
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": text
                        }
                    ]
                },
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": "{\"main_actors\":[{\"name\":\"Apple\",\"role\":\"Company announcing a new product launch\"}],\"other_actors\":[],\"category\":\"New Product Announced\"}"
                        }
                    ]
                }
            ],
            response_format=EventResponse,
            temperature=0.7,
            max_tokens=64,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        return response.choices[0].message.parsed
