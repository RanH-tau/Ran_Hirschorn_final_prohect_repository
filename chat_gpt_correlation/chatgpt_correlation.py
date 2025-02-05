import json
import os
import traceback
from dotenv import load_dotenv
from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import ChatPromptTemplate
import re
import pandas as pd

load_dotenv()


def get_column_mapping(source_columns, target_columns):
    try:
        llm = initialize_llm()

        source_cols_str = "\n".join(f"- {col}" for col in source_columns)
        target_cols_str = "\n".join(f"- {col}" for col in target_columns)

        messages = mapping_template.format_messages(
            source_columns=source_cols_str,
            target_columns=target_cols_str
        )

        response = llm(messages)
        content = response.content.strip('`')
        if content.startswith('python\n'):
            content = content[7:]

        result = json.loads(content)
        return result['mappings'], result['explanations']

    except Exception as e:
        print(f"Error in get_column_mapping: {str(e)}")
        print(f"Error details: {traceback.format_exc()}")
        raise RuntimeError(f"Failed to generate column mapping: {str(e)}")


def initialize_llm():
    try:
        return ChatOpenAI(
            model=os.getenv("OPENAI_MODEL_NAME"),
            api_key=os.getenv("OPENAI_API_KEY"),
            temperature=0.1,
        )
    except Exception as e:
        print(f"Error initializing OpenAI: {str(e)}")
        print(f"Error details: {traceback.format_exc()}")
        raise RuntimeError(f"Failed to initialize OpenAI: {str(e)}")


chat_template = ChatPromptTemplate.from_messages([
    ("system",
     """You are an expert in telecom data analysis. Analyze how a raw data entry matches against multiple metadata entries. 

     Important Instructions:
     1. Do NOT use link numbers for correlation
     2. Do NOT use coordinates (ITMX, ITMY, latitude, longitude) for correlation
     3. Focus only on technical parameters like:
        - Frequency
        - Polarization
        - Band width
        - Power levels
        - Antenna characteristics
        - Equipment types
        - Technical configurations
     4. Look for matching patterns in:
        - Equipment specifications
        - Technical parameters
        - Configuration details
     5. Consider physical characteristics of the link that would be consistent across different records"""),
    ("human",
     """Compare this raw data entry against multiple metadata entries:

Raw data entry:
{raw_data}

Metadata entries:
{metadata_rows}

For each metadata entry, determine:
1. Correlation (0-1) based on field matches (excluding link numbers and coordinates)
2. Explanation of why this correlation was assigned
3. Key matching points and differences found

Return a JSON array with an object for each metadata entry:
[
    {{
        "correlation": float,        // 0-1 correlation score
        "explanation": string,       // Brief explanation of the correlation reasoning
        "metadata_index": int,       // Index of the metadata entry in input array
        "matching_points": string[]  // List of key matching points found
    }},
    ...
]

Return ONLY the JSON array."""
     )
])

mapping_template = ChatPromptTemplate.from_messages([
    ("system",
     """You are an expert in data analysis and mapping, proficient in multiple languages including Hebrew and English. 
     Your task is to analyze two sets of column names and determine which columns should be mapped to each other.

     Consider the following carefully:
     1. Semantic meaning: Understand what each column represents across languages
     2. Field types: Consider the type of data each column might contain
     3. Name similarities: Look for columns that might represent the same data with different names
     4. Units: Pay attention to units in column names (e.g., KM, GHz, meters)
     5. Language variations: Account for fields in different languages that might mean the same thing

     Key Fields to Map:
     - Technical Parameters:
       * Frequency (Look for: Frequency, Band, תדר, with units like GHz) -> Frequency_GHz
       * Polarization (Look for: Polarization, קיטוב, polarization type indicators) -> Polarization
       * Length/Distance (Look for: Distance, Length, מרחק, with units like KM) -> Length_km
       * Antenna dimensions (Look for diameter info) -> NearAntennaDiameter_m, FarAntennaDiameter_m

     Note: Don't map these fields as they come from raw data:
     - Link numbers
     - Coordinates
     - RxLevel
     - TxLevel

     Return your response as a Python dictionary string where:
     - Keys are the source column names (from the matched metadata)
     - Values are the target column names (from the example metadata)
     Only include mappings you are confident about.
     Include a brief explanation for each mapping in a separate 'explanations' field."""),
    ("human",
     """Source columns (from matched metadata):\n{source_columns}\n\n
     Target columns (from example metadata):\n{target_columns}\n\n
     Please provide the mapping as a Python dictionary string with 'mappings' and 'explanations' keys.""")
])


def calculate_correlation_batch(raw_data_row, metadata_rows):
    llm = initialize_llm()
    try:
        # Convert raw data row to dictionary
        if isinstance(raw_data_row, pd.Series):
            raw_data_dict = raw_data_row.to_dict()
        else:
            raise ValueError("raw_data_row must be a pandas Series")

        # Convert metadata rows to list of dictionaries
        if isinstance(metadata_rows, pd.DataFrame):
            metadata_list = [row.to_dict() for _, row in metadata_rows.iterrows()]
        else:
            raise ValueError("metadata_rows must be a pandas DataFrame")

        # Serialize to JSON strings
        try:
            raw_data_str = json.dumps(raw_data_dict, ensure_ascii=False)
            metadata_str = json.dumps(metadata_list, ensure_ascii=False)
        except Exception as e:
            raise ValueError(f"Error serializing data to JSON: {e}")

        # Format messages for the LLM
        try:
            messages = chat_template.format_messages(
                raw_data=raw_data_str,
                metadata_rows=metadata_str
            )
        except Exception as e:
            raise ValueError(f"Error formatting messages: {e}")

        # Call the LLM
        try:
            response = llm(messages)
        except Exception as e:
            raise RuntimeError(f"Error calling LLM: {e}")

        # Clean and parse the LLM response
        try:
            content = response.content.strip()
            # Remove any code block markers if present
            if content.startswith('```'):
                content = content.split('```')[1]
            content = content.strip()
            if content.startswith('json'):
                content = content[4:].strip()

            # Add proper list brackets if missing
            if not content.startswith('['):
                content = '[' + content
            if not content.endswith(']'):
                content = content + ']'

            # Remove any trailing commas before closing brackets
            content = re.sub(r',(\s*})', r'\1', content)
            content = re.sub(r',(\s*\])', r'\1', content)

            print(f"Attempting to parse JSON: {content}")  # Debug print
            return json.loads(content)

        except json.JSONDecodeError as e:
            print(f"JSON parse error: {str(e)}")
            print(f"Raw response content: {response.content}")
            # Return a default response instead of failing
            return [{'correlation': 0, 'metadata_index': i}
                    for i in range(len(metadata_list))]
        except Exception as e:
            print(f"Error processing LLM response: {str(e)}")
            return [{'correlation': 0, 'metadata_index': i}
                    for i in range(len(metadata_list))]

    except Exception as e:
        print(f"Error in calculate_correlation_batch: {str(e)}")
        print("Raw data:", raw_data_dict if 'raw_data_dict' in locals() else 'Not available')
        print("Metadata sample:", metadata_list[:1] if 'metadata_list' in locals() else 'Not available')
        return [{'correlation': 0, 'metadata_index': i}
                for i in range(len(metadata_list) if 'metadata_list' in locals() else 1)]