import hashlib
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import aiohttp
import PyPDF2
import io

from magictables.utils import flatten_nested_structure


class BaseSource(ABC):
    @abstractmethod
    async def fetch_data(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_identifier(self) -> str:
        pass

    @abstractmethod
    def get_params(self) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_type(self) -> str:
        pass

    def get_id(self) -> str:
        # Generate a unique ID based on the source type, identifier, and params
        source_info = {
            "type": self.get_type(),
            "identifier": self.get_identifier(),
            "params": self.get_params(),
        }
        return hashlib.md5(json.dumps(source_info, sort_keys=True).encode()).hexdigest()


class RawSource(BaseSource):
    def __init__(self, data: List[Dict[str, Any]]):
        self.data = data

    async def fetch_data(self) -> List[Dict[str, Any]]:
        return self.data

    def get_identifier(self) -> str:
        return "raw_data"

    def get_params(self) -> Optional[Dict[str, Any]]:
        return {
            "data_hash": hashlib.md5(
                json.dumps(self.data, sort_keys=True).encode()
            ).hexdigest()
        }

    def get_type(self) -> str:
        return "raw"


class APISource(BaseSource):
    def __init__(self, api_url: str, params: Optional[Dict[str, Any]] = None):
        self.api_url = api_url
        self.params = params

    async def fetch_data(self) -> List[Dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.api_url, params=self.params) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        return flatten_nested_structure(data)
                    elif isinstance(data, dict):
                        return flatten_nested_structure(data)
                    else:
                        raise ValueError(
                            f"Unexpected data format from API: {type(data)}"
                        )
                else:
                    raise Exception(
                        f"Failed to fetch data from {self.api_url}. Status code: {response.status}"
                    )

    def get_identifier(self) -> str:
        return self.api_url

    def get_params(self) -> Optional[Dict[str, Any]]:
        return self.params

    def get_type(self) -> str:
        return "api"


class WebSource(BaseSource):
    def __init__(self, url: str):
        self.url = url

    async def fetch_data(self) -> List[Dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                if response.status == 200:
                    html_content = await response.text()
                    # Here you would typically parse the HTML content
                    # and extract the relevant data as a list of dictionaries
                    # For this example, we'll just return a simple dictionary
                    return [{"content": html_content}]
                else:
                    raise Exception(f"Failed to fetch data from {self.url}")

    def get_identifier(self) -> str:
        return self.url

    def get_params(self) -> Optional[Dict[str, Any]]:
        return None

    def get_type(self) -> str:
        return "web"


class PDFSource(BaseSource):
    def __init__(self, pdf_url: str):
        self.pdf_url = pdf_url

    async def fetch_data(self) -> List[Dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.pdf_url) as response:
                if response.status == 200:
                    pdf_content = await response.read()
                    pdf_file = io.BytesIO(pdf_content)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)

                    data = []
                    for page in pdf_reader.pages:
                        text = page.extract_text()
                        data.append({"page_content": text})

                    return data
                else:
                    raise Exception(f"Failed to fetch PDF from {self.pdf_url}")

    def get_identifier(self) -> str:
        return self.pdf_url

    def get_params(self) -> Optional[Dict[str, Any]]:
        return None

    def get_type(self) -> str:
        return "pdf"


class GenerativeSource(BaseSource):
    def __init__(self, query: str, parent_source_id: str):
        self.query = query
        self.parent_source_id = parent_source_id

    async def fetch_data(self) -> List[Dict[str, Any]]:
        # This method won't be used for fetching, as the data is generated
        raise NotImplementedError("GenerativeSource does not fetch data")

    def get_identifier(self) -> str:
        return f"generated_{self.parent_source_id}_{self.query}"

    def get_params(self) -> Optional[Dict[str, Any]]:
        return {"query": self.query, "parent_source_id": self.parent_source_id}

    def get_type(self) -> str:
        return "generative"
