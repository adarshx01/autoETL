from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from typing import List, Dict
import json
from config.settings import settings
import google.generativeai as genai

class BusinessRule(BaseModel):
    """Structured business rule model"""
    rule_id: str
    rule_name: str
    description: str
    source_tables: List[str]
    target_table: str
    transformation_logic: str
    validation_query: str
    expected_behavior: str

class NaturalLanguageProcessor:
    """
    Converts natural language business rules to executable SQL/Python code
    Supports both OpenAI and Google Gemini APIs
    """
    
    def __init__(self):
        # Use settings to determine provider
        if settings.AI_PROVIDER == "gemini":
            genai.configure(api_key=settings.GEMINI_API_KEY)
            self.llm = genai.GenerativeModel(settings.GEMINI_MODEL)
            self.is_gemini = True
        else:
            self.llm = ChatOpenAI(
                model=settings.OPENAI_MODEL,
                api_key=settings.OPENAI_API_KEY,
                temperature=0
            )
            self.is_gemini = False
            
        self.parser = PydanticOutputParser(pydantic_object=BusinessRule)
        
    def parse_business_rule(self, natural_language_rule: str) -> BusinessRule:
        """Convert natural language business rule to structured format"""
        
        # Get format instructions once to avoid double-formatting issues
        format_instructions = self.parser.get_format_instructions()
        
        prompt_text = f"""You are an expert data engineer. Convert the following business rule 
into a structured format.

Business Rule: {natural_language_rule}

Extract:
1. Source tables involved
2. Target table for results
3. SQL transformation logic (optimized query)
4. Validation query to verify the transformation
5. Expected behavior

{format_instructions}
"""
        
        if self.is_gemini:
            response = self.llm.generate_content(prompt_text)
            response_text = response.text
        else:
            # For OpenAI, use direct invoke without template formatting
            response = self.llm.invoke(prompt_text)
            response_text = response.content
            
        return self.parser.parse(response_text)
    
    def generate_transformation_code(self, rule: BusinessRule) -> str:
        """Generate Python/SQL code for the transformation"""
        
        code_prompt = f"""
Generate optimized Python code using Pandas to implement this transformation:

Rule: {rule.description}
Source Tables: {rule.source_tables}
Target Table: {rule.target_table}
Logic: {rule.transformation_logic}

Requirements:
- Use batch processing for large datasets
- Include error handling
- Add data quality checks
- Optimize for performance
"""
        
        if self.is_gemini:
            response = self.llm.generate_content(code_prompt)
            return response.text
        else:
            response = self.llm.invoke(code_prompt)
            return response.content