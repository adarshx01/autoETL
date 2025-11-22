from typing import List, Dict, Any
import itertools
from agents.nl_processor import BusinessRule
import json
import logging
from config.settings import settings
import google.generativeai as genai

logger = logging.getLogger(__name__)

class TestScenarioGenerator:
    """
    Enterprise AI-powered test scenario generator
    Creates comprehensive test cases automatically covering:
    - Normal/Happy path scenarios
    - Edge cases and boundary conditions
    - Data quality validation
    - Performance testing scenarios
    - Business rule validation
    - Negative testing scenarios
    """
    
    def __init__(self, llm):
        self.llm = llm
        self.is_gemini = isinstance(llm, genai.GenerativeModel)
    
    def generate_test_scenarios(self, rule: BusinessRule) -> List[Dict]:
        """
        Generate all possible test scenarios for a business rule
        Includes edge cases, boundary conditions, and normal cases
        """
        
        prompt = f"""
You are an expert QA engineer. Generate comprehensive test scenarios for this ETL data transformation rule.

Business Rule Details:
- Rule Name: {rule.rule_name}
- Description: {rule.description}
- Source Tables: {', '.join(rule.source_tables)}
- Target Table: {rule.target_table}
- Transformation Logic: {rule.transformation_logic}
- Validation Query: {rule.validation_query}

Generate 10-15 test scenarios covering:

1. **Normal/Happy Path** (3 scenarios):
   - Valid data with expected patterns
   - Typical business use cases
   - Standard data volumes

2. **Edge Cases** (3 scenarios):
   - Empty datasets
   - Single record
   - All NULL values in certain columns
   - Zero values in numeric fields

3. **Boundary Conditions** (2 scenarios):
   - Minimum/Maximum values
   - Date ranges at boundaries
   - Very large numbers

4. **Data Quality Issues** (3 scenarios):
   - Duplicate records
   - Invalid data formats
   - Missing required fields
   - Referential integrity violations

5. **Performance Testing** (2 scenarios):
   - Large volume datasets (10K+ records)
   - Complex aggregations

6. **Negative Testing** (2 scenarios):
   - Invalid transformations
   - Constraint violations

For EACH scenario, provide in JSON format:
{{
    "scenario_id": "unique_id",
    "name": "descriptive name",
    "category": "one of: HAPPY_PATH, EDGE_CASE, BOUNDARY, DATA_QUALITY, PERFORMANCE, NEGATIVE",
    "description": "detailed description",
    "input_data_description": "what input data looks like",
    "expected_output": "what output should be",
    "validation_query": "SQL query to validate results",
    "expected_record_count": estimated count,
    "assertions": ["list of specific checks to perform"]
}}

Return ONLY a valid JSON array of test scenarios.
"""
        
        try:
            if self.is_gemini:
                response = self.llm.generate_content(prompt)
                response_text = response.text
            else:
                response = self.llm.invoke(prompt)
                response_text = response.content
            
            # Parse response into structured test scenarios
            scenarios = self._parse_test_scenarios(response_text, rule)
            logger.info(f"Generated {len(scenarios)} test scenarios for rule: {rule.rule_name}")
            return scenarios
            
        except Exception as e:
            logger.error(f"Failed to generate test scenarios: {e}")
            # Return default scenarios as fallback
            return self._generate_default_scenarios(rule)
    
    def _parse_test_scenarios(self, llm_response: str, rule: BusinessRule) -> List[Dict]:
        """Parse LLM response into test scenario dictionaries"""
        try:
            # Clean the response to extract JSON
            response_clean = llm_response.strip()
            
            # Try to extract JSON array
            if '```json' in response_clean:
                start = response_clean.find('```json') + 7
                end = response_clean.find('```', start)
                response_clean = response_clean[start:end].strip()
            elif '```' in response_clean:
                start = response_clean.find('```') + 3
                end = response_clean.find('```', start)
                response_clean = response_clean[start:end].strip()
            
            scenarios = json.loads(response_clean)
            
            # Validate and enrich scenarios
            for i, scenario in enumerate(scenarios):
                if 'scenario_id' not in scenario:
                    scenario['scenario_id'] = f"TEST_{rule.rule_id}_{i+1}"
                if 'validation_query' not in scenario:
                    scenario['validation_query'] = rule.validation_query
                    
            return scenarios
            
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON from LLM response: {e}")
            return self._generate_default_scenarios(rule)
    
    def _generate_default_scenarios(self, rule: BusinessRule) -> List[Dict]:
        """Generate default test scenarios as fallback"""
        logger.info("Using default test scenario templates")
        
        scenarios = [
            {
                "scenario_id": f"{rule.rule_id}_HAPPY_001",
                "name": "Happy Path - Standard Data",
                "category": "HAPPY_PATH",
                "description": "Test transformation with valid, standard dataset",
                "input_data_description": f"Sample of records from {rule.source_tables[0]}",
                "expected_output": f"Transformed data in {rule.target_table}",
                "validation_query": rule.validation_query,
                "expected_record_count": 100,
                "assertions": [
                    "All records transformed successfully",
                    "No NULL values in required fields",
                    "Data types match target schema"
                ]
            },
            {
                "scenario_id": f"{rule.rule_id}_EDGE_001",
                "name": "Edge Case - Empty Dataset",
                "category": "EDGE_CASE",
                "description": "Test behavior with empty source data",
                "input_data_description": "Empty table or filtered to 0 records",
                "expected_output": "No records in target or appropriate handling",
                "validation_query": f"SELECT COUNT(*) FROM {rule.target_table}",
                "expected_record_count": 0,
                "assertions": [
                    "Process completes without errors",
                    "No orphaned data created"
                ]
            },
            {
                "scenario_id": f"{rule.rule_id}_EDGE_002",
                "name": "Edge Case - NULL Values",
                "category": "EDGE_CASE",
                "description": "Test handling of NULL values in source data",
                "input_data_description": "Records with NULL in various columns",
                "expected_output": "NULLs handled per business rules",
                "validation_query": rule.validation_query,
                "expected_record_count": 50,
                "assertions": [
                    "NULLs handled appropriately",
                    "No errors from NULL processing"
                ]
            },
            {
                "scenario_id": f"{rule.rule_id}_QUALITY_001",
                "name": "Data Quality - Duplicates",
                "category": "DATA_QUALITY",
                "description": "Test handling of duplicate records",
                "input_data_description": "Dataset with duplicate rows",
                "expected_output": "Duplicates handled per business logic",
                "validation_query": f"SELECT COUNT(*), COUNT(DISTINCT *) FROM {rule.target_table}",
                "expected_record_count": 100,
                "assertions": [
                    "Duplicates identified",
                    "Deduplication logic applied correctly"
                ]
            },
            {
                "scenario_id": f"{rule.rule_id}_PERF_001",
                "name": "Performance - Large Volume",
                "category": "PERFORMANCE",
                "description": "Test with large dataset (10K+ records)",
                "input_data_description": "Large volume of records",
                "expected_output": "All records processed efficiently",
                "validation_query": f"SELECT COUNT(*) FROM {rule.target_table}",
                "expected_record_count": 10000,
                "assertions": [
                    "Processing completes within SLA",
                    "Memory usage acceptable",
                    "No performance degradation"
                ]
            },
            {
                "scenario_id": f"{rule.rule_id}_BOUND_001",
                "name": "Boundary - Min/Max Values",
                "category": "BOUNDARY",
                "description": "Test with boundary values (min/max)",
                "input_data_description": "Records with extreme values",
                "expected_output": "Boundary values handled correctly",
                "validation_query": rule.validation_query,
                "expected_record_count": 10,
                "assertions": [
                    "No overflow/underflow errors",
                    "Boundary conditions met"
                ]
            }
        ]
        
        return scenarios